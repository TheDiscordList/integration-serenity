//! A tiny local HTTP server that records requests and answers from a closure.
//! Shared by the unit tests (`#[path]`) and the integration tests.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn body_text(&self) -> &str {
        std::str::from_utf8(&self.body).unwrap()
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Reply {
    pub fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_owned(), value.into()));
        self
    }
}

type Responder = Arc<dyn Fn(&Request) -> Reply + Send + Sync>;

pub struct MockServer {
    pub url: String,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl MockServer {
    pub async fn start(respond: impl Fn(&Request) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let respond: Responder = Arc::new(respond);

        let recorded = Arc::clone(&requests);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let recorded = Arc::clone(&recorded);
                let respond = Arc::clone(&respond);
                tokio::spawn(async move {
                    let _ = serve(stream, &recorded, &respond).await;
                });
            }
        });

        Self { url, requests }
    }

    /// Answers 200 `{}` to everything.
    pub async fn ok() -> Self {
        Self::start(|_| Reply::new(200, "{}")).await
    }

    pub fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    pub fn posts(&self) -> Vec<Request> {
        self.requests()
            .into_iter()
            .filter(|request| request.method == "POST")
            .collect()
    }

    /// Polls until at least `count` POSTs arrived, or panics after `timeout`.
    pub async fn wait_for_posts(&self, count: usize, timeout: Duration) -> Vec<Request> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let posts = self.posts();
            if posts.len() >= count {
                return posts;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "expected {count} POSTs, got {:?}",
                posts.iter().map(|post| &post.path).collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn serve(
    mut stream: TcpStream,
    recorded: &Mutex<Vec<Request>>,
    respond: &Responder,
) -> std::io::Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };

    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split(' ');
    let method = request_line.next().unwrap_or_default().to_owned();
    let path = request_line.next().unwrap_or_default().to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(key, _)| key == "content-length")
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(0);

    let mut body = buffer[head_end..].to_vec();
    while body.len() < length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }

    let request = Request {
        method,
        path,
        headers,
        body,
    };
    let reply = respond(&request);
    recorded.lock().unwrap().push(request);

    let mut response = format!(
        "HTTP/1.1 {} Mock\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
        reply.status,
        reply.body.len()
    );
    for (name, value) in &reply.headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    response.push_str(&reply.body);
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

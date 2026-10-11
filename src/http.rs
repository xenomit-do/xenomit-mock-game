//! Minimal HTTP/1.1 server plumbing for the mock Backpage and the
//! control API. One request per connection (`Connection: close`) keeps
//! the parser trivial and is fine for a test double; `reqwest` reopens
//! connections transparently.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

const MAX_HEAD: usize = 64 * 1024;
const MAX_BODY: usize = 4 * 1024 * 1024;

/// A parsed inbound request.
#[derive(Debug)]
pub struct Request {
    pub method: String,
    /// Raw target as sent (`/path?query` or absolute-form for proxies).
    pub target: String,
    /// Path component of `target` (query stripped, no decoding).
    pub path: String,
    /// Decoded query parameters.
    pub query: HashMap<String, String>,
    /// Lowercased header name → value, in wire order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub peer: SocketAddr,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.as_str())
    }

    /// `Cookie` header lookup by cookie name.
    pub fn cookie(&self, name: &str) -> Option<String> {
        for raw in self.header("cookie")?.split(';') {
            let part = raw.trim();
            if let Some((n, v)) = part.split_once('=')
                && n.trim().eq_ignore_ascii_case(name)
            {
                return Some(v.trim().to_string());
            }
        }
        None
    }

    /// `application/x-www-form-urlencoded` body as a map.
    pub fn form(&self) -> HashMap<String, String> {
        let text = String::from_utf8_lossy(&self.body);
        parse_query(&text)
    }

    /// `http://{host}` authority derived from the Host header — lets the
    /// mock emit absolute URLs that point back at itself regardless of
    /// the bound port.
    pub fn origin(&self) -> String {
        match self.header("host") {
            Some(host) => format!("http://{host}"),
            None => "http://localhost".into(),
        }
    }
}

/// `a=b&c=d` → map; `+` decodes to space, `%XX` percent-decodes.
pub fn parse_query(text: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in text.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        out.insert(percent_decode(k), percent_decode(v));
    }
    out
}

pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    Ok(b) => out.push(b),
                    Err(_) => out.push(b'%'),
                }
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn percent_encode(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Read a single request: header block up to `\r\n\r\n`, then a
/// `Content-Length` body. `None` on clean EOF before any bytes.
/// Returns the request plus any body bytes already read past the head.
async fn read_head(r: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 8192];
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let rest = buf.split_off(pos + 4);
            let head = buf;
            return Ok(Some((head, rest)));
        }
        if buf.len() > MAX_HEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header too large",
            ));
        }
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed mid-headers",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Read one request from a stream. `Ok(None)` on orderly EOF.
pub async fn read_request(
    r: &mut (impl AsyncRead + Unpin),
    peer: SocketAddr,
) -> io::Result<Option<Request>> {
    let Some((head, mut rest)) = read_head(r).await? else {
        return Ok(None);
    };
    let head_text = String::from_utf8_lossy(&head);
    let mut lines = head_text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    if method.is_empty() || target.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad request line",
        ));
    }
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        if name == "content-length" {
            content_length = value.parse().unwrap_or(0);
        }
        headers.push((name, value));
    }
    if content_length > MAX_BODY {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
    }
    let mut body = Vec::with_capacity(content_length.min(8192));
    if rest.len() > content_length {
        body.extend_from_slice(&rest[..content_length]);
        rest.clear();
    } else {
        body.append(&mut rest);
    }
    while body.len() < content_length {
        let want = content_length - body.len();
        let mut chunk = vec![0u8; want.min(8192)];
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed mid-body",
            ));
        }
        body.extend_from_slice(&chunk[..n]);
    }

    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), parse_query(q)),
        None => (target.clone(), HashMap::new()),
    };
    Ok(Some(Request {
        method,
        target,
        path,
        query,
        headers,
        body,
        peer,
    }))
}

pub fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        302 => "Found",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

/// Builder for a response; `Connection: close` is always appended.
pub struct Response {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl Response {
    pub fn new(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body: body.into(),
            headers: Vec::new(),
        }
    }

    pub fn text(body: impl Into<String>) -> Self {
        Self::new(200, "text/plain; charset=utf-8", body.into().into_bytes())
    }

    pub fn html(body: impl Into<String>) -> Self {
        Self::new(200, "text/html; charset=utf-8", body.into().into_bytes())
    }

    pub fn json(value: &serde_json::Value) -> Self {
        Self::new(
            200,
            "application/json",
            serde_json::to_vec(value).unwrap_or_default(),
        )
    }

    pub fn not_found() -> Self {
        Self::new(404, "text/plain", b"not found".to_vec())
    }

    /// 302 with a Location header — the backpage login flow is built on
    /// manually-followed redirects.
    pub fn redirect(location: &str) -> Self {
        let mut r = Self::new(302, "text/plain", Vec::new());
        r.headers.push(("Location".into(), location.to_string()));
        r
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

pub async fn write_response(w: &mut (impl AsyncWrite + Unpin), resp: Response) -> io::Result<()> {
    let mut out = Vec::with_capacity(resp.body.len() + 256);
    out.extend_from_slice(
        format!("HTTP/1.1 {} {}\r\n", resp.status, status_text(resp.status)).as_bytes(),
    );
    out.extend_from_slice(
        format!(
            "Content-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            resp.content_type,
            resp.body.len()
        )
        .as_bytes(),
    );
    for (name, value) in &resp.headers {
        out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&resp.body);
    w.write_all(&out).await?;
    w.flush().await
}

/// Serve `handler` on `listener` until `shutdown` fires. One task per
/// connection, one request per connection.
pub async fn serve<F, Fut>(
    listener: TcpListener,
    name: &'static str,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    handler: F,
) -> io::Result<()>
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Response> + Send,
{
    let handler = std::sync::Arc::new(handler);
    loop {
        let (stream, peer) = tokio::select! {
            r = listener.accept() => r?,
            _ = shutdown.changed() => return Ok(()),
        };
        stream.set_nodelay(true).ok();
        let handler = handler.clone();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            let (mut rd, mut wr) = stream.into_split();
            let result = tokio::select! {
                r = read_request(&mut rd, peer) => r,
                _ = shutdown.changed() => return,
            };
            match result {
                Ok(Some(req)) => {
                    let resp = handler(req).await;
                    let _ = write_response(&mut wr, resp).await;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing_debug(name, peer, &e);
                }
            }
        });
    }
}

fn tracing_debug(name: &str, peer: SocketAddr, e: &io::Error) {
    eprintln!("[{name}] {peer}: {e}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_parsing() {
        let q = parse_query("a=1&b=hello+world&c=%2F%3F");
        assert_eq!(q["a"], "1");
        assert_eq!(q["b"], "hello world");
        assert_eq!(q["c"], "/?");
    }

    #[test]
    fn cookie_lookup() {
        let req = Request {
            method: "GET".into(),
            target: "/x".into(),
            path: "/x".into(),
            query: HashMap::new(),
            headers: vec![("cookie".into(), "a=1; dosid=abc123; b=2".into())],
            body: vec![],
            peer: "127.0.0.1:1".parse().unwrap(),
        };
        assert_eq!(req.cookie("dosid").as_deref(), Some("abc123"));
        assert_eq!(req.cookie("missing"), None);
    }
}

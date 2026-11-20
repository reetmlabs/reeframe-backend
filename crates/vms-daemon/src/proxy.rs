//! The outbound HTTP proxy: a proxy in the site's own network that outgoing
//! TCP connections must go through, reached with HTTP CONNECT.

use std::fmt;

use anyhow::Context;
use base64::Engine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Longest proxy response head accepted, so a broken proxy cannot make the
/// client buffer without limit.
const MAX_RESPONSE_HEAD: usize = 8 * 1024;

/// A parsed `http://[user:pass@]host:port` proxy URL. `Debug` and
/// `Display` never show the password.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct OutboundProxy {
    host: String,
    port: u16,
    user: Option<String>,
    /// Base64 of `user:pass`, the value of `Proxy-Authorization: Basic`.
    basic_credentials: Option<String>,
}

impl OutboundProxy {
    /// Parses the `[network] outbound_proxy_url` setting. Empty means no
    /// proxy. Errors never contain the password.
    pub(crate) fn parse(url: &str) -> anyhow::Result<Option<Self>> {
        if url.is_empty() {
            return Ok(None);
        }
        let parsed = reqwest::Url::parse(url)
            .map_err(|e| anyhow::anyhow!("outbound_proxy_url is not a URL: {e}"))?;
        anyhow::ensure!(
            parsed.scheme() == "http",
            "outbound_proxy_url must start with http://, got {}://",
            parsed.scheme()
        );
        let host = parsed
            .host_str()
            .context("outbound_proxy_url has no host")?
            .to_owned();
        let port = parsed.port_or_known_default().unwrap_or(80);

        let user = (!parsed.username().is_empty())
            .then(|| percent_decode(parsed.username()))
            .transpose()?;
        let basic_credentials = match (&user, parsed.password()) {
            (None, None) => None,
            (user, password) => {
                let password = password.map(percent_decode).transpose()?;
                let pair = format!(
                    "{}:{}",
                    user.as_deref().unwrap_or(""),
                    password.as_deref().unwrap_or("")
                );
                Some(base64::engine::general_purpose::STANDARD.encode(pair))
            }
        };
        Ok(Some(Self {
            host,
            port,
            user,
            basic_credentials,
        }))
    }

    /// Opens a TCP tunnel through the proxy to `host:port`.
    pub(crate) async fn connect(&self, host: &str, port: u16) -> anyhow::Result<TcpStream> {
        let mut stream = TcpStream::connect((unbracket(&self.host), self.port))
            .await
            .with_context(|| format!("cannot reach the outbound proxy {self}"))?;

        let authority = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
        if let Some(credentials) = &self.basic_credentials {
            request.push_str(&format!("Proxy-Authorization: Basic {credentials}\r\n"));
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await?;

        let head = read_response_head(&mut stream)
            .await
            .with_context(|| format!("no valid answer from the outbound proxy {self}"))?;
        let status_line = head.lines().next().unwrap_or_default();
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .with_context(|| format!("outbound proxy {self} sent {status_line:?}"))?;
        match status {
            200..=299 => Ok(stream),
            407 => anyhow::bail!("outbound proxy {self} requires other credentials (407)"),
            _ => anyhow::bail!("outbound proxy {self} refused the tunnel: {status_line}"),
        }
    }
}

/// Reads up to the blank line that ends the response head. Reads byte by
/// byte so no bytes of the tunnel behind it are consumed.
async fn read_response_head(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        anyhow::ensure!(head.len() < MAX_RESPONSE_HEAD, "response head too long");
        let byte = stream.read_u8().await?;
        head.push(byte);
    }
    Ok(String::from_utf8_lossy(&head).into_owned())
}

fn percent_decode(text: &str) -> anyhow::Result<String> {
    percent_encoding::percent_decode_str(text)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .context("outbound_proxy_url credentials are not valid UTF-8")
}

fn unbracket(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

impl fmt::Display for OutboundProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.user {
            Some(user) => write!(f, "http://{user}:***@{}:{}", self.host, self.port),
            None => write!(f, "http://{}:{}", self.host, self.port),
        }
    }
}

impl fmt::Debug for OutboundProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OutboundProxy({self})")
    }
}

/// `url` with any password replaced by `***`, for logs and `Debug` output.
pub(crate) fn redact(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut parsed) if parsed.password().is_some() => {
            let _ = parsed.set_password(Some("***"));
            parsed.to_string()
        }
        Ok(_) => url.to_owned(),
        // Unparsable, so it may still hold a password somewhere.
        Err(_) if url.contains('@') => "<redacted>".to_owned(),
        Err(_) => url.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    #[test]
    fn empty_means_direct_and_only_http_is_accepted() {
        assert_eq!(OutboundProxy::parse("").unwrap(), None);
        for url in [
            "https://proxy.example:3128",
            "socks5://proxy.example:1080",
            "proxy.example:3128",
        ] {
            assert!(OutboundProxy::parse(url).is_err(), "{url}");
        }
        let proxy = OutboundProxy::parse("http://proxy.example:3128")
            .unwrap()
            .unwrap();
        assert_eq!(proxy.host, "proxy.example");
        assert_eq!(proxy.port, 3128);
        assert_eq!(proxy.basic_credentials, None);
    }

    #[test]
    fn the_password_never_shows_in_debug_display_or_errors() {
        let proxy = OutboundProxy::parse("http://alice:s%40cret@proxy.example:3128")
            .unwrap()
            .unwrap();
        for text in [format!("{proxy:?}"), proxy.to_string()] {
            assert!(
                !text.contains("s@cret") && !text.contains("s%40cret"),
                "{text}"
            );
            assert!(text.contains("alice"), "{text}");
        }

        let error = OutboundProxy::parse("https://alice:s3cret@proxy.example:3128")
            .unwrap_err()
            .to_string();
        assert!(!error.contains("s3cret"), "{error}");

        assert_eq!(
            redact("http://alice:s3cret@proxy.example:3128"),
            "http://alice:***@proxy.example:3128/"
        );
        assert_eq!(
            redact("http://proxy.example:3128"),
            "http://proxy.example:3128"
        );
        assert_eq!(redact("not a url alice:s3cret@x"), "<redacted>");
    }

    /// A stand-in proxy that answers one CONNECT with `status` and then pipes
    /// the connection to `upstream`. Sends the request head it received.
    async fn stand_in_proxy(
        status: &'static str,
        upstream: std::net::SocketAddr,
    ) -> (std::net::SocketAddr, oneshot::Receiver<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (head_tx, head_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (client, _) = listener.accept().await.unwrap();
            let mut client = BufReader::new(client);
            let mut head = Vec::new();
            loop {
                let mut line = String::new();
                client.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                head.push(line.trim_end().to_owned());
            }
            head_tx.send(head).unwrap();
            let mut client = client.into_inner();
            client
                .write_all(format!("HTTP/1.1 {status}\r\n\r\n").as_bytes())
                .await
                .unwrap();
            if status.starts_with('2') {
                let mut server = TcpStream::connect(upstream).await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            }
        });
        (addr, head_rx)
    }

    async fn echo_server() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (mut read, mut write) = stream.split();
            let _ = tokio::io::copy(&mut read, &mut write).await;
        });
        addr
    }

    async fn assert_tunnel_echoes(stream: &mut TcpStream) {
        stream.write_all(b"hello").await.unwrap();
        let mut reply = [0u8; 5];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"hello");
    }

    #[tokio::test]
    async fn connect_opens_a_tunnel_without_credentials() {
        let upstream = echo_server().await;
        let (proxy_addr, head) = stand_in_proxy("200 Connection established", upstream).await;
        let proxy = OutboundProxy::parse(&format!("http://{proxy_addr}"))
            .unwrap()
            .unwrap();

        let mut stream = proxy.connect("coordinator.example", 443).await.unwrap();

        assert_tunnel_echoes(&mut stream).await;
        let head = head.await.unwrap();
        assert_eq!(head[0], "CONNECT coordinator.example:443 HTTP/1.1");
        assert!(head.contains(&"Host: coordinator.example:443".to_owned()));
        assert!(!head
            .iter()
            .any(|line| line.starts_with("Proxy-Authorization")));
    }

    #[tokio::test]
    async fn connect_sends_basic_credentials() {
        let upstream = echo_server().await;
        let (proxy_addr, head) = stand_in_proxy("200 OK", upstream).await;
        let proxy = OutboundProxy::parse(&format!("http://alice:s3cret@{proxy_addr}"))
            .unwrap()
            .unwrap();

        let mut stream = proxy.connect("coordinator.example", 8443).await.unwrap();

        assert_tunnel_echoes(&mut stream).await;
        let expected = format!(
            "Proxy-Authorization: Basic {}",
            base64::engine::general_purpose::STANDARD.encode("alice:s3cret")
        );
        assert!(head.await.unwrap().contains(&expected));
    }

    #[tokio::test]
    async fn a_407_answer_fails_without_leaking_the_password() {
        let upstream = echo_server().await;
        let (proxy_addr, _head) =
            stand_in_proxy("407 Proxy Authentication Required", upstream).await;
        let proxy = OutboundProxy::parse(&format!("http://alice:wrong-pass@{proxy_addr}"))
            .unwrap()
            .unwrap();

        let error = proxy.connect("coordinator.example", 443).await.unwrap_err();

        let text = format!("{error:#}");
        assert!(text.contains("407"), "{text}");
        assert!(!text.contains("wrong-pass"), "{text}");
    }
}

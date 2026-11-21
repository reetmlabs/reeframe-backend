//! Turning the Coordinator's ICE servers into webrtcbin settings.

use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reeframe_protocol::IceServer;

/// ICE servers in webrtcbin's URL form.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct IceUrls {
    /// webrtcbin takes a single STUN server, so only the first one is used.
    pub stun: Option<String>,
    pub turn: Vec<String>,
}

/// Converts `RTCIceServer` URLs (`stun:host:port`, `turn:host:port?transport=tcp`,
/// `turns:...`) into `stun://host:port` and `turn(s)://user:pass@host:port?...`.
pub(crate) fn urls(servers: &[IceServer]) -> IceUrls {
    let mut urls = IceUrls::default();
    for server in servers {
        for url in &server.urls {
            if let Some(rest) = url.strip_prefix("stun:") {
                if urls.stun.is_none() {
                    urls.stun = Some(format!("stun://{}", rest.trim_start_matches("//")));
                }
                continue;
            }
            let (scheme, rest) = if let Some(rest) = url.strip_prefix("turns:") {
                ("turns", rest)
            } else if let Some(rest) = url.strip_prefix("turn:") {
                ("turn", rest)
            } else {
                tracing::warn!(
                    url,
                    "Ignoring an ICE server URL that is neither STUN nor TURN"
                );
                continue;
            };
            let (Some(username), Some(credential)) = (&server.username, &server.credential) else {
                tracing::warn!(url, "Ignoring a TURN server without credentials");
                continue;
            };
            // Time-limited TURN usernames contain ':', which would otherwise
            // split user and password in the wrong place.
            let user = utf8_percent_encode(username, NON_ALPHANUMERIC);
            let pass = utf8_percent_encode(credential, NON_ALPHANUMERIC);
            let rest = rest.trim_start_matches("//");
            urls.turn.push(format!("{scheme}://{user}:{pass}@{rest}"));
        }
    }
    urls
}

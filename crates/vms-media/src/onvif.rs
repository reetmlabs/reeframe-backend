//! ONVIF device discovery and stream-profile resolution.
//!
//! Two stateless operations that never touch the DB or configured cameras:
//!
//! - [`discover`]: WS-Discovery. Sends a multicast probe on the LAN and
//!   collects the ONVIF devices that reply, each identified by its
//!   device-service `xaddr`.
//! - [`resolve_streams`]: calls a device's ONVIF Media service
//!   (`GetCapabilities` -> `GetProfiles` -> `GetStreamUri`) to get its main
//!   and optional sub-stream RTSP URLs, the values for the `rtsp_url` and
//!   `sub_rtsp_url` fields of `POST /cameras`.
//!
//! No ONVIF crate is used. We only need one UDP probe and three SOAP calls,
//! and the available crates either panic when discovery finds nothing or
//! lack the WS-Security authentication most cameras require for Media calls.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use base64::Engine;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use rand::RngCore;
use sha1::{Digest, Sha1};
use tokio::net::UdpSocket;
use uuid::Uuid;
use vms_core::VmsError;

const WS_DISCOVERY_MULTICAST: &str = "239.255.255.250:3702";

// -- Public types --

/// One device that answered a WS-Discovery probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    /// The device service URL. Pass it to [`resolve_streams`] to fetch the
    /// stream URIs.
    pub xaddr: String,
    /// Raw ONVIF scope URIs (e.g. `onvif://www.onvif.org/name/CAM1`,
    /// `onvif://www.onvif.org/hardware/...`). Best-effort identification
    /// hints; vendors may omit them or format them differently.
    pub scopes: Vec<String>,
}

/// Stream URLs resolved from a device's ONVIF media profiles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedStreams {
    pub main_rtsp_url: String,
    /// `None` if the device only exposes a single profile.
    pub sub_rtsp_url: Option<String>,
}

// -- Discovery --

/// Broadcast a WS-Discovery probe and collect `ProbeMatch` replies until
/// `timeout` elapses. Finding nothing (no ONVIF devices, or multicast blocked
/// on the LAN) returns `Ok(vec![])`.
pub async fn discover(timeout: Duration) -> Result<Vec<DiscoveredDevice>, VmsError> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| VmsError::Media(format!("ONVIF discovery: failed to bind UDP socket: {e}")))?;

    let target: SocketAddr = WS_DISCOVERY_MULTICAST
        .parse()
        .expect("WS_DISCOVERY_MULTICAST is a valid socket address");

    let probe = probe_message(Uuid::new_v4());
    socket
        .send_to(probe.as_bytes(), target)
        .await
        .map_err(|e| VmsError::Media(format!("ONVIF discovery: failed to send probe: {e}")))?;

    let mut devices = Vec::new();
    let mut seen_xaddrs = HashSet::new();
    let mut buf = vec![0u8; 65536];
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        let received = match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await {
            Ok(Ok((n, _src))) => n,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "ONVIF discovery: error receiving probe response");
                break;
            }
            Err(_) => break, // deadline elapsed
        };

        let Ok(text) = std::str::from_utf8(&buf[..received]) else {
            continue;
        };
        for device in parse_probe_matches(text) {
            if seen_xaddrs.insert(device.xaddr.clone()) {
                devices.push(device);
            }
        }
    }

    Ok(devices)
}

fn probe_message(message_id: Uuid) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<e:Envelope xmlns:e="http://www.w3.org/2003/05/soap-envelope"
            xmlns:w="http://schemas.xmlsoap.org/ws/2004/08/addressing"
            xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery"
            xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
  <e:Header>
    <w:MessageID>uuid:{message_id}</w:MessageID>
    <w:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</w:To>
    <w:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</w:Action>
  </e:Header>
  <e:Body>
    <d:Probe>
      <d:Types>dn:NetworkVideoTransmitter</d:Types>
    </d:Probe>
  </e:Body>
</e:Envelope>"#
    )
}

/// Parses every `ProbeMatch` block in one WS-Discovery UDP datagram.
/// Tags are matched by local name only, because vendors bind the discovery
/// namespaces to different prefixes (or none) and only the local names are
/// fixed by the spec.
fn parse_probe_matches(xml: &str) -> Vec<DiscoveredDevice> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut devices = Vec::new();
    let mut in_probe_match = false;
    let mut field: Option<&'static str> = None;
    let mut xaddr: Option<String> = None;
    let mut scopes: Vec<String> = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => match e.local_name().as_ref() {
                b"ProbeMatch" => {
                    in_probe_match = true;
                    xaddr = None;
                    scopes.clear();
                }
                b"XAddrs" if in_probe_match => field = Some("XAddrs"),
                b"Scopes" if in_probe_match => field = Some("Scopes"),
                _ => {}
            },
            Ok(Event::Text(t)) => {
                if let (Some(current_field), Ok(text)) = (field, t.decode()) {
                    match current_field {
                        "XAddrs" if xaddr.is_none() => {
                            xaddr = text.split_whitespace().next().map(str::to_string);
                        }
                        "Scopes" => {
                            scopes = text.split_whitespace().map(str::to_string).collect();
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => match e.local_name().as_ref() {
                b"XAddrs" | b"Scopes" => field = None,
                b"ProbeMatch" => {
                    in_probe_match = false;
                    if let Some(xaddr) = xaddr.take() {
                        devices.push(DiscoveredDevice {
                            xaddr,
                            scopes: std::mem::take(&mut scopes),
                        });
                    }
                }
                _ => {}
            },
            Err(e) => {
                tracing::debug!(error = %e, "ONVIF discovery: malformed ProbeMatch XML, skipping");
                break;
            }
            _ => {}
        }
    }

    devices
}

// -- Stream resolution --

/// Resolve a discovered device's main and (if present) sub-stream RTSP URLs
/// via its ONVIF Media service. `credentials` is `(username, password)`.
/// Most cameras reject `GetProfiles`/`GetStreamUri` without WS-Security auth,
/// but `None` is allowed for test cameras that have no auth configured.
pub async fn resolve_streams(
    xaddr: &str,
    credentials: Option<(&str, &str)>,
) -> Result<ResolvedStreams, VmsError> {
    let security = credentials.map(|(user, pass)| ws_security_header(user, pass));

    let caps_response =
        soap_call(xaddr, &envelope(security.as_deref(), GET_CAPABILITIES_BODY)).await?;
    // Fall back to the device xaddr if the response has no `Media` entry;
    // some devices answer Media requests at the same endpoint.
    let media_xaddr = parse_media_xaddr(&caps_response).unwrap_or_else(|| xaddr.to_string());

    let profiles_response = soap_call(
        &media_xaddr,
        &envelope(security.as_deref(), GET_PROFILES_BODY),
    )
    .await?;
    let profiles = parse_profiles(&profiles_response);
    let (main, sub) = pick_main_and_sub(profiles).ok_or_else(|| {
        VmsError::Media(format!(
            "ONVIF device at {xaddr} returned no media profiles"
        ))
    })?;

    let main_rtsp_url = fetch_stream_uri(&media_xaddr, &main.token, security.as_deref()).await?;
    let sub_rtsp_url = match sub {
        Some(profile) => {
            Some(fetch_stream_uri(&media_xaddr, &profile.token, security.as_deref()).await?)
        }
        None => None,
    };

    Ok(ResolvedStreams {
        main_rtsp_url,
        sub_rtsp_url,
    })
}

async fn fetch_stream_uri(
    media_xaddr: &str,
    token: &str,
    security: Option<&str>,
) -> Result<String, VmsError> {
    let body = envelope(security, &get_stream_uri_body(token));
    let response = soap_call(media_xaddr, &body).await?;
    parse_stream_uri(&response).ok_or_else(|| {
        VmsError::Media(format!(
            "ONVIF: GetStreamUri response for profile '{token}' had no Uri"
        ))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProfileInfo {
    token: String,
    /// `(width, height)`, when the profile's `VideoEncoderConfiguration`
    /// included one.
    resolution: Option<(u32, u32)>,
}

/// The highest-resolution profile is "main" and the next one, if any, is
/// "sub": a high-res stream for recording and a lower-res one for the sub relay
/// and analytics. Profiles without a resolution sort last, so a main profile
/// is always picked.
fn pick_main_and_sub(mut profiles: Vec<ProfileInfo>) -> Option<(ProfileInfo, Option<ProfileInfo>)> {
    if profiles.is_empty() {
        return None;
    }
    profiles.sort_by_key(|p| {
        std::cmp::Reverse(
            p.resolution
                .map(|(w, h)| u64::from(w) * u64::from(h))
                .unwrap_or(0),
        )
    });
    let main = profiles.remove(0);
    let sub = if profiles.is_empty() {
        None
    } else {
        Some(profiles.remove(0))
    };
    Some((main, sub))
}

// -- SOAP envelopes --

const GET_CAPABILITIES_BODY: &str = r#"<GetCapabilities xmlns="http://www.onvif.org/ver10/device/wsdl"><Category>All</Category></GetCapabilities>"#;

const GET_PROFILES_BODY: &str = r#"<GetProfiles xmlns="http://www.onvif.org/ver10/media/wsdl"/>"#;

fn get_stream_uri_body(token: &str) -> String {
    let token = quick_xml::escape::escape(token);
    format!(
        r#"<GetStreamUri xmlns="http://www.onvif.org/ver10/media/wsdl">
  <StreamSetup>
    <Stream xmlns="http://www.onvif.org/ver10/schema">RTP-Unicast</Stream>
    <Transport xmlns="http://www.onvif.org/ver10/schema"><Protocol>RTSP</Protocol></Transport>
  </StreamSetup>
  <ProfileToken>{token}</ProfileToken>
</GetStreamUri>"#
    )
}

fn envelope(security: Option<&str>, body: &str) -> String {
    let security = security.unwrap_or_default();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">
  <s:Header>{security}</s:Header>
  <s:Body>{body}</s:Body>
</s:Envelope>"#
    )
}

/// WS-Security `UsernameToken` header with a `PasswordDigest`, the auth scheme
/// ONVIF's Media/Device services expect. `username` is XML-escaped; `password`
/// only appears hashed into the digest.
fn ws_security_header(username: &str, password: &str) -> String {
    let mut nonce_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce_b64 = base64::engine::general_purpose::STANDARD.encode(nonce_bytes);
    let created = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let digest = password_digest(&nonce_bytes, &created, password);
    let username = quick_xml::escape::escape(username);

    format!(
        r#"<Security xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      <UsernameToken>
        <Username>{username}</Username>
        <Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">{digest}</Password>
        <Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">{nonce_b64}</Nonce>
        <Created xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">{created}</Created>
      </UsernameToken>
    </Security>"#
    )
}

/// WS-Security 1.0 `PasswordDigest`: `Base64(SHA1(nonce + created + password))`,
/// with `nonce` as raw bytes and `created`/`password` as UTF-8 bytes, exactly
/// as sent in the XML, because the camera recomputes it the same way.
fn password_digest(nonce: &[u8], created: &str, password: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(nonce);
    hasher.update(created.as_bytes());
    hasher.update(password.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

async fn soap_call(url: &str, body: &str) -> Result<String, VmsError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| VmsError::Media(format!("ONVIF: failed to build HTTP client: {e}")))?;

    let response = client
        .post(url)
        .header("Content-Type", "application/soap+xml; charset=utf-8")
        .body(body.to_string())
        .send()
        .await
        .map_err(|e| VmsError::Media(format!("ONVIF: request to {url} failed: {e}")))?;

    let status = response.status();
    let text = response.text().await.map_err(|e| {
        VmsError::Media(format!(
            "ONVIF: failed to read response body from {url}: {e}"
        ))
    })?;

    if !status.is_success() {
        return Err(VmsError::Media(format!(
            "ONVIF: {url} returned HTTP {status}"
        )));
    }
    Ok(text)
}

// -- Response parsing --

/// Extracts the `Media` service's `XAddr` from a `GetCapabilitiesResponse`.
/// Each sibling service block (`Device`, `Media`, `PTZ`, ...) has its own
/// `XAddr`; only the one inside `Media` is returned.
fn parse_media_xaddr(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut in_media = false;
    let mut awaiting_xaddr_text = false;

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => match e.local_name().as_ref() {
                b"Media" => in_media = true,
                b"XAddr" if in_media => awaiting_xaddr_text = true,
                _ => {}
            },
            Ok(Event::Text(t)) if awaiting_xaddr_text => {
                if let Ok(text) = t.decode() {
                    return Some(text.trim().to_string());
                }
            }
            Ok(Event::End(e)) => match e.local_name().as_ref() {
                b"XAddr" => awaiting_xaddr_text = false,
                b"Media" => in_media = false,
                _ => {}
            },
            Err(_) => break,
            _ => {}
        }
    }
    None
}

/// Extracts every `Profiles` block's `token` attribute and, if present, its
/// `VideoEncoderConfiguration/Resolution`, from a `GetProfilesResponse`.
fn parse_profiles(xml: &str) -> Vec<ProfileInfo> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut profiles = Vec::new();
    let mut current_token: Option<String> = None;
    let mut in_resolution = false;
    let mut width: Option<u32> = None;
    let mut height: Option<u32> = None;
    let mut awaiting: Option<&'static str> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => match e.local_name().as_ref() {
                b"Profiles" => {
                    current_token = e.attributes().flatten().find_map(|attr| {
                        (attr.key.local_name().as_ref() == b"token")
                            .then(|| String::from_utf8_lossy(&attr.value).into_owned())
                    });
                    width = None;
                    height = None;
                }
                b"Resolution" => in_resolution = true,
                b"Width" if in_resolution => awaiting = Some("Width"),
                b"Height" if in_resolution => awaiting = Some("Height"),
                _ => {}
            },
            Ok(Event::Text(t)) => {
                if let (Some(field), Ok(text)) = (awaiting, t.decode()) {
                    if let Ok(value) = text.trim().parse::<u32>() {
                        match field {
                            "Width" => width = Some(value),
                            "Height" => height = Some(value),
                            _ => {}
                        }
                    }
                }
            }
            Ok(Event::End(e)) => match e.local_name().as_ref() {
                b"Width" | b"Height" => awaiting = None,
                b"Resolution" => in_resolution = false,
                b"Profiles" => {
                    if let Some(token) = current_token.take() {
                        let resolution = match (width, height) {
                            (Some(w), Some(h)) => Some((w, h)),
                            _ => None,
                        };
                        profiles.push(ProfileInfo { token, resolution });
                    }
                }
                _ => {}
            },
            Err(_) => break,
            _ => {}
        }
    }
    profiles
}

/// Extracts `Uri` from a `GetStreamUriResponse`.
fn parse_stream_uri(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut awaiting = false;

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                if e.local_name().as_ref() == b"Uri" {
                    awaiting = true;
                }
            }
            Ok(Event::Text(t)) if awaiting => {
                if let Ok(text) = t.decode() {
                    return Some(text.trim().to_string());
                }
            }
            Ok(Event::End(e)) => {
                if e.local_name().as_ref() == b"Uri" {
                    awaiting = false;
                }
            }
            Err(_) => break,
            _ => {}
        }
    }
    None
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_probe_match() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<e:Envelope xmlns:e="http://www.w3.org/2003/05/soap-envelope"
            xmlns:w="http://schemas.xmlsoap.org/ws/2004/08/addressing"
            xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery"
            xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
  <e:Header><w:MessageID>uuid:abc</w:MessageID></e:Header>
  <e:Body>
    <d:ProbeMatches>
      <d:ProbeMatch>
        <w:EndpointReference><w:Address>urn:uuid:1234</w:Address></w:EndpointReference>
        <d:Types>dn:NetworkVideoTransmitter</d:Types>
        <d:Scopes>onvif://www.onvif.org/type/video_encoder onvif://www.onvif.org/name/CAM1</d:Scopes>
        <d:XAddrs>http://192.168.1.10/onvif/device_service</d:XAddrs>
        <d:MetadataVersion>1</d:MetadataVersion>
      </d:ProbeMatch>
    </d:ProbeMatches>
  </e:Body>
</e:Envelope>"#;

        let devices = parse_probe_matches(xml);
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].xaddr, "http://192.168.1.10/onvif/device_service");
        assert_eq!(
            devices[0].scopes,
            vec![
                "onvif://www.onvif.org/type/video_encoder".to_string(),
                "onvif://www.onvif.org/name/CAM1".to_string(),
            ]
        );
    }

    #[test]
    fn parses_multiple_probe_matches_in_one_datagram() {
        let xml = r#"<e:Envelope xmlns:e="http://www.w3.org/2003/05/soap-envelope"
            xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery">
  <e:Body>
    <d:ProbeMatches>
      <d:ProbeMatch>
        <d:Scopes>onvif://www.onvif.org/name/CAM1</d:Scopes>
        <d:XAddrs>http://192.168.1.10/onvif/device_service</d:XAddrs>
      </d:ProbeMatch>
      <d:ProbeMatch>
        <d:Scopes>onvif://www.onvif.org/name/CAM2</d:Scopes>
        <d:XAddrs>http://192.168.1.11/onvif/device_service</d:XAddrs>
      </d:ProbeMatch>
    </d:ProbeMatches>
  </e:Body>
</e:Envelope>"#;

        let devices = parse_probe_matches(xml);
        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0].xaddr, "http://192.168.1.10/onvif/device_service");
        assert_eq!(devices[1].xaddr, "http://192.168.1.11/onvif/device_service");
    }

    #[test]
    fn empty_or_malformed_probe_response_yields_no_devices() {
        assert!(parse_probe_matches("").is_empty());
        assert!(parse_probe_matches("<not valid xml").is_empty());
        assert!(parse_probe_matches("<e:Envelope></e:Envelope>").is_empty());
    }

    #[test]
    fn parses_media_xaddr_and_ignores_sibling_services() {
        let xml = r#"<Envelope>
  <Body>
    <GetCapabilitiesResponse>
      <Capabilities>
        <Device><XAddr>http://192.168.1.10/onvif/device_service</XAddr></Device>
        <Media><XAddr>http://192.168.1.10/onvif/media_service</XAddr></Media>
        <PTZ><XAddr>http://192.168.1.10/onvif/ptz_service</XAddr></PTZ>
      </Capabilities>
    </GetCapabilitiesResponse>
  </Body>
</Envelope>"#;

        assert_eq!(
            parse_media_xaddr(xml),
            Some("http://192.168.1.10/onvif/media_service".to_string())
        );
    }

    #[test]
    fn parse_media_xaddr_returns_none_without_a_media_block() {
        let xml = r#"<Envelope><Body><GetCapabilitiesResponse><Capabilities>
            <Device><XAddr>http://192.168.1.10/onvif/device_service</XAddr></Device>
        </Capabilities></GetCapabilitiesResponse></Body></Envelope>"#;
        assert_eq!(parse_media_xaddr(xml), None);
    }

    #[test]
    fn parses_profiles_with_resolution() {
        let xml = r#"<Envelope><Body><GetProfilesResponse>
  <Profiles token="Profile_1">
    <Name>MainStream</Name>
    <VideoEncoderConfiguration token="VEC_1">
      <Resolution><Width>1920</Width><Height>1080</Height></Resolution>
    </VideoEncoderConfiguration>
  </Profiles>
  <Profiles token="Profile_2">
    <Name>SubStream</Name>
    <VideoEncoderConfiguration token="VEC_2">
      <Resolution><Width>640</Width><Height>480</Height></Resolution>
    </VideoEncoderConfiguration>
  </Profiles>
</GetProfilesResponse></Body></Envelope>"#;

        let profiles = parse_profiles(xml);
        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[0].token, "Profile_1");
        assert_eq!(profiles[0].resolution, Some((1920, 1080)));
        assert_eq!(profiles[1].token, "Profile_2");
        assert_eq!(profiles[1].resolution, Some((640, 480)));
    }

    #[test]
    fn parses_profile_without_resolution() {
        let xml = r#"<Envelope><Body><GetProfilesResponse>
  <Profiles token="Profile_1"><Name>Only</Name></Profiles>
</GetProfilesResponse></Body></Envelope>"#;

        let profiles = parse_profiles(xml);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].token, "Profile_1");
        assert_eq!(profiles[0].resolution, None);
    }

    #[test]
    fn picks_highest_resolution_as_main() {
        let profiles = vec![
            ProfileInfo {
                token: "sub".into(),
                resolution: Some((640, 480)),
            },
            ProfileInfo {
                token: "main".into(),
                resolution: Some((1920, 1080)),
            },
        ];
        let (main, sub) = pick_main_and_sub(profiles).unwrap();
        assert_eq!(main.token, "main");
        assert_eq!(sub.unwrap().token, "sub");
    }

    #[test]
    fn single_profile_has_no_sub() {
        let profiles = vec![ProfileInfo {
            token: "only".into(),
            resolution: Some((1920, 1080)),
        }];
        let (main, sub) = pick_main_and_sub(profiles).unwrap();
        assert_eq!(main.token, "only");
        assert!(sub.is_none());
    }

    #[test]
    fn no_profiles_returns_none() {
        assert!(pick_main_and_sub(Vec::new()).is_none());
    }

    #[test]
    fn unresolvable_profiles_still_pick_a_deterministic_main() {
        let profiles = vec![
            ProfileInfo {
                token: "a".into(),
                resolution: None,
            },
            ProfileInfo {
                token: "b".into(),
                resolution: None,
            },
        ];
        let (main, sub) = pick_main_and_sub(profiles).unwrap();
        assert_eq!(main.token, "a");
        assert_eq!(sub.unwrap().token, "b");
    }

    #[test]
    fn parses_stream_uri() {
        let xml = r#"<Envelope><Body><GetStreamUriResponse>
  <MediaUri><Uri>rtsp://192.168.1.10:554/main</Uri><InvalidAfterConnect>false</InvalidAfterConnect></MediaUri>
</GetStreamUriResponse></Body></Envelope>"#;
        assert_eq!(
            parse_stream_uri(xml),
            Some("rtsp://192.168.1.10:554/main".to_string())
        );
    }

    #[test]
    fn parse_stream_uri_returns_none_when_absent() {
        assert_eq!(parse_stream_uri("<Envelope><Body></Body></Envelope>"), None);
    }

    /// Expected value computed in Python with base64 of
    /// `hashlib.sha1(nonce + created.encode() + password.encode()).digest()`
    /// on the same fixed inputs.
    #[test]
    fn password_digest_matches_known_vector() {
        let nonce = b"0123456789abcdef";
        let created = "2026-01-01T00:00:00Z";
        let password = "secret";
        let digest = password_digest(nonce, created, password);
        assert_eq!(digest, "AHdjPQqhLugJUej2bfmGQqPj3Vo=");
    }

    #[test]
    fn ws_security_header_escapes_username() {
        let header = ws_security_header("a&b<c>", "pw");
        assert!(header.contains("a&amp;b&lt;c&gt;"));
        assert!(!header.contains("<Username>a&b<c></Username>"));
    }

    #[test]
    fn get_stream_uri_body_escapes_token() {
        let body = get_stream_uri_body("token&<injected>");
        assert!(body.contains("token&amp;&lt;injected&gt;"));
    }
}

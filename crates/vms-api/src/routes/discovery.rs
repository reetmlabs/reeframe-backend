//! ONVIF device discovery — `POST /discovery/onvif/probe` (WS-Discovery) and
//! `POST /discovery/onvif/resolve` (per-device main/sub stream URL
//! resolution). Both are stateless and touch nothing in the DB; turning a
//! discovered device into an actual camera is a separate `POST /cameras`
//! call using whatever URLs `resolve` returned.

use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use vms_media::{discover, resolve_streams, DiscoveredDevice, ResolvedStreams};

use crate::error::{parse_body, ApiError};

const DEFAULT_PROBE_TIMEOUT_SECS: u64 = 3;
const MAX_PROBE_TIMEOUT_SECS: u64 = 15;

// -- Probe --

#[derive(Deserialize, Default)]
pub struct ProbeBody {
    /// How long to wait for devices to reply, in seconds. Defaults to 3,
    /// capped at 15 — the request blocks for this whole duration.
    timeout_secs: Option<u64>,
}

#[derive(Serialize)]
pub struct DiscoveredDeviceDto {
    pub xaddr: String,
    pub scopes: Vec<String>,
}

impl From<DiscoveredDevice> for DiscoveredDeviceDto {
    fn from(d: DiscoveredDevice) -> Self {
        Self {
            xaddr: d.xaddr,
            scopes: d.scopes,
        }
    }
}

/// POST /discovery/onvif/probe
///
/// Body is optional (`{}` or omitted entirely uses the default timeout).
#[handler]
pub async fn probe(req: &mut Request) -> Result<Json<Vec<DiscoveredDeviceDto>>, ApiError> {
    let body: ProbeBody = match req.payload().await {
        Ok(bytes) if !bytes.is_empty() => {
            serde_json::from_slice(bytes).map_err(|e| ApiError::bad_request(e.to_string()))?
        }
        _ => ProbeBody::default(),
    };

    let timeout_secs = body.timeout_secs.unwrap_or(DEFAULT_PROBE_TIMEOUT_SECS);
    if timeout_secs == 0 || timeout_secs > MAX_PROBE_TIMEOUT_SECS {
        return Err(ApiError::bad_request(format!(
            "timeout_secs must be between 1 and {MAX_PROBE_TIMEOUT_SECS}"
        )));
    }

    let devices = discover(Duration::from_secs(timeout_secs)).await?;
    Ok(Json(
        devices.into_iter().map(DiscoveredDeviceDto::from).collect(),
    ))
}

// -- Resolve --

#[derive(Deserialize)]
pub struct ResolveBody {
    /// The device-service `xaddr` from a `probe` response.
    pub xaddr: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Serialize)]
pub struct ResolvedStreamsDto {
    pub main_rtsp_url: String,
    pub sub_rtsp_url: Option<String>,
}

impl From<ResolvedStreams> for ResolvedStreamsDto {
    fn from(r: ResolvedStreams) -> Self {
        Self {
            main_rtsp_url: r.main_rtsp_url,
            sub_rtsp_url: r.sub_rtsp_url,
        }
    }
}

/// POST /discovery/onvif/resolve
///
/// `username`/`password` are optional — most cameras require them for
/// `GetProfiles`/`GetStreamUri`, but some test/dev cameras have no auth
/// configured at all.
#[handler]
pub async fn resolve(req: &mut Request) -> Result<Json<ResolvedStreamsDto>, ApiError> {
    let body: ResolveBody = parse_body(req).await?;
    if body.xaddr.trim().is_empty() {
        return Err(ApiError::bad_request("xaddr must not be empty"));
    }

    let credentials = match (&body.username, &body.password) {
        (Some(username), Some(password)) => Some((username.as_str(), password.as_str())),
        _ => None,
    };

    let resolved = resolve_streams(&body.xaddr, credentials).await?;
    Ok(Json(resolved.into()))
}

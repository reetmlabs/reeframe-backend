//! What the backend trusts when it talks to its Coordinator, shared by
//! enrollment, the control channel and the token key fetch so all accept the
//! same certificates.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::RootCertStore;

/// The roots for the Coordinator's certificate: the Mozilla set, so the
/// result does not depend on the host's CA store, plus every certificate in
/// `ca_file` when it is set.
pub(crate) fn trusted_roots(ca_file: &str) -> anyhow::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if ca_file.is_empty() {
        return Ok(roots);
    }

    let path = Path::new(ca_file);
    let certs = CertificateDer::pem_file_iter(path)
        .with_context(|| format!("cannot read [coordinator] ca_file {}", path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("[coordinator] ca_file {} is not valid PEM", path.display()))?;
    anyhow::ensure!(
        !certs.is_empty(),
        "[coordinator] ca_file {} contains no certificate",
        path.display()
    );
    for cert in certs {
        roots.add(cert).with_context(|| {
            format!(
                "[coordinator] ca_file {} holds an unusable certificate",
                path.display()
            )
        })?;
    }
    Ok(roots)
}

/// Parses `coordinator_url` and requires `https://`, since the site key and
/// the Coordinator's tokens must never travel unencrypted.
pub(crate) fn https_url(coordinator_url: &str) -> anyhow::Result<reqwest::Url> {
    let url = reqwest::Url::parse(coordinator_url)
        .with_context(|| format!("coordinator URL {coordinator_url:?} is not a URL"))?;
    anyhow::ensure!(
        url.scheme() == "https",
        "coordinator URL {coordinator_url:?} must start with https://"
    );
    Ok(url)
}

/// Limit for one JWKS fetch, so a Coordinator that drops traffic cannot stall
/// token checks.
pub(crate) const JWKS_TIMEOUT: Duration = Duration::from_secs(10);

/// An HTTP client for the Coordinator that trusts exactly `roots`. It uses
/// rustls with ring like the control channel, not the system TLS library,
/// so enrollment and the control channel cannot disagree about a certificate.
pub(crate) fn http_client(
    roots: RootCertStore,
    timeout: Duration,
) -> anyhow::Result<reqwest::Client> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .timeout(timeout)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_ca::TestCa;

    fn public_root_count() -> usize {
        webpki_roots::TLS_SERVER_ROOTS.len()
    }

    #[test]
    fn without_a_ca_file_only_the_public_roots_are_trusted() {
        assert_eq!(trusted_roots("").unwrap().len(), public_root_count());
    }

    #[test]
    fn a_ca_file_adds_its_certificates_to_the_public_roots() {
        let first = TestCa::new();
        let second = TestCa::new();
        let path = first.pem_file();
        let bundle = std::fs::read_to_string(&path).unwrap()
            + &std::fs::read_to_string(second.pem_file()).unwrap();
        std::fs::write(&path, bundle).unwrap();

        let roots = trusted_roots(path.to_str().unwrap()).unwrap();

        assert_eq!(roots.len(), public_root_count() + 2);
        for public in webpki_roots::TLS_SERVER_ROOTS {
            assert!(roots
                .roots
                .iter()
                .any(|root| root.subject == public.subject));
        }
    }

    #[test]
    fn a_missing_empty_or_certificate_free_ca_file_is_an_error() {
        let dir = std::env::temp_dir().join(format!("vms-ca-file-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.pem");
        std::fs::write(&empty, "").unwrap();
        let key_only = dir.join("key.pem");
        std::fs::write(
            &key_only,
            rcgen::KeyPair::generate().unwrap().serialize_pem(),
        )
        .unwrap();
        let text = dir.join("text.pem");
        std::fs::write(&text, "not a certificate").unwrap();

        let missing = trusted_roots(dir.join("missing.pem").to_str().unwrap()).unwrap_err();
        assert!(
            format!("{missing:#}").contains("cannot read"),
            "{missing:#}"
        );
        for path in [empty, key_only, text] {
            let error = trusted_roots(path.to_str().unwrap()).unwrap_err();
            assert!(
                format!("{error:#}").contains("no certificate"),
                "{}: {error:#}",
                path.display()
            );
        }
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn only_https_coordinator_urls_are_accepted() {
        assert!(https_url("https://coordinator.example").is_ok());
        for url in [
            "http://coordinator.example",
            "ws://coordinator.example",
            "coordinator.example",
        ] {
            assert!(https_url(url).is_err(), "{url}");
        }
    }
}

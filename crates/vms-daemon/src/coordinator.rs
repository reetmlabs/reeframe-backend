//! What the backend trusts when it talks to its Coordinator, shared by
//! enrollment and the control channel so both accept the same certificates.

use std::path::Path;

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
}

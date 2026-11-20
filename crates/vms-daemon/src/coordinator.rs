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

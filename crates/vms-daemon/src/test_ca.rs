//! A throwaway certificate authority for tests that need a Coordinator
//! certificate the public roots do not trust.

use std::path::PathBuf;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

pub(crate) struct TestCa {
    cert: rcgen::Certificate,
    key: KeyPair,
}

impl TestCa {
    pub(crate) fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "Reeframe test CA");
        let cert = params.self_signed(&key).unwrap();
        Self { cert, key }
    }

    /// Writes the CA certificate to a new PEM file and returns its path.
    pub(crate) fn pem_file(&self) -> PathBuf {
        let path = std::env::temp_dir().join(format!("vms-test-ca-{}.pem", uuid::Uuid::new_v4()));
        std::fs::write(&path, self.cert.pem()).unwrap();
        path
    }

    /// A server certificate for `host`, signed by this CA.
    pub(crate) fn leaf(&self, host: &str) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(vec![host.to_owned()])
            .unwrap()
            .signed_by(&key, &self.cert, &self.key)
            .unwrap();
        (
            cert.der().clone(),
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
    }
}

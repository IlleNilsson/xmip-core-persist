//! A cluster's certificate authority for the tests of a node and its
//! Storage nodes over mutual TLS, and the identities it issues. Test-only.

/// A cluster's certificate authority.
pub(crate) struct Authority(rcgen::CertifiedKey);

impl Authority {
    pub(crate) fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key_pair = rcgen::KeyPair::generate().expect("a key");
        let cert = params.self_signed(&key_pair).expect("signed");
        Self(rcgen::CertifiedKey { cert, key_pair })
    }

    /// An identity for a node at 127.0.0.1, trusting `anchors`.
    pub(crate) fn issue(&self, anchors: &Self) -> tls::Identity {
        let key = rcgen::KeyPair::generate().expect("a key");
        let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).expect("params");
        let leaf = params
            .signed_by(&key, &self.0.cert, &self.0.key_pair)
            .expect("issued");
        tls::Identity::from_pem(
            leaf.pem().as_bytes(),
            key.serialize_pem().as_bytes(),
            anchors.0.cert.pem().as_bytes(),
        )
        .expect("identity")
    }
}

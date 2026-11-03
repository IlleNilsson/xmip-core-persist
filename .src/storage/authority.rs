//! A cluster's certificate authority for the tests of a node and its
//! Storage nodes over mutual TLS, and the identities it issues. Test-only.

/// A cluster's certificate authority.
pub(crate) struct Authority(rcgen::CertifiedIssuer<'static, rcgen::KeyPair>);

impl Authority {
    pub(crate) fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().expect("a key");
        Self(rcgen::CertifiedIssuer::self_signed(params, key).expect("signed"))
    }

    /// An identity for a node at 127.0.0.1, trusting `anchors`: server and
    /// client authentication, as a node's certificate is (ADR-0034).
    pub(crate) fn issue(&self, anchors: &Self) -> tls::Identity {
        self.issued(
            anchors,
            &[
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            ],
        )
    }

    /// An identity for a person, trusting `anchors`: client authentication
    /// alone, as an operator's certificate is.
    pub(crate) fn issue_person(&self, anchors: &Self) -> tls::Identity {
        self.issued(anchors, &[rcgen::ExtendedKeyUsagePurpose::ClientAuth])
    }

    fn issued(&self, anchors: &Self, uses: &[rcgen::ExtendedKeyUsagePurpose]) -> tls::Identity {
        let key = rcgen::KeyPair::generate().expect("a key");
        let mut params =
            rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).expect("params");
        params.extended_key_usages = uses.to_vec();
        let leaf = params.signed_by(&key, &self.0).expect("issued");
        tls::Identity::from_pem(
            leaf.pem().as_bytes(),
            key.serialize_pem().as_bytes(),
            anchors.0.pem().as_bytes(),
        )
        .expect("identity")
    }
}

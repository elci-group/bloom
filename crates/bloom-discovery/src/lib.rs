//! LAN service discovery for Bloom.
//!
//! Discovered records are *candidates only*. DNS-SD data is unauthenticated
//! and must never be treated as proof of node identity, authorization, or trust.

mod mdns;

pub use mdns::{Advertisement, DiscoveryError, MdnsConfig, MdnsDiscovery};

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

/// Maximum number of DNS-SD candidates kept in memory. Advertisements are
/// unauthenticated, so registry growth must remain bounded.
pub const MAX_CANDIDATES: usize = 256;

/// An untrusted peer candidate learned from mDNS/DNS-SD.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateNode {
    /// DNS-SD instance fullname, used as the stable key for this advertisement.
    pub instance: String,
    /// Optional identity claim from TXT data; it is not verified here.
    pub node_id: Option<String>,
    pub hostname: String,
    pub port: u16,
    pub endpoint: String,
    pub addresses: Vec<IpAddr>,
    pub properties: BTreeMap<String, String>,
    /// Unix timestamp in milliseconds when the record was last resolved.
    pub last_seen_unix_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CandidateRegistry {
    inner: Arc<RwLock<BTreeMap<String, CandidateNode>>>,
}

impl CandidateRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, candidate: CandidateNode) {
        let mut entries = self.inner.write().expect("candidate registry poisoned");
        if !entries.contains_key(&candidate.instance)
            && entries.len() >= MAX_CANDIDATES
            && let Some(oldest) = entries
                .values()
                .min_by_key(|entry| entry.last_seen_unix_ms)
                .map(|entry| entry.instance.clone())
        {
            entries.remove(&oldest);
        }
        entries.insert(candidate.instance.clone(), candidate);
    }

    pub fn remove(&self, instance: &str) -> Option<CandidateNode> {
        self.inner
            .write()
            .expect("candidate registry poisoned")
            .remove(instance)
    }

    pub fn get(&self, instance: &str) -> Option<CandidateNode> {
        self.inner
            .read()
            .expect("candidate registry poisoned")
            .get(instance)
            .cloned()
    }

    /// Return a point-in-time, deterministic snapshot for API serialization.
    pub fn snapshot(&self) -> Vec<CandidateNode> {
        self.inner
            .read()
            .expect("candidate registry poisoned")
            .values()
            .cloned()
            .collect()
    }
}

pub(crate) fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn candidate(instance: &str, age_ms: u64) -> CandidateNode {
        CandidateNode {
            instance: instance.into(),
            node_id: Some("claimed-id".into()),
            hostname: "peer.local.".into(),
            port: 4455,
            endpoint: "http://peer.local.:4455".into(),
            addresses: vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
            properties: BTreeMap::new(),
            last_seen_unix_ms: unix_ms().saturating_sub(age_ms),
        }
    }

    #[test]
    fn registry_tracks_candidates_without_assigning_trust() {
        let registry = CandidateRegistry::new();
        registry.upsert(candidate("a", 0));
        assert_eq!(registry.snapshot().len(), 1);
        assert_eq!(
            registry.get("a").unwrap().node_id.as_deref(),
            Some("claimed-id")
        );
        assert_eq!(registry.remove("a").unwrap().instance, "a");
        assert!(registry.snapshot().is_empty());
    }

    #[test]
    fn registry_evicts_oldest_when_full() {
        let registry = CandidateRegistry::new();
        for index in 0..MAX_CANDIDATES {
            registry.upsert(candidate(
                &format!("node-{index}"),
                MAX_CANDIDATES as u64 - index as u64,
            ));
        }
        registry.upsert(candidate("newest", 0));
        assert_eq!(registry.snapshot().len(), MAX_CANDIDATES);
        assert!(registry.get("node-0").is_none());
        assert!(registry.get("newest").is_some());
    }
}

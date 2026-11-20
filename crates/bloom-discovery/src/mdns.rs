use crate::{CandidateNode, CandidateRegistry, unix_ms};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};
use thiserror::Error;

const DEFAULT_SERVICE_TYPE: &str = "_bloom._tcp.local.";

#[derive(Debug, Clone)]
pub struct Advertisement {
    /// Unverified identity claim included in TXT records.
    pub node_id: String,
    /// Port on which the Bloom node accepts LAN connections.
    pub port: u16,
    /// Whether the advertised port serves HTTPS.
    pub https: bool,
    /// Optional DNS-SD instance label. Defaults to `node_id`.
    pub instance_name: Option<String>,
    /// Optional host label. Defaults to `bloom-<node_id>.local.`.
    pub hostname: Option<String>,
    /// Exact interface addresses to publish for a specific bind. `None` asks
    /// mdns-sd to advertise the host's available interface addresses.
    pub addresses: Option<Vec<IpAddr>>,
}

#[derive(Debug, Clone)]
pub struct MdnsConfig {
    /// DNS-SD service type including domain suffix, usually `_bloom._tcp.local.`.
    pub service_type: String,
    pub advertisement: Option<Advertisement>,
}

impl Default for MdnsConfig {
    fn default() -> Self {
        Self {
            service_type: DEFAULT_SERVICE_TYPE.into(),
            advertisement: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("mDNS operation failed: {0}")]
    Mdns(String),
    #[error("invalid service advertisement: {0}")]
    Advertisement(String),
}

/// Owns the mDNS daemon and its background browse worker.
pub struct MdnsDiscovery {
    registry: CandidateRegistry,
    daemon: Option<ServiceDaemon>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    registered_fullname: Option<String>,
}

impl MdnsDiscovery {
    pub fn start(config: MdnsConfig) -> Result<Self, DiscoveryError> {
        let service = config
            .advertisement
            .map(|advertisement| service_info(&config.service_type, advertisement))
            .transpose()?;
        let daemon = ServiceDaemon::new().map_err(|e| DiscoveryError::Mdns(e.to_string()))?;
        let registry = CandidateRegistry::new();
        let stop = Arc::new(AtomicBool::new(false));
        let registered_fullname = if let Some(info) = service {
            let fullname = info.get_fullname().to_owned();
            if let Err(error) = daemon.register(info) {
                let _ = daemon.shutdown();
                return Err(DiscoveryError::Mdns(error.to_string()));
            }
            Some(fullname)
        } else {
            None
        };
        let receiver = match daemon.browse(&config.service_type) {
            Ok(receiver) => receiver,
            Err(error) => {
                if let Some(fullname) = &registered_fullname {
                    let _ = daemon.unregister(fullname);
                }
                let _ = daemon.shutdown();
                return Err(DiscoveryError::Mdns(error.to_string()));
            }
        };
        let worker_registry = registry.clone();
        let worker_stop = Arc::clone(&stop);
        let worker = thread::Builder::new()
            .name("bloom-mdns-discovery".into())
            .spawn(move || {
                while !worker_stop.load(Ordering::Relaxed) {
                    match receiver.recv_timeout(Duration::from_millis(250)) {
                        Ok(ServiceEvent::ServiceResolved(info)) => {
                            worker_registry.upsert(candidate_from(info))
                        }
                        Ok(ServiceEvent::ServiceRemoved(_, fullname)) => {
                            worker_registry.remove(&fullname);
                        }
                        Ok(_) => {}
                        Err(flume::RecvTimeoutError::Timeout) => {}
                        Err(flume::RecvTimeoutError::Disconnected) => break,
                    }
                }
            });
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => {
                if let Some(fullname) = &registered_fullname {
                    let _ = daemon.unregister(fullname);
                }
                let _ = daemon.shutdown();
                return Err(DiscoveryError::Mdns(error.to_string()));
            }
        };
        Ok(Self {
            registry,
            daemon: Some(daemon),
            stop,
            worker: Some(worker),
            registered_fullname,
        })
    }

    pub fn registry(&self) -> CandidateRegistry {
        self.registry.clone()
    }
}

impl Drop for MdnsDiscovery {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(daemon) = self.daemon.take() {
            if let Some(fullname) = self.registered_fullname.take() {
                let _ = daemon.unregister(&fullname);
            }
            let _ = daemon.shutdown();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn service_info(
    service_type: &str,
    advertisement: Advertisement,
) -> Result<ServiceInfo, DiscoveryError> {
    if advertisement.port == 0 {
        return Err(DiscoveryError::Advertisement("port must be nonzero".into()));
    }
    let instance = advertisement
        .instance_name
        .unwrap_or_else(|| advertisement.node_id.clone());
    let hostname = advertisement
        .hostname
        .unwrap_or_else(|| format!("bloom-{}.local.", sanitize_label(&advertisement.node_id)));
    let props = [
        ("node_id", advertisement.node_id.as_str()),
        ("protocol", "1"),
        ("tls", if advertisement.https { "1" } else { "0" }),
    ];
    let addresses = advertisement.addresses;
    let address_list = addresses
        .as_ref()
        .map(|items| {
            items
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let info = ServiceInfo::new(
        service_type,
        &instance,
        &hostname,
        address_list,
        advertisement.port,
        &props[..],
    )
    .map_err(|e| DiscoveryError::Advertisement(e.to_string()))?;
    Ok(if addresses.is_none() {
        info.enable_addr_auto()
    } else {
        info
    })
}

fn sanitize_label(value: &str) -> String {
    let label: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let label = label.trim_matches('-');
    if label.is_empty() {
        "node".into()
    } else {
        label.chars().take(50).collect()
    }
}

fn candidate_from(info: ServiceInfo) -> CandidateNode {
    let instance = info.get_fullname().to_owned();
    let hostname = info.get_hostname().to_owned();
    let port = info.get_port();
    let mut addresses: Vec<IpAddr> = info.get_addresses().iter().copied().collect();
    addresses.sort();
    let node_id = info.get_property_val_str("node_id").map(str::to_owned);
    let properties = info
        .get_properties()
        .iter()
        .map(|p| (p.key().to_owned(), p.val_str().to_owned()))
        .collect::<BTreeMap<_, _>>();
    // Never follow arbitrary URLs from TXT records. TXT data can contain an
    // identity claim, but the connection target comes from DNS-SD SRV/A/AAAA.
    let scheme = if properties.get("tls").is_some_and(|value| value == "1") {
        "https"
    } else {
        "http"
    };
    let endpoint = addresses
        .first()
        .map(|ip| format!("{scheme}://{}", SocketAddr::new(*ip, port)))
        .unwrap_or_else(|| format!("{scheme}://{hostname}:{port}"));
    CandidateNode {
        instance,
        node_id,
        hostname,
        port,
        endpoint,
        addresses,
        properties,
        last_seen_unix_ms: unix_ms(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_endpoint_uses_dns_address_and_brackets_ipv6() {
        let info = ServiceInfo::new(
            "_bloom._tcp.local.",
            "peer",
            "peer.local.",
            "2001:db8::1",
            4321,
            &[
                ("endpoint", "http://attacker.invalid:9"),
                ("node_id", "claimed"),
            ][..],
        )
        .unwrap();

        let candidate = candidate_from(info);

        assert_eq!(candidate.endpoint, "http://[2001:db8::1]:4321");
        assert_eq!(
            candidate.properties["endpoint"],
            "http://attacker.invalid:9"
        );
    }
}

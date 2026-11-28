//! Bloom's household link: the peer a Goya TV pairs with to show the
//! household's machines (Goya webOS Launcher, Slice 2).
//!
//! * **Devices** are configured, not discovered: a name, optionally the
//!   URL of the device's `bloomd`, and optionally its MAC address for
//!   Wake-on-LAN. At most [`MAX_DEVICES`].
//! * **Telemetry.** A background thread reads each device's
//!   `GET /bloom/v1/resources` every [`REFRESH_SECS`] seconds and keeps a
//!   one-line summary: `CPU 23% · RAM 48%`, `Offline`, or `No Bloom node`.
//! * **TV fields.** `d1_name` … `d4_name` and `d1_state` … `d4_state`
//!   (text), and `devices` (int). They are pushed to the TV whenever they
//!   change (`goya_lanlink_peer::App::fields`).
//! * **Acts.** `wake N` sends a Wake-on-LAN magic packet for device N
//!   (UDP broadcast, port 9), and `refresh` re-reads every device now.
//!   Sending a packet is not proof the machine woke; the notice says
//!   "sent", and the next readings show whether it came up.

use goya_lanlink_peer::{App, DesktopMessage, FieldValue};
use std::collections::BTreeMap;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const MAX_DEVICES: usize = 4;
pub const REFRESH_SECS: u64 = 5;

/// One configured device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    pub name: String,
    /// Base URL of the device's bloomd, e.g. `https://192.168.1.20:7420`.
    pub bloom_url: Option<String>,
    pub mac: Option<[u8; 6]>,
}

/// What the last reading said about a device.
#[derive(Debug, Clone, PartialEq)]
pub enum Reading {
    Unknown,
    NoNode,
    Offline(String),
    Online { cpu_percent: f64, ram_percent: f64 },
}

impl Reading {
    pub fn summary(&self) -> String {
        match self {
            Reading::Unknown => "Checking…".into(),
            Reading::NoNode => "No Bloom node".into(),
            Reading::Offline(_) => "Offline".into(),
            Reading::Online { cpu_percent, ram_percent } => {
                format!("CPU {}% · RAM {}%", cpu_percent.round() as i64, ram_percent.round() as i64)
            }
        }
    }
}

/// Parse `NAME|URL|MAC`; URL and MAC may be empty or `-`.
pub fn parse_device(spec: &str) -> Result<Device, String> {
    let mut parts = spec.split('|');
    let name = parts.next().unwrap_or("").trim().to_string();
    if name.is_empty() || name.chars().count() > 32 {
        return Err(format!("device {spec:?}: the name must be 1–32 characters"));
    }
    let opt = |s: Option<&str>| s.map(str::trim).filter(|s| !s.is_empty() && *s != "-").map(str::to_string);
    let bloom_url = opt(parts.next());
    if let Some(u) = &bloom_url {
        let uri: ureq::http::Uri = u.parse().map_err(|_| format!("device {name}: invalid Bloom URL"))?;
        let host = uri.host().unwrap_or("");
        let loopback = matches!(host, "127.0.0.1" | "localhost" | "[::1]");
        let scheme = uri.scheme_str().unwrap_or("");
        if host.is_empty() || uri.authority().is_some_and(|a| a.as_str().contains('@'))
            || uri.query().is_some() || u.contains('#')
            || !(scheme == "https" || (scheme == "http" && loopback)) {
            return Err(format!("device {name}: bloomd requires HTTPS, or HTTP on loopback only ({u})"));
        }
    }
    let mac = opt(parts.next()).map(|m| parse_mac(&m)).transpose().map_err(|e| format!("device {name}: {e}"))?;
    if parts.next().is_some() {
        return Err(format!("device {spec:?}: expected NAME|URL|MAC"));
    }
    Ok(Device { name, bloom_url, mac })
}

pub fn parse_mac(s: &str) -> Result<[u8; 6], String> {
    let parts: Vec<&str> = s.split([':', '-']).collect();
    if parts.len() != 6 {
        return Err(format!("{s:?} is not a MAC address"));
    }
    let mut mac = [0u8; 6];
    for (i, p) in parts.iter().enumerate() {
        if p.len() != 2 {
            return Err(format!("{s:?} is not a MAC address"));
        }
        mac[i] = u8::from_str_radix(p, 16).map_err(|_| format!("{s:?} is not a MAC address"))?;
    }
    Ok(mac)
}

/// The Wake-on-LAN magic packet: six 0xFF bytes, then the MAC 16 times.
pub fn magic_packet(mac: [u8; 6]) -> [u8; 102] {
    let mut p = [0xffu8; 102];
    for i in 0..16 {
        p[6 + i * 6..12 + i * 6].copy_from_slice(&mac);
    }
    p
}

fn send_wake(mac: [u8; 6]) -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_broadcast(true)?;
    socket.send_to(&magic_packet(mac), "255.255.255.255:9")?;
    Ok(())
}

/// Read a capacity vector (`GET /bloom/v1/resources`) into a reading.
pub fn reading_from_resources(v: &serde_json::Value) -> Reading {
    let cpu = v.pointer("/cpu/utilisation_percent").and_then(serde_json::Value::as_f64);
    let total = v.pointer("/memory/total_bytes").and_then(serde_json::Value::as_f64);
    let avail = v.pointer("/memory/available_bytes").and_then(serde_json::Value::as_f64);
    match (cpu, total, avail) {
        (Some(cpu), Some(total), Some(avail)) if total > 0.0 => Reading::Online {
            cpu_percent: cpu.clamp(0.0, 100.0),
            ram_percent: ((total - avail) / total * 100.0).clamp(0.0, 100.0),
        },
        _ => Reading::Offline("unreadable capacity vector".into()),
    }
}

/// How the household reads one device. A trait so tests need no network.
pub trait Probe: Send + Sync + 'static {
    fn read(&self, device: &Device) -> Reading;
}

/// Reads bloomd over HTTP(S) with an optional bearer token.
pub struct HttpProbe {
    agent: ureq::Agent,
    token: Option<String>,
}

impl HttpProbe {
    pub fn new(token: Option<String>, ca_pem: Option<&[u8]>) -> Result<Self, String> {
        let mut tls = ureq::tls::TlsConfig::builder();
        if let Some(pem) = ca_pem {
            let mut certs = Vec::new();
            for item in ureq::tls::parse_pem(pem) {
                match item.map_err(|e| format!("invalid CA bundle: {e}"))? {
                    ureq::tls::PemItem::Certificate(c) => certs.push(c.to_owned()),
                    _ => return Err("CA bundle must contain certificates only".into()),
                }
            }
            if certs.is_empty() { return Err("CA bundle contains no certificates".into()); }
            tls = tls.root_certs(ureq::tls::RootCerts::from(certs));
        }
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(3)))
            .http_status_as_error(true)
            .max_redirects(0)
            .tls_config(tls.build())
            .build()
            .into();
        Ok(HttpProbe { agent, token })
    }
}

impl Probe for HttpProbe {
    fn read(&self, device: &Device) -> Reading {
        let Some(base) = &device.bloom_url else { return Reading::NoNode };
        let url = format!("{}/bloom/v1/resources", base.trim_end_matches('/'));
        let mut req = self.agent.get(&url);
        if let Some(t) = &self.token {
            req = req.header("Authorization", &format!("Bearer {t}"));
        }
        match req.call() {
            Ok(resp) if !resp.status().is_success() => Reading::Offline(format!("HTTP {}", resp.status())),
            Ok(mut resp) => match resp.body_mut().read_json::<serde_json::Value>() {
                Ok(v) => reading_from_resources(&v),
                Err(e) => Reading::Offline(e.to_string()),
            },
            Err(e) => Reading::Offline(e.to_string()),
        }
    }
}

/// The household state shared by the refresher and the link.
pub struct Household {
    devices: Vec<Device>,
    readings: Mutex<Vec<Reading>>,
    probe: Box<dyn Probe>,
}

impl Household {
    pub fn new(devices: Vec<Device>, probe: Box<dyn Probe>) -> Result<Arc<Self>, String> {
        if devices.is_empty() || devices.len() > MAX_DEVICES {
            return Err(format!("configure 1 to {MAX_DEVICES} devices, not {}", devices.len()));
        }
        let readings = Mutex::new(vec![Reading::Unknown; devices.len()]);
        Ok(Arc::new(Household { devices, readings, probe }))
    }

    pub fn refresh(&self) {
        let fresh: Vec<Reading> = self.devices.iter().map(|d| self.probe.read(d)).collect();
        *self.readings.lock().unwrap() = fresh;
    }

    /// Refresh every [`REFRESH_SECS`] seconds in a background thread.
    pub fn spawn_refresher(self: &Arc<Self>) {
        let h = Arc::clone(self);
        std::thread::spawn(move || loop {
            h.refresh();
            std::thread::sleep(Duration::from_secs(REFRESH_SECS));
        });
    }

    pub fn fields(&self) -> BTreeMap<String, FieldValue> {
        let readings = self.readings.lock().unwrap();
        let mut f = BTreeMap::new();
        for i in 0..MAX_DEVICES {
            let (name, state) = match (self.devices.get(i), readings.get(i)) {
                (Some(d), Some(r)) => (d.name.clone(), r.summary()),
                _ => (String::new(), String::new()),
            };
            let level = match readings.get(i) {
                Some(Reading::Online { cpu_percent, .. }) => (cpu_percent / 25.0).ceil() as i64,
                _ => -1,
            };
            f.insert(format!("d{}_cpu", i + 1), FieldValue::Int(level.clamp(-1, 4)));
            f.insert(format!("d{}_wake", i + 1), FieldValue::Bool(self.devices.get(i).is_some_and(|d| d.mac.is_some())));
            f.insert(format!("d{}_name", i + 1), FieldValue::Text(name));
            f.insert(format!("d{}_state", i + 1), FieldValue::Text(state));
        }
        f.insert("devices".into(), FieldValue::Int(self.devices.len() as i64));
        f
    }
}

/// The link's application: [`Household`] behind `goya.lanlink/0`.
pub struct HouseholdLink {
    pub household: Arc<Household>,
    /// Replaced in tests; sends the packet in production.
    pub waker: fn([u8; 6]) -> std::io::Result<()>,
}

impl HouseholdLink {
    pub fn new(household: Arc<Household>) -> Self {
        HouseholdLink { household, waker: send_wake }
    }
}

impl App for HouseholdLink {
    fn fields(&mut self) -> BTreeMap<String, FieldValue> {
        self.household.fields()
    }

    fn act(&mut self, _presentation_id: Option<&str>, name: &str, arg: Option<&str>) -> DesktopMessage {
        let refuse = |reason: String| DesktopMessage::Refused { reason };
        match name {
            "refresh" => {
                self.household.refresh();
                DesktopMessage::Fields { values: self.household.fields(), notice: Some("Readings refreshed".into()) }
            }
            "wake" => {
                let n = arg.and_then(|a| a.parse::<usize>().ok()).filter(|n| (1..=MAX_DEVICES).contains(n));
                let Some(device) = n.and_then(|n| self.household.devices.get(n - 1)) else {
                    return refuse(format!("no device {}", arg.unwrap_or("")));
                };
                let Some(mac) = device.mac else {
                    return refuse(format!("{} has no MAC address configured", device.name));
                };
                match (self.waker)(mac) {
                    Ok(()) => DesktopMessage::Fields {
                        values: self.household.fields(),
                        notice: Some(format!("Wake-on-LAN sent to {}", device.name)),
                    },
                    Err(e) => refuse(format!("could not send Wake-on-LAN: {e}")),
                }
            }
            _ => refuse(format!("the household link has no act {name}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed;
    impl Probe for Fixed {
        fn read(&self, d: &Device) -> Reading {
            match d.name.as_str() {
                "Desk" => Reading::Online { cpu_percent: 23.4, ram_percent: 47.6 },
                "Tablet" => Reading::Offline("timeout".into()),
                _ => Reading::NoNode,
            }
        }
    }

    fn devices() -> Vec<Device> {
        vec![
            parse_device("Desk|https://192.168.1.20:7420|aa:bb:cc:dd:ee:ff").unwrap(),
            parse_device("Tablet|https://192.168.1.21:7420|").unwrap(),
            parse_device("Phone").unwrap(),
        ]
    }

    #[test]
    fn devices_parse_strictly() {
        let d = parse_device("Desk|https://192.168.1.20:7420|aa:bb:cc:dd:ee:ff").unwrap();
        assert_eq!(d.mac, Some([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]));
        assert_eq!(parse_device("Local|http://127.0.0.1:7420|-").unwrap().mac, None);
        assert!(parse_device("Desk|http://192.168.1.20:7420").unwrap_err().contains("HTTPS"));
        assert!(parse_device("Desk||aa:bb:cc").is_err());
        assert!(parse_device("|https://x").is_err());
        assert!(parse_device("a|b|c|d").is_err());
        for url in ["http://localhost.evil:7420", "http://127.0.0.10:7420", "http://127.0.0.1@evil:7420", "https://", "https://desk/#x", "https://desk/?token=x"] {
            assert!(parse_device(&format!("Desk|{url}")).is_err(), "{url}");
        }
        assert!(parse_device("Desk|http://[::1]:7420").is_ok());
    }

    #[test]
    fn ca_bundle_must_contain_certificates() {
        assert!(HttpProbe::new(None, Some(b"not a certificate")).is_err());
        assert!(HttpProbe::new(None, None).is_ok());
    }

    #[test]
    fn magic_packet_layout() {
        let p = magic_packet([1, 2, 3, 4, 5, 6]);
        assert_eq!(&p[..6], &[0xff; 6]);
        assert_eq!(&p[6..12], &[1, 2, 3, 4, 5, 6]);
        assert_eq!(&p[96..], &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn capacity_vectors_become_summaries() {
        let v = serde_json::json!({"cpu": {"utilisation_percent": 8.2}, "memory": {"total_bytes": 1000, "available_bytes": 680}});
        assert_eq!(reading_from_resources(&v).summary(), "CPU 8% · RAM 32%");
        assert_eq!(reading_from_resources(&serde_json::json!({})).summary(), "Offline");
    }

    #[test]
    fn fields_cover_every_row() {
        let h = Household::new(devices(), Box::new(Fixed)).unwrap();
        assert_eq!(h.fields()["d1_state"], FieldValue::Text("Checking…".into()));
        h.refresh();
        let f = h.fields();
        assert_eq!(f["d1_name"], FieldValue::Text("Desk".into()));
        assert_eq!(f["d1_state"], FieldValue::Text("CPU 23% · RAM 48%".into()));
        assert_eq!(f["d2_state"], FieldValue::Text("Offline".into()));
        assert_eq!(f["d3_state"], FieldValue::Text("No Bloom node".into()));
        assert_eq!(f["d4_name"], FieldValue::Text(String::new()));
        assert_eq!(f["devices"], FieldValue::Int(3));
        assert_eq!(f["d1_cpu"], FieldValue::Int(1));
        assert_eq!(f["d2_cpu"], FieldValue::Int(-1));
        assert_eq!(f["d1_wake"], FieldValue::Bool(true));
        assert_eq!(f["d2_wake"], FieldValue::Bool(false));
        assert!(Household::new(vec![], Box::new(Fixed)).is_err());
    }

    #[test]
    fn wake_needs_a_mac_and_reports_sent_not_woken() {
        let h = Household::new(devices(), Box::new(Fixed)).unwrap();
        let mut link = HouseholdLink { household: h, waker: |_| Ok(()) };
        match link.act(None, "wake", Some("1")) {
            DesktopMessage::Fields { notice, .. } => assert_eq!(notice.as_deref(), Some("Wake-on-LAN sent to Desk")),
            m => panic!("{m:?}"),
        }
        assert_eq!(
            link.act(None, "wake", Some("2")),
            DesktopMessage::Refused { reason: "Tablet has no MAC address configured".into() }
        );
        assert!(matches!(link.act(None, "wake", Some("9")), DesktopMessage::Refused { .. }));
        assert!(matches!(link.act(None, "reboot", None), DesktopMessage::Refused { .. }));
    }
}

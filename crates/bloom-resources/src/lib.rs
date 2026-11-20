use bloom_core::capacity::{
    CpuCapacity, GpuCapacity, InferenceCapacity, MemoryCapacity, ModelCapacityEntry,
    NetworkCapacity, NpuCapacity, PowerCapacity, PowerPolicy, StorageCapacity, ThermalCapacity,
};
use bloom_core::node::AvailabilityClass;
use bloom_core::{CapacityVector, NodeId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct HostFacts {
    pub hostname: Option<String>,
    pub os: Option<String>,
    pub kernel: Option<String>,
}

pub fn host_facts() -> HostFacts {
    HostFacts {
        hostname: sysinfo::System::host_name().ok(),
        os: sysinfo::System::name().ok(),
        kernel: sysinfo::System::kernel_version().ok(),
    }
}

pub struct TelemetryCollector {
    node: NodeId,
    availability: AvailabilityClass,
    storage_dir: Option<PathBuf>,
    system: sysinfo::System,
}

impl TelemetryCollector {
    pub fn new(
        node: NodeId,
        availability: AvailabilityClass,
        storage_dir: Option<PathBuf>,
    ) -> Result<Self, sysinfo::Error> {
        let refresh = sysinfo::RefreshKind::nothing()
            .with_cpu(sysinfo::CpuRefreshKind::everything())
            .with_memory(sysinfo::MemoryRefreshKind::everything());
        Ok(Self {
            node,
            availability,
            storage_dir,
            system: sysinfo::System::new_with_specifics(refresh)?,
        })
    }

    pub fn snapshot(
        &mut self,
        models: Vec<ModelCapacityEntry>,
        inference: InferenceCapacity,
        reserved_bytes: u64,
    ) -> CapacityVector {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();

        let cores = self.system.cpus().len() as u32;
        let utilisation = self.system.global_cpu_usage();
        let available_threads = ((100.0 - utilisation.clamp(0.0, 100.0)) / 100.0
            * cores.max(1) as f32)
            .floor()
            .max(0.0) as u32;
        let brand = self
            .system
            .cpus()
            .first()
            .map(|cpu| cpu.brand().trim().to_string())
            .unwrap_or_default();

        let model_bytes = models
            .iter()
            .map(|entry| entry.size_bytes.unwrap_or(0))
            .fold(0u64, u64::saturating_add);
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let (storage_available, storage_mount) = match disks {
            Ok(disks) => storage_for(&disks, self.storage_dir.as_deref()),
            Err(_) => (0, None),
        };
        let components = sysinfo::Components::new_with_refreshed_list();
        let temperature = match components {
            Ok(components) => components
                .iter()
                .filter_map(|component| {
                    let label = component.label().to_ascii_lowercase();
                    let relevant = label.contains("cpu")
                        || label.contains("soc")
                        || label.contains("package")
                        || label.contains("acpi");
                    if relevant {
                        component.temperature()
                    } else {
                        None
                    }
                })
                .fold(None::<f32>, |acc, value| match acc {
                    Some(current) if current >= value => acc,
                    _ => Some(value),
                }),
            Err(_) => None,
        };

        CapacityVector {
            node: self.node.clone(),
            updated_at_ms: bloom_core::capacity::unix_millis(),
            availability: self.availability.clone(),
            cpu: CpuCapacity {
                arch: std::env::consts::ARCH.to_string(),
                brand,
                logical_cores: cores,
                utilisation_percent: utilisation,
                available_threads,
            },
            gpus: detect_gpus(),
            npu: detect_npu(),
            memory: MemoryCapacity {
                total_bytes: self.system.total_memory(),
                available_bytes: self.system.available_memory(),
                reserved_bytes,
            },
            storage: StorageCapacity {
                available_bytes: storage_available,
                model_bytes,
                models_dir: self
                    .storage_dir
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .or(storage_mount),
            },
            models,
            network: NetworkCapacity {
                lan_reachable: true,
                latency_ms_hint: None,
                throughput_mbps_hint: None,
            },
            inference,
            thermal: ThermalCapacity {
                temperature_celsius: temperature,
                throttling: temperature.is_some_and(|t| t >= 85.0),
            },
            power: power_state(),
        }
    }
}

fn storage_for(disks: &sysinfo::Disks, dir: Option<&Path>) -> (u64, Option<String>) {
    let mut chosen = None;
    if let Some(dir) = dir {
        for disk in disks.iter() {
            let mount = disk.mount_point();
            if dir.starts_with(mount) {
                chosen = Some(disk);
                break;
            }
        }
    }
    let disk = chosen.or_else(|| {
        disks
            .iter()
            .find(|disk| disk.mount_point() == Path::new("/"))
    });
    match disk {
        Some(disk) => (
            disk.available_space(),
            Some(disk.mount_point().display().to_string()),
        ),
        None => {
            let fallback = disks.first();
            (
                fallback.map(|d| d.available_space()).unwrap_or(0),
                fallback.map(|d| d.mount_point().display().to_string()),
            )
        }
    }
}

const PCI_VENDORS: &[(u16, &str)] = &[(0x10de, "nvidia"), (0x1002, "amd"), (0x8086, "intel")];

fn detect_gpus() -> Vec<GpuCapacity> {
    let Ok(entries) = std::fs::read_dir("/sys/class/drm") else {
        return Vec::new();
    };
    let mut cards: Vec<_> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("card") && !name.contains('-'))
                .unwrap_or(false)
        })
        .collect();
    cards.sort();
    let mut gpus = Vec::new();
    for card in cards {
        let vendor_path = card.join("device/vendor");
        let vendor_raw = std::fs::read_to_string(&vendor_path).unwrap_or_default();
        let vendor_id =
            u16::from_str_radix(vendor_raw.trim().trim_start_matches("0x"), 16).unwrap_or_default();
        let vendor = PCI_VENDORS
            .iter()
            .find(|(id, _)| *id == vendor_id)
            .map(|(_, name)| name.to_string())
            .unwrap_or_else(|| format!("vendor:{vendor_id:04x}"));
        gpus.push(GpuCapacity {
            name: format!(
                "gpu-{}",
                card.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
            ),
            backend: vendor,
            utilisation_percent: None,
            free_vram_bytes: None,
            total_vram_bytes: None,
        });
    }
    gpus
}

fn detect_npu() -> NpuCapacity {
    let present = std::fs::read_dir("/sys/class/accel")
        .map(|entries| entries.count() > 0)
        .unwrap_or(false);
    NpuCapacity {
        present,
        supported_operations: Vec::new(),
        available_capacity_percent: None,
    }
}

fn power_state() -> PowerCapacity {
    let mut on_battery: Option<bool> = None;
    let mut batteries: HashMap<String, String> = HashMap::new();
    if let Ok(entries) = std::fs::read_dir("/sys/class/power_supply") {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();
            let kind = std::fs::read_to_string(path.join("type")).unwrap_or_default();
            let kind = kind.trim().to_string();
            if kind == "Battery" {
                let status = std::fs::read_to_string(path.join("status"))
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                batteries.insert(name, status);
            }
        }
    }
    if !batteries.is_empty() {
        on_battery = Some(
            batteries
                .values()
                .any(|status| status.eq_ignore_ascii_case("discharging")),
        );
    }
    PowerCapacity {
        on_battery,
        policy: PowerPolicy::Unrestricted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bloom_core::ModelId;
    use bloom_core::capacity::ModelResidency;

    #[test]
    fn snapshot_reports_core_resources() {
        let mut collector = TelemetryCollector::new(
            NodeId::new("test-node"),
            AvailabilityClass::Continuous,
            Some(std::env::temp_dir()),
        )
        .unwrap();
        let vector = collector.snapshot(Vec::new(), InferenceCapacity::default(), 0);
        assert_eq!(vector.node.as_str(), "test-node");
        assert!(vector.updated_at_ms > 0);
        assert!(vector.memory.total_bytes > 0);
        assert!(vector.cpu.logical_cores >= 1);
        assert!(vector.cpu.arch == std::env::consts::ARCH);
    }

    #[test]
    fn model_bytes_aggregate() {
        let mut collector = TelemetryCollector::new(
            NodeId::new("test-node"),
            AvailabilityClass::Continuous,
            None,
        )
        .unwrap();
        let models = vec![ModelCapacityEntry {
            model: ModelId::new("m"),
            quantisation: None,
            residency: ModelResidency::OnDisk,
            capabilities: vec!["chat".into()],
            size_bytes: Some(1_000),
        }];
        let vector = collector.snapshot(models, InferenceCapacity::default(), 0);
        assert_eq!(vector.storage.model_bytes, 1_000);
    }
}

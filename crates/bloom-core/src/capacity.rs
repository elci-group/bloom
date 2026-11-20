use crate::id::{ModelId, NodeId};
use crate::node::AvailabilityClass;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityVector {
    pub node: NodeId,
    pub updated_at_ms: u64,
    pub availability: AvailabilityClass,
    pub cpu: CpuCapacity,
    #[serde(default)]
    pub gpus: Vec<GpuCapacity>,
    pub npu: NpuCapacity,
    pub memory: MemoryCapacity,
    pub storage: StorageCapacity,
    #[serde(default)]
    pub models: Vec<ModelCapacityEntry>,
    pub network: NetworkCapacity,
    pub inference: InferenceCapacity,
    pub thermal: ThermalCapacity,
    pub power: PowerCapacity,
}

impl CapacityVector {
    pub fn initial(node: NodeId) -> Self {
        Self {
            node,
            updated_at_ms: unix_millis(),
            availability: AvailabilityClass::Continuous,
            cpu: CpuCapacity {
                arch: std::env::consts::ARCH.to_string(),
                brand: String::new(),
                logical_cores: 0,
                utilisation_percent: 0.0,
                available_threads: 0,
            },
            gpus: Vec::new(),
            npu: NpuCapacity {
                present: false,
                supported_operations: Vec::new(),
                available_capacity_percent: None,
            },
            memory: MemoryCapacity {
                total_bytes: 0,
                available_bytes: 0,
                reserved_bytes: 0,
            },
            storage: StorageCapacity {
                available_bytes: 0,
                model_bytes: 0,
                models_dir: None,
            },
            models: Vec::new(),
            network: NetworkCapacity {
                lan_reachable: false,
                latency_ms_hint: None,
                throughput_mbps_hint: None,
            },
            inference: InferenceCapacity {
                active_requests: 0,
                queue_depth: 0,
                measured_tokens_per_second: None,
            },
            thermal: ThermalCapacity {
                temperature_celsius: None,
                throttling: false,
            },
            power: PowerCapacity {
                on_battery: None,
                policy: PowerPolicy::Unrestricted,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuCapacity {
    pub arch: String,
    pub brand: String,
    pub logical_cores: u32,
    pub utilisation_percent: f32,
    pub available_threads: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuCapacity {
    pub name: String,
    pub backend: String,
    pub utilisation_percent: Option<f32>,
    pub free_vram_bytes: Option<u64>,
    pub total_vram_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NpuCapacity {
    pub present: bool,
    #[serde(default)]
    pub supported_operations: Vec<String>,
    pub available_capacity_percent: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryCapacity {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub reserved_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCapacity {
    pub available_bytes: u64,
    pub model_bytes: u64,
    pub models_dir: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelResidency {
    Resident,
    OnDisk,
    Absent,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCapacityEntry {
    pub model: ModelId,
    pub quantisation: Option<String>,
    pub residency: ModelResidency,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkCapacity {
    pub lan_reachable: bool,
    pub latency_ms_hint: Option<f32>,
    pub throughput_mbps_hint: Option<f32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InferenceCapacity {
    pub active_requests: u32,
    pub queue_depth: u32,
    pub measured_tokens_per_second: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThermalCapacity {
    pub temperature_celsius: Option<f32>,
    pub throttling: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerCapacity {
    pub on_battery: Option<bool>,
    pub policy: PowerPolicy,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PowerPolicy {
    #[default]
    Unrestricted,
    Conservative,
    BatterySaver,
}

pub fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

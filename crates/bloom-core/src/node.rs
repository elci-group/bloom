use crate::id::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub name: String,
    pub version: String,
    pub endpoint: String,
    pub harness: String,
    pub availability: AvailabilityClass,
    #[serde(default)]
    pub modes: NodeModes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityClass {
    Continuous,
    Restricted { reason: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct NodeModes {
    pub direct_inference: bool,
    pub swarm_inference: bool,
}

impl Default for NodeModes {
    fn default() -> Self {
        Self {
            direct_inference: true,
            swarm_inference: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub healthy: bool,
    pub harness_healthy: bool,
    pub draining: bool,
    pub uptime_seconds: u64,
    pub active_jobs: u32,
}

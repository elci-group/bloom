use crate::id::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoutingMode {
    #[default]
    Direct,
    Swarm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingOptions {
    #[serde(default)]
    pub routing: RoutingMode,
    #[serde(default)]
    pub preferred_node: Option<NodeId>,
    #[serde(default = "default_true")]
    pub allow_failover: bool,
    #[serde(default = "default_true")]
    pub allow_model_loading: bool,
    #[serde(default)]
    pub allow_cloud: bool,
}

fn default_true() -> bool {
    true
}

impl Default for RoutingOptions {
    fn default() -> Self {
        Self {
            routing: RoutingMode::Direct,
            preferred_node: None,
            allow_failover: true,
            allow_model_loading: true,
            allow_cloud: false,
        }
    }
}

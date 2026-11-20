use crate::error::HarnessError;
use bloom_core::{ModelId, RequestId};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;

pub type InferenceStream = Pin<Box<dyn Stream<Item = Result<StreamEvent, HarnessError>> + Send>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessHealth {
    pub healthy: bool,
    pub detail: String,
    pub uptime_seconds: Option<u64>,
}

impl HarnessHealth {
    pub fn healthy(detail: impl Into<String>) -> Self {
        Self {
            healthy: true,
            detail: detail.into(),
            uptime_seconds: None,
        }
    }

    pub fn down(detail: impl Into<String>) -> Self {
        Self {
            healthy: false,
            detail: detail.into(),
            uptime_seconds: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCapability {
    Chat,
    Completion,
    Embedding,
    Audio,
    Vision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryModel {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub resident: Option<bool>,
    pub size_bytes: Option<u64>,
    pub quantisation: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<ModelCapability>,
    pub modified_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelInventory {
    pub models: Vec<InventoryModel>,
}

impl ModelInventory {
    pub fn get(&self, model: &str) -> Option<&InventoryModel> {
        self.models
            .iter()
            .find(|m| m.id == model || m.name == model)
    }

    pub fn contains(&self, model: &str) -> bool {
        self.get(model).is_some()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub loaded_models: u32,
    pub resident_memory_bytes: Option<u64>,
    pub active_requests: u32,
    pub queue_depth: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

impl ChatMessage {
    pub fn new(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: content.into(),
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::new("user", content)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::new("assistant", content)
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self::new("system", content)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GenerationParameters {
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub stop: Vec<String>,
    pub seed: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct InferenceRequest {
    pub id: RequestId,
    pub model: ModelId,
    pub messages: Vec<ChatMessage>,
    pub parameters: GenerationParameters,
    pub stream: bool,
}

#[derive(Debug, Clone, Serialize)]
pub enum StreamEvent {
    Start {
        model: ModelId,
    },
    Chunk {
        delta: String,
    },
    Usage {
        prompt_tokens: u64,
        completion_tokens: u64,
    },
    Done {
        finish_reason: String,
    },
    Failed {
        detail: String,
    },
}

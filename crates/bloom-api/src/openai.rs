use bloom_core::RoutingOptions;
use bloom_harness::GenerationParameters;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StopSpec {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<OpenAiMessage>,
    #[serde(default)]
    pub stream: bool,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub stop: Option<StopSpec>,
    pub seed: Option<u64>,
    #[serde(default)]
    pub bloom: RoutingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiMessage {
    pub role: String,
    pub content: String,
}

impl ChatCompletionRequest {
    pub fn stop_tokens(&self) -> Vec<String> {
        match &self.stop {
            Some(StopSpec::One(token)) => vec![token.clone()],
            Some(StopSpec::Many(tokens)) => tokens.clone(),
            None => Vec::new(),
        }
    }

    pub fn parameters(&self) -> GenerationParameters {
        GenerationParameters {
            temperature: self.temperature,
            top_p: self.top_p,
            max_tokens: self.max_tokens,
            stop: self.stop_tokens(),
            seed: self.seed,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

impl Usage {
    pub fn new(prompt_tokens: u64, completion_tokens: u64) -> Self {
        Self {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: &'static str,
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

#[derive(Debug, Clone, Serialize)]
pub struct Choice {
    pub index: u32,
    pub message: OpenAiMessage,
    pub finish_reason: String,
}

pub fn chunk_frame(
    id: &str,
    model: &str,
    delta: serde_json::Value,
    finish_reason: Option<&str>,
    usage: Option<Usage>,
) -> String {
    let mut frame = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": bloom_core::capacity::unix_millis() / 1000,
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason,
        }]
    });
    if let Some(usage) = usage {
        frame["usage"] = serde_json::to_value(usage).unwrap_or_default();
    }
    format!("data: {frame}\n\n")
}

pub fn error_frame(message: &str) -> String {
    let frame = json!({"error": {"message": message}});
    format!("data: {frame}\n\n")
}

pub fn done_frame() -> String {
    "data: [DONE]\n\n".to_string()
}

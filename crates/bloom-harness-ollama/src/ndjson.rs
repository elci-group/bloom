use serde::Deserialize;

#[derive(Debug, Clone)]
pub enum ChatEvent {
    Delta(String),
    Done {
        reason: String,
        prompt_tokens: Option<u64>,
        completion_tokens: Option<u64>,
    },
}

#[derive(Deserialize)]
struct ChatLine {
    #[serde(default)]
    message: Option<ChatLineMessage>,
    #[serde(default)]
    done: Option<bool>,
    #[serde(default)]
    done_reason: Option<String>,
    #[serde(default, rename = "prompt_eval_count")]
    prompt_eval_count: Option<u64>,
    #[serde(default, rename = "eval_count")]
    eval_count: Option<u64>,
}

#[derive(Deserialize)]
struct ChatLineMessage {
    #[serde(default)]
    content: Option<String>,
}

pub fn chat_event(line: &str) -> Option<ChatEvent> {
    let parsed: ChatLine = serde_json::from_str(line.trim()).ok()?;
    if parsed.done == Some(true) {
        return Some(ChatEvent::Done {
            reason: parsed.done_reason.unwrap_or_else(|| "stop".to_string()),
            prompt_tokens: parsed.prompt_eval_count,
            completion_tokens: parsed.eval_count,
        });
    }
    let delta = parsed.message?.content?;
    if delta.is_empty() {
        return None;
    }
    Some(ChatEvent::Delta(delta))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_delta_line() {
        let line = r#"{"model":"qwen2.5:7b","created_at":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":"hello"},"done":false}"#;
        match chat_event(line) {
            Some(ChatEvent::Delta(delta)) => assert_eq!(delta, "hello"),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_done_line_with_usage() {
        let line = r#"{"model":"qwen2.5:7b","created_at":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":12,"eval_count":34}"#;
        match chat_event(line) {
            Some(ChatEvent::Done {
                reason,
                prompt_tokens,
                completion_tokens,
            }) => {
                assert_eq!(reason, "stop");
                assert_eq!(prompt_tokens, Some(12));
                assert_eq!(completion_tokens, Some(34));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn skips_empty_and_garbage_lines() {
        assert!(chat_event("").is_none());
        assert!(chat_event("not json").is_none());
        assert!(chat_event(r#"{"message":{"content":""},"done":false}"#).is_none());
    }
}

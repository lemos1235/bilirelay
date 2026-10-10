use serde_json::{Map, Value, json};

pub fn request_to_chat(payload: &Map<String, Value>) -> Result<Map<String, Value>, String> {
    if let Some(id) = payload.get("previous_response_id").filter(|v| !v.is_null()) {
        return Err(format!(
            "chat_completions upstreams do not support previous_response_id ({id})"
        ));
    }
    if payload
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|t| !t.is_empty())
    {
        return Err("chat_completions upstreams do not currently support tools".to_string());
    }

    let mut messages = Vec::new();
    match payload.get("instructions") {
        None | Some(Value::Null) => {}
        Some(Value::String(text)) => messages.push(json!({ "role": "system", "content": text })),
        Some(_) => return Err("instructions must be a string".to_string()),
    }
    match payload.get("input") {
        None | Some(Value::Null) => {}
        Some(Value::String(text)) => messages.push(json!({ "role": "user", "content": text })),
        Some(Value::Array(items)) => {
            for (index, item) in items.iter().enumerate() {
                messages.push(input_message(item).map_err(|e| format!("input[{index}]: {e}"))?);
            }
        }
        Some(_) => return Err("input must be a string or an array".to_string()),
    }
    if messages.is_empty() {
        return Err(
            "the request contains no messages (both instructions and input are empty)".to_string(),
        );
    }

    let mut chat = Map::new();
    if let Some(model) = payload.get("model") {
        chat.insert("model".to_string(), model.clone());
    }
    chat.insert("messages".to_string(), Value::Array(messages));
    chat.insert("stream".to_string(), Value::Bool(false));
    for key in ["temperature", "top_p", "user"] {
        if let Some(value) = payload.get(key).filter(|v| !v.is_null()) {
            chat.insert(key.to_string(), value.clone());
        }
    }
    if let Some(max) = payload.get("max_output_tokens").filter(|v| !v.is_null()) {
        chat.insert("max_tokens".to_string(), max.clone());
    }
    if let Some(effort) = payload
        .get("reasoning")
        .and_then(|r| r.get("effort"))
        .filter(|v| !v.is_null())
    {
        chat.insert("reasoning_effort".to_string(), effort.clone());
    }
    if let Some(format) = payload.get("text").and_then(|t| t.get("format"))
        && let Some(response_format) = response_format(format)?
    {
        chat.insert("response_format".to_string(), response_format);
    }
    Ok(chat)
}

fn input_message(item: &Value) -> Result<Value, String> {
    let kind = item
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("message");
    if kind != "message" {
        return Err(format!(
            "chat_completions upstreams do not support input of type '{kind}'"
        ));
    }
    let role = match item.get("role").and_then(Value::as_str) {
        Some("developer") => "system",
        Some(role @ ("system" | "user" | "assistant")) => role,
        other => return Err(format!("unsupported role {other:?}")),
    };
    let content = match item.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => {
            let mut texts = Vec::with_capacity(parts.len());
            for part in parts {
                match part.get("type").and_then(Value::as_str) {
                    Some("input_text" | "output_text" | "text") => {
                        texts.push(part.get("text").and_then(Value::as_str).unwrap_or_default())
                    }
                    other => {
                        return Err(format!(
                            "chat_completions upstreams do not support content of type {other:?}"
                        ));
                    }
                }
            }
            texts.join("\n")
        }
        _ => return Err("content must be a string or an array of content items".to_string()),
    };
    Ok(json!({ "role": role, "content": content }))
}

fn response_format(format: &Value) -> Result<Option<Value>, String> {
    match format.get("type").and_then(Value::as_str) {
        None | Some("text") => Ok(None),
        Some("json_object") => Ok(Some(json!({ "type": "json_object" }))),
        Some("json_schema") => {
            let mut schema = Map::new();
            for key in ["name", "description", "schema", "strict"] {
                if let Some(value) = format.get(key) {
                    schema.insert(key.to_string(), value.clone());
                }
            }
            Ok(Some(
                json!({ "type": "json_schema", "json_schema": schema }),
            ))
        }
        Some(other) => Err(format!("unsupported text.format.type '{other}'")),
    }
}

/// Convert a standard Chat Completions response or a successful Cline data envelope.
pub fn response_from_chat(body: &[u8]) -> Option<Vec<u8>> {
    let chat: Value = serde_json::from_slice(body).ok()?;
    let chat = if chat.get("choices").is_some() {
        &chat
    } else if chat.get("success").and_then(Value::as_bool) == Some(true) {
        chat.get("data")?
    } else {
        return None;
    };
    let choice = chat.get("choices")?.get(0)?;
    let message = choice.get("message")?;
    let id = chat.get("id").and_then(Value::as_str).unwrap_or("chat");

    let text = match message.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|p| p.get("text").and_then(Value::as_str))
            .collect(),
        Some(Value::Null) | None => String::new(),
        Some(_) => return None,
    };
    let reasoning = ["reasoning_content", "reasoning"]
        .iter()
        .find_map(|key| message.get(*key).and_then(Value::as_str))
        .filter(|r| !r.is_empty());
    let refusal = message
        .get("refusal")
        .and_then(Value::as_str)
        .filter(|r| !r.is_empty());

    let incomplete_reason = match choice.get("finish_reason").and_then(Value::as_str) {
        Some("length") => Some("max_output_tokens"),
        Some("content_filter") => Some("content_filter"),
        _ => None,
    };
    let status = if incomplete_reason.is_some() {
        "incomplete"
    } else {
        "completed"
    };

    let mut output = Vec::new();
    if let Some(reasoning) = reasoning {
        output.push(json!({
            "type": "reasoning",
            "id": format!("rs_{id}"),
            "summary": [],
            "content": [{ "type": "reasoning_text", "text": reasoning }],
        }));
    }
    let content = if let Some(refusal) = refusal {
        Some(json!([{ "type": "refusal", "refusal": refusal }]))
    } else if !text.is_empty() || reasoning.is_none() {
        Some(json!([{ "type": "output_text", "text": text, "annotations": [] }]))
    } else {
        None
    };
    if let Some(content) = content {
        output.push(json!({
            "type": "message",
            "id": format!("msg_{id}"),
            "status": status,
            "role": "assistant",
            "content": content,
        }));
    }

    let response = json!({
        "id": format!("resp_{id}"),
        "object": "response",
        "created_at": chat.get("created").cloned().unwrap_or(Value::Null),
        "status": status,
        "incomplete_details": incomplete_reason.map(|reason| json!({ "reason": reason })),
        "error": null,
        "model": chat.get("model").cloned().unwrap_or(Value::Null),
        "output": output,
        "usage": chat.get("usage").map(usage_from_chat),
    });
    serde_json::to_vec(&response).ok()
}

fn usage_from_chat(usage: &Value) -> Value {
    let count = |pointer: &str| usage.pointer(pointer).and_then(Value::as_u64).unwrap_or(0);
    let input = count("/prompt_tokens");
    let output = count("/completion_tokens");
    let total = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(input + output);
    json!({
        "input_tokens": input,
        "input_tokens_details": { "cached_tokens": count("/prompt_tokens_details/cached_tokens") },
        "output_tokens": output,
        "output_tokens_details": { "reasoning_tokens": count("/completion_tokens_details/reasoning_tokens") },
        "total_tokens": total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chat_request(payload: Value) -> Result<Value, String> {
        request_to_chat(payload.as_object().unwrap()).map(Value::Object)
    }

    fn responses(chat: Value) -> Value {
        serde_json::from_slice(&response_from_chat(chat.to_string().as_bytes()).unwrap()).unwrap()
    }

    #[test]
    fn converts_translation_request() {
        let chat = chat_request(json!({
            "model": "translate",
            "temperature": 0.0,
            "instructions": "You are a translator",
            "input": [
                { "type": "message", "role": "user", "content": "Requirements" },
                { "role": "developer", "content": [{ "type": "input_text", "text": "a" }, { "type": "input_text", "text": "b" }] }
            ],
            "reasoning": { "effort": "low" },
            "max_output_tokens": 8192,
            "store": false,
            "text": { "format": { "type": "text" } }
        }))
        .unwrap();
        assert_eq!(
            chat,
            json!({
                "model": "translate",
                "messages": [
                    { "role": "system", "content": "You are a translator" },
                    { "role": "user", "content": "Requirements" },
                    { "role": "system", "content": "a\nb" }
                ],
                "temperature": 0.0,
                "max_tokens": 8192,
                "reasoning_effort": "low",
                "stream": false
            })
        );
    }

    #[test]
    fn chat_requests_explicitly_disable_streaming() {
        for payload in [
            json!({ "model": "m", "input": "hi" }),
            json!({ "model": "m", "input": "hi", "stream": false }),
        ] {
            let chat = chat_request(payload).unwrap();
            assert_eq!(chat["stream"], false);
        }
    }

    #[test]
    fn string_input_and_json_schema() {
        let chat = chat_request(json!({
            "model": "m",
            "input": "hi",
            "text": { "format": { "type": "json_schema", "name": "out", "schema": { "type": "object" }, "strict": true } }
        }))
        .unwrap();
        assert_eq!(
            chat["messages"],
            json!([{ "role": "user", "content": "hi" }])
        );
        assert_eq!(
            chat["response_format"],
            json!({ "type": "json_schema", "json_schema": { "name": "out", "schema": { "type": "object" }, "strict": true } })
        );
    }

    #[test]
    fn rejects_what_chat_cannot_express() {
        assert!(
            chat_request(json!({ "model": "m", "input": "x", "tools": [{ "type": "function" }] }))
                .is_err()
        );
        assert!(
            chat_request(json!({ "model": "m", "input": "x", "previous_response_id": "resp_1" }))
                .is_err()
        );
        assert!(
            chat_request(json!({ "model": "m", "input": [{ "type": "function_call_output" }] }))
                .is_err()
        );
        assert!(chat_request(json!({ "model": "m", "input": [{ "role": "user", "content": [{ "type": "input_image" }] }] })).is_err());
        assert!(chat_request(json!({ "model": "m" })).is_err());
    }

    #[test]
    fn converts_completed_response_with_reasoning_and_usage() {
        let out = responses(json!({
            "id": "abc",
            "created": 1,
            "model": "deepseek-chat",
            "choices": [{ "index": 0, "finish_reason": "stop",
                "message": { "role": "assistant", "content": "Hello", "reasoning_content": "Let me think" } }],
            "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
                "completion_tokens_details": { "reasoning_tokens": 3 } }
        }));
        assert_eq!(out["status"], "completed");
        assert_eq!(out["incomplete_details"], Value::Null);
        assert_eq!(out["model"], "deepseek-chat");
        assert_eq!(out["output"][0]["type"], "reasoning");
        assert_eq!(out["output"][0]["content"][0]["text"], "Let me think");
        assert_eq!(out["output"][1]["type"], "message");
        assert_eq!(
            out["output"][1]["content"][0],
            json!({ "type": "output_text", "text": "Hello", "annotations": [] })
        );
        assert_eq!(out["usage"]["input_tokens"], 10);
        assert_eq!(out["usage"]["output_tokens"], 5);
        assert_eq!(out["usage"]["total_tokens"], 15);
        assert_eq!(out["usage"]["output_tokens_details"]["reasoning_tokens"], 3);
    }

    #[test]
    fn cline_data_envelope_matches_standard_response() {
        let chat = json!({
            "id": "cline-call",
            "object": "chat.completion",
            "created": 123,
            "model": "minimax/minimax-m2.5",
            "choices": [{ "index": 0, "finish_reason": "stop",
                "message": { "role": "assistant", "content": "Hello", "reasoning": "Thinking" } }],
            "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
                "prompt_tokens_details": { "cached_tokens": 2 },
                "completion_tokens_details": { "reasoning_tokens": 3 } }
        });
        let standard = responses(chat.clone());
        let wrapped = responses(json!({ "success": true, "data": chat }));
        assert_eq!(wrapped, standard);
        assert_eq!(wrapped["id"], "resp_cline-call");
        assert_eq!(wrapped["created_at"], 123);
        assert_eq!(wrapped["model"], "minimax/minimax-m2.5");
        assert_eq!(wrapped["output"][0]["content"][0]["text"], "Thinking");
        assert_eq!(wrapped["output"][1]["content"][0]["text"], "Hello");
        assert_eq!(wrapped["usage"]["input_tokens_details"]["cached_tokens"], 2);
        assert_eq!(
            wrapped["usage"]["output_tokens_details"]["reasoning_tokens"],
            3
        );
        assert_eq!(wrapped["usage"]["total_tokens"], 15);
    }

    #[test]
    fn rejects_failed_or_malformed_data_envelopes() {
        let chat = json!({ "choices": [{ "message": { "content": "Hello" } }] });
        for body in [
            json!({ "success": false, "data": chat.clone() }),
            json!({ "data": chat.clone() }),
            json!({ "success": "true", "data": chat }),
            json!({ "success": true }),
            json!({ "success": true, "data": null }),
            json!({ "success": true, "data": "not a chat response" }),
            json!({ "success": true, "data": { "choices": [] } }),
        ] {
            assert!(response_from_chat(body.to_string().as_bytes()).is_none());
        }
    }

    #[test]
    fn truncated_response_is_incomplete_without_empty_message() {
        let out = responses(json!({
            "id": "x",
            "choices": [{ "finish_reason": "length",
                "message": { "role": "assistant", "content": null, "reasoning_content": "Partial reasoning" } }]
        }));
        assert_eq!(out["status"], "incomplete");
        assert_eq!(out["incomplete_details"]["reason"], "max_output_tokens");
        assert_eq!(out["output"].as_array().unwrap().len(), 1);
        assert_eq!(out["output"][0]["type"], "reasoning");
    }

    #[test]
    fn malformed_chat_response_is_rejected() {
        assert!(response_from_chat(b"not json").is_none());
        assert!(response_from_chat(br#"{"choices":[]}"#).is_none());
        assert!(response_from_chat(br#"{"output":[]}"#).is_none());
    }
}

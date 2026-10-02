//! The tool catalog and its dispatch.
//!
//! Every tool is a thin composition of engine reads/writes from
//! [`Zeron`]; the only logic that lives here is argument resolution (chat
//! by prefix, project by path), sender attribution, and the "how do I
//! deliver a message to a chat in this state" choice the composer makes
//! for humans.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use zeron_doc::SessionCommandPayload;
use zeron_proto::{
    Chat, ChatConfig, HarnessId, ReasoningLevel, RunRequest, SandboxLevel, Session, SessionStatus,
    Space, UserInputAnswer,
};

use crate::transcript::{RenderOptions, RenderedMessage, render_entries};
use crate::zeron::{HarnessInfo, TurnOutcome, Zeron, session_for, short};

/// Default and ceiling for the blocking waits.
const MAX_BATCH: usize = 32;
const DEFAULT_WAIT: Duration = Duration::from_secs(600);
const MAX_WAIT: Duration = Duration::from_secs(3600);
/// Live delegated tasks (Working / AwaitingInput) allowed under one root.
const MAX_LIVE_TASKS: usize = 16;
/// The engine's delegation depth cap (`MAX_DELEGATION_DEPTH`), mirrored for
/// `whoami`.
const MAX_DELEGATION_DEPTH: u8 = 2;
/// A session row older than this is not trusted to still be working
/// (the UI's staleness window): a crashed host must not read as busy forever.
const SESSION_STALE: chrono::Duration = chrono::Duration::seconds(45);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDef {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

pub struct Tools {
    zeron: Arc<Zeron>,
}

fn chat_key_schema(extra: Value) -> Value {
    let mut properties = json!({
        "chat": {
            "type": "string",
            "description": "Chat id, unique id prefix, or exact title."
        }
    });
    if let (Some(base), Some(more)) = (properties.as_object_mut(), extra.as_object()) {
        for (k, v) in more {
            base.insert(k.clone(), v.clone());
        }
    }
    json!({ "type": "object", "properties": properties, "required": ["chat"] })
}

fn catalog() -> Vec<ToolDef> {
    let mut tools = vec![
        ToolDef {
            name: "whoami",
            description: "Which chat and device this server speaks for, plus the engine's workspace mode. Call this first when you need to know your own chat id.",
            input_schema: json!({ "type": "object", "properties": {} }),
        },
        ToolDef {
            name: "list_devices",
            description: "Devices in this workspace (the local engine's device is flagged). Chats and projects are hosted on a device.",
            input_schema: json!({ "type": "object", "properties": {} }),
        },
        ToolDef {
            name: "list_projects",
            description: "Projects: a folder on a device. Each chat belongs to one project, which fixes its host device and working directory.",
            input_schema: json!({ "type": "object", "properties": {} }),
        },
        ToolDef {
            name: "list_harnesses",
            description: "Agent harnesses (claude-code, codex, cursor, …) and whether each is available on this device.",
            input_schema: json!({ "type": "object", "properties": {} }),
        },
        ToolDef {
            name: "list_models",
            description: "Models a harness offers on this device. Model ids are harness-specific strings; pass one to create_chat.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "harness": { "type": "string", "description": "Harness id, e.g. claude-code or codex." }
                },
                "required": ["harness"]
            }),
        },
        ToolDef {
            name: "list_chats",
            description: "Chats with their project, host device, harness/model, live status, and last activity. Newest activity first.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "project": { "type": "string", "description": "Only chats in this project (id, path, or name)." },
                    "device": { "type": "string", "description": "Only chats hosted on this device (id or name)." },
                    "include_archived": { "type": "boolean", "default": false },
                    "parent": { "type": "string", "description": "Only chats created by this chat (id, prefix, or title) — e.g. your own id to list the chats you spawned." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 50 }
                }
            }),
        },
        ToolDef {
            name: "get_chat",
            description: "One chat's metadata, live status, and any question its agent is currently blocked on.",
            input_schema: chat_key_schema(json!({})),
        },
        ToolDef {
            name: "create_chat",
            description: "Create a chat in a project (or project-less on a device) with a harness and model. The new chat records your chat as its parent (parentChatId). Optionally send a first prompt and wait for the reply. Returns the new chat id. For parallel delegation use create_chats, or leave wait=false on every launch and wait only after all chats have been started.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "project": { "type": "string", "description": "Project id, path, or name. Required unless device is given." },
                    "device": { "type": "string", "description": "Host device (id or name) for a project-less chat; defaults to this device." },
                    "parent": { "type": "string", "description": "Parent chat to record (id, prefix, or title). Defaults to the chat you are speaking from." },
                    "harness": { "type": "string", "description": "Harness id (see list_harnesses). Defaults to claude-code when available." },
                    "model": { "type": "string", "description": "Model id from list_models. Omit for the harness default." },
                    "reasoning": { "type": "string", "description": "Reasoning level the model supports (e.g. low, medium, high, max)." },
                    "sandbox": { "type": "string", "enum": ["read-only", "workspace-write", "danger-full-access"], "default": "workspace-write" },
                    "title": { "type": "string", "description": "Sidebar title. Otherwise the engine titles it from the first exchange." },
                    "branch": { "type": "string", "description": "Branch label to record on the chat." },
                    "cwd": { "type": "string", "description": "Working directory override (an existing worktree path). Defaults to the project folder." },
                    "prompt": { "type": "string", "description": "First message to send right away." },
                    "wait": { "type": "boolean", "default": false, "description": "With prompt: block until the first turn finishes and return the reply. For work that takes more than a few seconds, use notify. Some harnesses sometimes move a long tool call to the background and the reply is lost." },
                    "notify": {
                        "type": "boolean",
                        "default": false,
                        "description": "With prompt: return at once. When the chat's work settles (completed, errored, or interrupted), Zeron sends its final message to your chat. The message is steered into your running turn where your harness supports it, and otherwise starts your next turn. Continue with other work or end your turn. Do not poll. Cannot be combined with wait."
                    },
                    "timeout_secs": { "type": "integer", "minimum": 1, "maximum": 3600, "default": 600 }
                }
            }),
        },
        ToolDef {
            name: "read_chat",
            description: "Read a chat's transcript as plain messages (newest window by default). Tool calls are summarized one per line.",
            input_schema: chat_key_schema(json!({
                "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 40, "description": "How many messages, counted from the newest." },
                "offset": { "type": "integer", "minimum": 0, "default": 0, "description": "Skip this many newest messages (paging backwards)." },
                "include_reasoning": { "type": "boolean", "default": false },
                "include_tools": { "type": "boolean", "default": true }
            })),
        },
        ToolDef {
            name: "send_message",
            description: "Send a message to a chat. Messages are attributed to your chat. mode 'auto' starts a turn when the chat is idle, steers a running turn through its live mailbox (at the next supported input boundary without interrupting the agent). Use mode 'queue' only to explicitly hold a message for later. With wait=true, blocks until the turn finishes and returns the assistant's reply. For parallel work use send_messages, or send to all chats with wait=false before waiting.",
            input_schema: chat_key_schema(json!({
                "text": { "type": "string" },
                "mode": { "type": "string", "enum": ["auto", "run", "steer", "queue"], "default": "auto" },
                "wait": { "type": "boolean", "default": false, "description": "Block until the turn finishes and return the reply. For work that takes more than a few seconds, use notify. Some harnesses sometimes move a long tool call to the background and the reply is lost." },
                "notify": { "type": "boolean", "default": false, "description": "Arm the task for this turn: when it settles, Zeron sends its final message to your chat. Only for tasks you delegated. Cannot be combined with wait or mode 'queue'." },
                "timeout_secs": { "type": "integer", "minimum": 1, "maximum": 3600, "default": 600 }
            })),
        },
        ToolDef {
            name: "wait_for_turn",
            description: "Block until a chat is no longer working: returns completed, awaitingInput (answer with respond_to_input), errored, or timedOut, with the newest assistant message. For work that takes more than a few seconds, use notify. Some harnesses sometimes move a long tool call to the background and the reply is lost.",
            input_schema: chat_key_schema(json!({
                "timeout_secs": { "type": "integer", "minimum": 1, "maximum": 3600, "default": 600 }
            })),
        },
        ToolDef {
            name: "interrupt_chat",
            description: "Stop the chat's running turn.",
            input_schema: chat_key_schema(json!({})),
        },
        ToolDef {
            name: "respond_to_input",
            description: "Answer a question the chat's agent is blocked on (see get_chat / wait_for_turn pendingInput). Labels are the option strings, or free text for open questions.",
            input_schema: chat_key_schema(json!({
                "request_id": { "type": "string", "description": "The pending request id. Defaults to the chat's current pending question." },
                "answers": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "question_id": { "type": "string" },
                            "labels": { "type": "array", "items": { "type": "string" } }
                        },
                        "required": ["question_id", "labels"]
                    }
                }
            })),
        },
        ToolDef {
            name: "task_status",
            description: "The tasks you delegated (with the tasks they delegated): state, notice state, and — for one chat — its final message.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "chat": { "type": "string", "description": "A delegated task (id, prefix, or title). Omit to list every task you delegated." },
                    "reply_chars": { "type": "integer", "minimum": 0, "maximum": 100000, "default": 8000, "description": "With chat: how much of the task's final message to return." }
                }
            }),
        },
        ToolDef {
            name: "task_cancel",
            description: "Stop a delegated task and every task below it. Sends no notice.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "chat": { "type": "string", "description": "A delegated task (id, prefix, or title)." }
                },
                "required": ["chat"]
            }),
        },
        ToolDef {
            name: "archive_chat",
            description: "Archive a chat (hide it from the active sidebar list); pass archived=false to restore. Use it to tidy up chats you created.",
            input_schema: chat_key_schema(json!({
                "archived": { "type": "boolean", "default": true }
            })),
        },
    ];
    for (name, single, description) in [
        (
            "create_chats",
            "create_chat",
            "Create multiple independent side chats concurrently. Put each chat's prompt in its request to start all work together. Prefer this for parallel delegation, including harnesses that execute tool calls sequentially. Each request has create_chat arguments; wait defaults to false. Results preserve request order and include per-request errors; successful requests are not rolled back.",
        ),
        (
            "send_messages",
            "send_message",
            "Send messages to multiple independent chats concurrently. Prefer this to delegate parallel work to existing chats. Each request has send_message arguments; wait defaults to false. If wait=true, requests still run concurrently. Results preserve request order and include per-request errors; successful sends are not rolled back. Do not include dependent messages to the same chat.",
        ),
    ] {
        let item_schema = tools
            .iter()
            .find(|tool| tool.name == single)
            .unwrap()
            .input_schema
            .clone();
        tools.push(ToolDef {
            name, description,
            input_schema: json!({
                "type": "object",
                "properties": {"requests": {"type": "array", "minItems": 1, "maxItems": MAX_BATCH, "items": item_schema}},
                "required": ["requests"],
            }),
        });
    }
    tools
}

// ---- argument shapes ---------------------------------------------------------

#[derive(Deserialize)]
struct BatchArgs {
    requests: Vec<Value>,
}

#[derive(Deserialize)]
struct ChatArgs {
    chat: String,
}

#[derive(Deserialize)]
struct ListModelsArgs {
    harness: String,
}

#[derive(Deserialize, Default)]
struct ListChatsArgs {
    project: Option<String>,
    device: Option<String>,
    #[serde(default)]
    include_archived: bool,
    parent: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize, Default)]
struct CreateChatArgs {
    project: Option<String>,
    device: Option<String>,
    parent: Option<String>,
    harness: Option<String>,
    model: Option<String>,
    reasoning: Option<String>,
    sandbox: Option<String>,
    title: Option<String>,
    branch: Option<String>,
    cwd: Option<String>,
    prompt: Option<String>,
    #[serde(default)]
    wait: bool,
    #[serde(default)]
    notify: bool,
    timeout_secs: Option<u64>,
}

#[derive(Deserialize)]
struct ReadChatArgs {
    chat: String,
    limit: Option<usize>,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    include_reasoning: bool,
    include_tools: Option<bool>,
}

#[derive(Deserialize)]
struct SendArgs {
    chat: String,
    text: String,
    mode: Option<String>,
    #[serde(default)]
    wait: bool,
    #[serde(default)]
    notify: bool,
    timeout_secs: Option<u64>,
}

#[derive(Deserialize, Default)]
struct TaskStatusArgs {
    chat: Option<String>,
    reply_chars: Option<usize>,
}

#[derive(Deserialize)]
struct WaitArgs {
    chat: String,
    timeout_secs: Option<u64>,
}

#[derive(Deserialize)]
struct ArchiveArgs {
    chat: String,
    archived: Option<bool>,
}

#[derive(Deserialize)]
struct RespondArgs {
    chat: String,
    request_id: Option<String>,
    #[serde(default)]
    answers: Vec<AnswerArg>,
}

#[derive(Deserialize)]
struct AnswerArg {
    question_id: String,
    labels: Vec<String>,
}

fn parse<T: serde::de::DeserializeOwned>(args: Value) -> Result<T, String> {
    serde_json::from_value(args).map_err(|e| format!("invalid arguments: {e}"))
}

fn wait_duration(secs: Option<u64>) -> Duration {
    secs.map(Duration::from_secs)
        .unwrap_or(DEFAULT_WAIT)
        .min(MAX_WAIT)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn parse_enum<T: serde::de::DeserializeOwned>(what: &str, raw: &str) -> Result<T, String> {
    serde_json::from_value(Value::String(raw.trim().to_owned()))
        .map_err(|_| format!("unknown {what}: {raw:?}"))
}

/// Title/request-id text that lands inside a notice header or a delegated
/// task's prompt line: no line breaks or markup metacharacters, 80 chars.
/// (Same rule as `sanitize_meta` in zeron-engine's delegation module.)
fn sanitize_meta(raw: &str) -> String {
    raw.chars()
        .filter(|c| !matches!(c, '\n' | '\r' | '<' | '>' | '[' | ']' | '"'))
        .take(80)
        .collect()
}

// ---- summaries ---------------------------------------------------------------

/// Live posture of one chat as the tools report it.
fn status_of(session: Option<&Session>) -> (String, Option<i64>) {
    let Some(session) = session else {
        return ("idle".into(), None);
    };
    let age = chrono::Utc::now() - session.updated_at;
    let stale = age > SESSION_STALE;
    let label = match session.status {
        SessionStatus::Working if stale => "working (stale — host may be offline)",
        SessionStatus::Working => "working",
        SessionStatus::AwaitingInput => "awaitingInput",
        SessionStatus::Errored => "errored",
        SessionStatus::Idle => "idle",
    };
    (label.into(), Some(age.num_seconds().max(0)))
}

fn summarize_chat(chat: &Chat, spaces: &[Space], sessions: &[Session]) -> Value {
    let space = chat
        .space_id
        .as_deref()
        .and_then(|id| spaces.iter().find(|s| s.id == id));
    let session = session_for(sessions, chat);
    let (status, status_age) = status_of(session.as_ref());
    json!({
        "id": chat.id,
        "title": chat.title,
        "project": space.map(|s| json!({
            "id": s.id,
            "name": s.display_name(),
            "path": s.path,
        })),
        "deviceId": chat.device_id,
        "cwd": chat.cwd.clone().or_else(|| space.map(|s| s.path.clone())),
        "branch": chat.branch,
        "harness": chat.config.as_ref().map(|c| c.harness),
        "model": chat.config.as_ref().and_then(|c| c.model.clone()),
        "reasoning": chat.config.as_ref().and_then(|c| c.reasoning),
        "archived": chat.archived,
        "parentChatId": chat.parent_chat_id,
        "delegatedBy": chat.delegation.as_ref().map(|d| d.by.clone()),
        "depth": chat.delegation.as_ref().map(|d| d.depth),
        "status": status,
        "statusAgeSecs": status_age,
        "lastMessageAt": chat.last_message_at,
        "lastMessagePreview": chat.last_message_preview,
        "createdAt": chat.created_at,
    })
}

fn last_pending_input(messages: &[RenderedMessage]) -> Option<Value> {
    messages.iter().rev().find_map(|m| m.pending_input.clone())
}

// ---- dispatch ----------------------------------------------------------------

impl Tools {
    pub fn new(zeron: Arc<Zeron>) -> Self {
        Self { zeron }
    }

    pub fn list(&self) -> Vec<ToolDef> {
        catalog()
    }

    pub fn has(&self, name: &str) -> bool {
        catalog().iter().any(|t| t.name == name)
    }

    /// `Ok` is the tool's structured result; `Err` is a message the model
    /// should read (surfaced as `isError`, never as a protocol error).
    pub async fn call(&self, name: &str, args: Value) -> Result<Value, String> {
        let result = match name {
            "whoami" => self.whoami().await,
            "list_devices" => self.list_devices().await,
            "list_projects" => self.list_projects().await,
            "list_harnesses" => self.list_harnesses().await,
            "list_models" => self.list_models(parse(args)?).await,
            "list_chats" => self.list_chats(parse(args)?).await,
            "get_chat" => self.get_chat(parse(args)?).await,
            "create_chat" => self.create_chat(parse(args)?).await,
            "create_chats" => self.batch(parse(args)?, true).await,
            "send_messages" => self.batch(parse(args)?, false).await,
            "read_chat" => self.read_chat(parse(args)?).await,
            "send_message" => self.send_message(parse(args)?).await,
            "wait_for_turn" => self.wait_for_turn(parse(args)?).await,
            "interrupt_chat" => self.interrupt_chat(parse(args)?).await,
            "respond_to_input" => self.respond_to_input(parse(args)?).await,
            "task_status" => self.task_status(parse(args)?).await,
            "task_cancel" => self.task_cancel(parse(args)?).await,
            "archive_chat" => self.archive_chat(parse(args)?).await,
            other => return Err(format!("unknown tool: {other}")),
        };
        result.map_err(|e| e.to_string())
    }

    /// Poll every request together, including its optional wait. A waiting
    /// first chat must not prevent subsequent chats from receiving their work.
    /// All `notify` requests in one call share one batch — their results land
    /// together in one notice.
    async fn batch(&self, args: BatchArgs, create: bool) -> anyhow::Result<Value> {
        anyhow::ensure!(
            (1..=MAX_BATCH).contains(&args.requests.len()),
            "requests must contain between 1 and {MAX_BATCH} items"
        );
        let wants_notify =
            |args: &Value| args.get("notify").and_then(Value::as_bool).unwrap_or(false);
        let batch = args
            .requests
            .iter()
            .any(wants_notify)
            .then(|| uuid::Uuid::new_v4().to_string());
        let shared_batch = batch.clone();
        let results = futures::future::join_all(args.requests.into_iter().enumerate().map(
            |(index, args)| {
                let batch = shared_batch.clone();
                async move {
                    let result = if create {
                        match serde_json::from_value::<CreateChatArgs>(args) {
                            Ok(args) => self.create_chat_with_batch(args, batch.as_deref()).await,
                            Err(error) => Err(error.into()),
                        }
                    } else {
                        match serde_json::from_value::<SendArgs>(args) {
                            Ok(args) => self.send_message_with_batch(args, batch.as_deref()).await,
                            Err(error) => Err(error.into()),
                        }
                    };
                    match result {
                        Ok(result) => json!({"index": index, "isError": false, "result": result}),
                        Err(error) => {
                            json!({"index": index, "isError": true, "error": error.to_string()})
                        }
                    }
                }
            },
        ))
        .await;
        let mut out = json!({"results": results});
        let any_armed = results.iter().any(|r| {
            r["result"]
                .get("task")
                .and_then(|t| t.get("notify"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
        });
        if any_armed && let Some(batch) = batch {
            out["batch"] = json!(batch);
            // Every request has finished — arm-failed requests are not part
            // of the batch. Seal it so the engine may release the notice.
            if let Some(delegator) = self.zeron.origin().chat_id.clone()
                && let Err(err) = self.zeron.seal_delegation_batch(&delegator, &batch).await
            {
                tracing::warn!(error = %err, "sealing the delegation batch failed");
            }
        }
        Ok(out)
    }

    async fn whoami(&self) -> anyhow::Result<Value> {
        let origin = self.zeron.origin().clone();
        let local_device = self.zeron.local_device_id().await?;
        let engine = self.zeron.engine_info().await.unwrap_or(Value::Null);
        let chat = match origin.chat_id.as_deref() {
            Some(id) => match self.zeron.resolve_chat(id).await {
                Ok(chat) => {
                    let (spaces, sessions) =
                        tokio::try_join!(self.zeron.spaces(), self.zeron.sessions())?;
                    summarize_chat(&chat, &spaces, &sessions)
                }
                Err(_) => json!({ "id": id }),
            },
            None => Value::Null,
        };
        let delegation = match origin.chat_id.as_deref() {
            Some(id) => self.zeron.resolve_chat(id).await.ok().map(|chat| {
                let depth = chat.delegation.as_ref().map(|d| d.depth);
                let can_delegate = match &chat.delegation {
                    Some(d) => d.depth < MAX_DELEGATION_DEPTH,
                    // A top-level chat delegates; a side chat does not.
                    None => chat.parent_chat_id.is_none(),
                };
                json!({
                    "delegatedBy": chat.delegation.as_ref().map(|d| d.by.clone()),
                    "depth": depth,
                    "maxDepth": MAX_DELEGATION_DEPTH,
                    "canDelegate": can_delegate,
                })
            }),
            None => None,
        };
        Ok(json!({
            "chat": chat,
            "delegation": delegation.unwrap_or(Value::Null),
            "originDeviceId": origin.device_id,
            "localDeviceId": local_device,
            "workspaceScope": engine.get("workspaceScope").cloned().unwrap_or(Value::Null),
            "note": if origin.chat_id.is_some() {
                "Messages you send are attributed to this chat; it cannot message itself."
            } else {
                "Not running inside a chat: messages are sent without attribution."
            },
        }))
    }

    async fn list_devices(&self) -> anyhow::Result<Value> {
        let (devices, local) =
            tokio::try_join!(self.zeron.devices(), self.zeron.local_device_id())?;
        Ok(json!({
            "devices": devices.iter().map(|d| json!({
                "id": d.id,
                "name": d.name,
                "platform": d.platform,
                "local": d.id == local,
                "lastSeenAt": d.last_seen_at,
                "version": d.version,
            })).collect::<Vec<_>>()
        }))
    }

    async fn list_projects(&self) -> anyhow::Result<Value> {
        let (spaces, devices) = tokio::try_join!(self.zeron.spaces(), self.zeron.devices())?;
        let device_name = |id: &str| devices.iter().find(|d| d.id == id).map(|d| d.name.clone());
        Ok(json!({
            "projects": spaces.iter().map(|s| json!({
                "id": s.id,
                "name": s.display_name(),
                "path": s.path,
                "deviceId": s.device_id,
                "deviceName": device_name(&s.device_id),
                "git": s.git_detected,
            })).collect::<Vec<_>>()
        }))
    }

    async fn list_harnesses(&self) -> anyhow::Result<Value> {
        let harnesses = self.zeron.harnesses().await?;
        Ok(json!({
            "harnesses": harnesses.iter().map(|h| json!({
                "id": h.id,
                "name": h.name,
                "available": h.available(),
                "installed": h.installed,
                "steersMidTurn": h.steers_mid_turn(),
                "reasoningLevels": h.reasoning_levels,
            })).collect::<Vec<_>>()
        }))
    }

    async fn list_models(&self, args: ListModelsArgs) -> anyhow::Result<Value> {
        let harness: HarnessId =
            parse_enum("harness", &args.harness).map_err(anyhow::Error::msg)?;
        let models = self.zeron.models(harness).await?;
        Ok(json!({
            "harness": harness,
            "models": models.iter().map(|m| json!({
                "id": m.id,
                "label": m.label,
                "description": m.description,
                "reasoningLevels": m.reasoning_levels,
            })).collect::<Vec<_>>()
        }))
    }

    async fn list_chats(&self, args: ListChatsArgs) -> anyhow::Result<Value> {
        let (mut chats, spaces, sessions) = tokio::try_join!(
            self.zeron.chats(),
            self.zeron.spaces(),
            self.zeron.sessions()
        )?;
        if let Some(project) = args.project.as_deref() {
            let space = self.zeron.resolve_space(project).await?;
            chats.retain(|c| c.space_id.as_deref() == Some(space.id.as_str()));
        }
        if args.device.is_some() {
            let device = self.zeron.resolve_device_id(args.device.as_deref()).await?;
            chats.retain(|c| c.device_id == device);
        }
        if !args.include_archived {
            chats.retain(|c| !c.archived);
        }
        if let Some(parent) = args.parent.as_deref() {
            let parent = self.zeron.resolve_chat(parent).await?;
            chats.retain(|c| {
                c.delegation
                    .as_ref()
                    .map(|d| d.by.as_str())
                    .or(c.parent_chat_id.as_deref())
                    == Some(parent.id.as_str())
            });
        }
        chats.sort_by(|a, b| {
            let a_at = a.last_message_at.unwrap_or(a.created_at);
            let b_at = b.last_message_at.unwrap_or(b.created_at);
            b_at.cmp(&a_at)
        });
        let total = chats.len();
        let limit = args.limit.unwrap_or(50).clamp(1, 500);
        Ok(json!({
            "total": total,
            "chats": chats.iter().take(limit)
                .map(|c| summarize_chat(c, &spaces, &sessions))
                .collect::<Vec<_>>()
        }))
    }

    async fn get_chat(&self, args: ChatArgs) -> anyhow::Result<Value> {
        let chat = self.zeron.resolve_chat(&args.chat).await?;
        let (spaces, sessions, entries) = tokio::try_join!(
            self.zeron.spaces(),
            self.zeron.sessions(),
            self.zeron.transcript(&chat.id)
        )?;
        let rendered = render_entries(&entries, RenderOptions::default());
        let mut summary = summarize_chat(&chat, &spaces, &sessions);
        summary["messageCount"] = json!(rendered.len());
        summary["pendingInput"] = last_pending_input(&rendered).unwrap_or(Value::Null);
        summary["lastMessage"] = rendered.last().map(|m| json!(m)).unwrap_or(Value::Null);
        Ok(summary)
    }

    async fn create_chat(&self, args: CreateChatArgs) -> anyhow::Result<Value> {
        self.create_chat_with_batch(args, None).await
    }

    /// `create_chat` with an optional shared `notify` batch (create_chats
    /// mints one for the whole call).
    async fn create_chat_with_batch(
        &self,
        args: CreateChatArgs,
        batch: Option<&str>,
    ) -> anyhow::Result<Value> {
        let origin_id = self.zeron.origin().chat_id.clone();
        let caller = match origin_id.as_deref() {
            Some(id) => Some(self.zeron.resolve_chat(id).await?),
            None => None,
        };
        if args.notify {
            anyhow::ensure!(
                args.prompt.as_deref().is_some_and(|p| !p.trim().is_empty()),
                "notify needs a prompt"
            );
            anyhow::ensure!(!args.wait, "notify and wait cannot be combined");
            anyhow::ensure!(
                caller.is_some(),
                "notify needs a calling chat; this server was not started by one"
            );
        }
        let harnesses = self.zeron.harnesses().await?;
        let harness = match args.harness.as_deref() {
            Some(raw) => {
                let id: HarnessId = parse_enum("harness", raw).map_err(anyhow::Error::msg)?;
                if let Some(info) = harnesses.iter().find(|h| h.id == id)
                    && !info.available()
                {
                    anyhow::bail!(
                        "harness {raw} is not available on this device (see list_harnesses)"
                    );
                }
                id
            }
            None => default_harness(&harnesses)?,
        };
        if let Some(model) = args.model.as_deref()
            && let Ok(models) = self.zeron.models(harness).await
            && !models.is_empty()
            && !models.iter().any(|m| m.id == model)
        {
            anyhow::bail!(
                "model {model:?} is not offered by {harness:?}; available: {}",
                models
                    .iter()
                    .map(|m| m.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        let reasoning: Option<ReasoningLevel> = match args.reasoning.as_deref() {
            Some(raw) => Some(parse_enum("reasoning level", raw).map_err(anyhow::Error::msg)?),
            None => None,
        };
        // A delegated task's child defaults to at most its own sandbox level;
        // the engine still enforces the cap, so this only sets the default.
        let caller_level = caller
            .as_ref()
            .and_then(|c| c.config.as_ref())
            .map(|c| c.sandbox)
            .unwrap_or(SandboxLevel::WorkspaceWrite);
        let sandbox: SandboxLevel = match args.sandbox.as_deref() {
            Some(raw) => parse_enum("sandbox", raw).map_err(anyhow::Error::msg)?,
            None if caller.as_ref().is_some_and(|c| c.delegation.is_some()) => {
                caller_level.min(SandboxLevel::WorkspaceWrite)
            }
            None => SandboxLevel::WorkspaceWrite,
        };
        let config = ChatConfig {
            harness,
            model: args.model.clone(),
            reasoning,
            model_options: Default::default(),
            sandbox,
        };

        let (space, device_id) = match args.project.as_deref() {
            Some(project) => {
                let space = self.zeron.resolve_space(project).await?;
                let device_id = space.device_id.clone();
                (Some(space), device_id)
            }
            None => (
                None,
                self.zeron.resolve_device_id(args.device.as_deref()).await?,
            ),
        };

        // Speaking for a chat makes the new chat a delegated task: the
        // engine derives parentChatId (the delegator's root) and enforces
        // the fork/depth/sandbox rules, surfacing them as this call's error.
        let delegated_by = origin_id.clone();
        let parent_chat_id = match args
            .parent
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
        {
            Some(key) => Some(self.zeron.resolve_chat(key).await?.id),
            None => None,
        };
        // The live-task limit is an MCP-side check over the snapshots: the
        // engine's rule set never refuses to create the row.
        if let Some(caller) = caller.as_ref()
            && args.prompt.is_some()
        {
            let root = caller
                .parent_chat_id
                .clone()
                .unwrap_or_else(|| caller.id.clone());
            let (chats, sessions) = tokio::try_join!(self.zeron.chats(), self.zeron.sessions())?;
            let live = chats
                .iter()
                .filter(|c| c.delegation.is_some())
                .filter(|c| c.parent_chat_id.as_deref() == Some(root.as_str()))
                .filter(|c| {
                    session_for(&sessions, c).is_some_and(|s| {
                        matches!(
                            s.status,
                            SessionStatus::Working | SessionStatus::AwaitingInput
                        )
                    })
                })
                .count();
            anyhow::ensure!(
                live < MAX_LIVE_TASKS,
                "{MAX_LIVE_TASKS} delegated tasks are already running under this chat. Wait for a notice or cancel one with task_cancel."
            );
        }
        let chat_id = uuid::Uuid::new_v4().to_string();
        let mut mutate = json!({
            "op": "createChat",
            "chatId": chat_id,
            "deviceId": device_id,
            "config": config,
        });
        if let Some(by) = &delegated_by {
            mutate["delegatedBy"] = json!(by);
        }
        if let Some(space) = &space {
            mutate["spaceId"] = json!(space.id);
        }
        if let Some(parent) = &parent_chat_id {
            mutate["parentChatId"] = json!(parent);
        }
        if let Some(branch) = args
            .branch
            .as_deref()
            .map(str::trim)
            .filter(|b| !b.is_empty())
        {
            mutate["branch"] = json!(branch);
        }
        if let Some(cwd) = args.cwd.as_deref().map(str::trim).filter(|c| !c.is_empty()) {
            mutate["cwd"] = json!(cwd);
        }
        self.zeron.mutate(mutate).await?;
        if let Some(title) = args
            .title
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            self.zeron
                .mutate(json!({ "op": "renameChat", "chatId": chat_id, "title": title }))
                .await?;
        }

        // The engine lists the task under its root: the delegator's own
        // parent, or the delegator itself when it is top-level.
        let listed_under = caller
            .as_ref()
            .and_then(|c| c.parent_chat_id.clone().or_else(|| Some(c.id.clone())));
        let mut result = json!({
            "chatId": chat_id,
            "deviceId": device_id,
            "project": space.as_ref().map(|s| json!({ "id": s.id, "name": s.display_name(), "path": s.path })),
            "harness": harness,
            "model": args.model,
            "reasoning": reasoning,
            "title": args.title,
            "parentChatId": if delegated_by.is_some() { listed_under } else { parent_chat_id.clone() },
        });
        if let Some(by) = &delegated_by {
            result["task"] = json!({
                "delegatedBy": by,
                "depth": caller
                    .as_ref()
                    .and_then(|c| c.delegation.as_ref())
                    .map(|d| d.depth + 1)
                    .unwrap_or(1),
                "notify": args.notify,
                "batch": if args.notify { batch.map(str::to_owned).or_else(|| Some(uuid::Uuid::new_v4().to_string())) } else { None },
            });
        }
        if let Some(prompt) = args.prompt.filter(|p| !p.trim().is_empty()) {
            // The row may not have folded into WatchChats yet; build the
            // chat locally from what we just wrote rather than re-reading.
            let chat = Chat {
                id: chat_id.clone(),
                device_id: device_id.clone(),
                title: args.title.clone(),
                archived: false,
                cwd: args.cwd.clone(),
                branch: args.branch.clone(),
                checkout_id: None,
                source_context: None,
                config: Some(config),
                last_message_preview: None,
                last_message_at: None,
                created_at: chrono::Utc::now(),
                harness_session_id: None,
                harness_session_cwd: None,
                delegation: None,
                parent_chat_id: parent_chat_id.clone(),
                space_id: space.as_ref().map(|s| s.id.clone()),
                last_seen_at: None,
                room_gen: None,
            };
            let (prompt, batch) = if args.notify {
                // `batch` absent means a lone create_chat — seal with the
                // arm; a shared batch seals once after the batch tool ends.
                let shared = batch.is_some();
                let batch = result["task"]["batch"]
                    .as_str()
                    .expect("notify result carries a batch")
                    .to_owned();
                let label = caller
                    .as_ref()
                    .and_then(|c| c.title.clone())
                    .map(|t| sanitize_meta(&t))
                    .filter(|t| !t.is_empty())
                    .unwrap_or_else(|| short(origin_id.as_deref().unwrap_or("")).to_owned());
                (
                    format!(
                        "[Delegated task from Zeron chat {label} ({}). Zeron delivers the final message of your turn to that chat automatically. Put the complete result in that message. Do not send the result with send_message.]

{prompt}",
                        short(origin_id.as_deref().unwrap_or_default())
                    ),
                    Some((batch, !shared)),
                )
            } else {
                (prompt, None)
            };
            let sent = self
                .deliver_notify(
                    &chat,
                    space.as_ref(),
                    &harnesses,
                    None,
                    prompt,
                    "run",
                    batch.as_ref().map(|(b, s)| (b.as_str(), *s)),
                )
                .await
                .map_err(notify_error)?;
            result["sent"] = sent;
            if args.wait {
                result["turn"] = self
                    .await_turn(
                        &chat,
                        None,
                        true,
                        wait_duration(args.timeout_secs),
                        now_millis(),
                    )
                    .await?;
            }
        }
        Ok(result)
    }

    async fn read_chat(&self, args: ReadChatArgs) -> anyhow::Result<Value> {
        let chat = self.zeron.resolve_chat(&args.chat).await?;
        let entries = self.zeron.transcript(&chat.id).await?;
        let rendered = render_entries(
            &entries,
            RenderOptions {
                include_reasoning: args.include_reasoning,
                include_tools: args.include_tools.unwrap_or(true),
            },
        );
        let total = rendered.len();
        let limit = args.limit.unwrap_or(40).clamp(1, 500);
        let end = total.saturating_sub(args.offset);
        let start = end.saturating_sub(limit);
        let window = &rendered[start..end];
        Ok(json!({
            "chatId": chat.id,
            "title": chat.title,
            "total": total,
            "returned": window.len(),
            "olderRemaining": start,
            "newerSkipped": total - end,
            "pendingInput": last_pending_input(&rendered),
            "messages": window,
        }))
    }

    async fn send_message(&self, args: SendArgs) -> anyhow::Result<Value> {
        self.send_message_with_batch(args, None).await
    }

    /// `send_message` with an optional shared `notify` batch (send_messages
    /// mints one for the whole call).
    async fn send_message_with_batch(
        &self,
        args: SendArgs,
        batch: Option<&str>,
    ) -> anyhow::Result<Value> {
        let text = args.text.trim();
        if text.is_empty() {
            anyhow::bail!("text is empty");
        }
        let chat = self.zeron.resolve_chat(&args.chat).await?;
        if args.notify {
            anyhow::ensure!(!args.wait, "notify and wait cannot be combined");
            anyhow::ensure!(
                args.mode.as_deref() != Some("queue"),
                "notify cannot be combined with mode: queue"
            );
            let origin = self.zeron.origin().chat_id.clone();
            anyhow::ensure!(
                origin.is_some(),
                "notify needs a calling chat; this server was not started by one"
            );
            anyhow::ensure!(
                chat.delegation
                    .as_ref()
                    .is_some_and(|d| d.by.as_str() == origin.as_deref().unwrap()),
                "notify works only for tasks you delegated"
            );
        }
        if self.zeron.origin().chat_id.as_deref() == Some(chat.id.as_str()) {
            anyhow::bail!(
                "refusing to send a message to your own chat ({})",
                short(&chat.id)
            );
        }
        let (spaces, sessions, harnesses) = tokio::try_join!(
            self.zeron.spaces(),
            self.zeron.sessions(),
            self.zeron.harnesses()
        )?;
        let space = chat
            .space_id
            .as_deref()
            .and_then(|id| spaces.iter().find(|s| s.id == id));
        let baseline = session_for(&sessions, &chat);
        let mode = args.mode.as_deref().unwrap_or("auto");
        let sent_at = now_millis();
        let body = self.attribute(&chat, text).await;
        let mut result = json!({
            "chatId": chat.id,
            "title": chat.title,
        });
        let shared = batch.is_some();
        let batch = if args.notify {
            batch
                .map(str::to_owned)
                .or_else(|| Some(uuid::Uuid::new_v4().to_string()))
        } else {
            None
        };
        result["sent"] = self
            .deliver_notify(
                &chat,
                space,
                &harnesses,
                baseline.as_ref(),
                body,
                mode,
                batch.as_deref().map(|b| (b, !shared)),
            )
            .await
            .map_err(notify_error)?;
        if args.notify {
            result["task"] = json!({ "notify": true, "batch": batch });
        }
        if args.wait {
            result["turn"] = self
                .await_turn(
                    &chat,
                    baseline.as_ref(),
                    true,
                    wait_duration(args.timeout_secs),
                    sent_at,
                )
                .await?;
        }
        Ok(result)
    }

    async fn wait_for_turn(&self, args: WaitArgs) -> anyhow::Result<Value> {
        let chat = self.zeron.resolve_chat(&args.chat).await?;
        let turn = self
            .await_turn(&chat, None, false, wait_duration(args.timeout_secs), 0)
            .await?;
        Ok(json!({ "chatId": chat.id, "title": chat.title, "turn": turn }))
    }

    async fn archive_chat(&self, args: ArchiveArgs) -> anyhow::Result<Value> {
        let chat = self.zeron.resolve_chat(&args.chat).await?;
        let archived = args.archived.unwrap_or(true);
        self.zeron
            .mutate(json!({ "op": "setChatArchived", "chatId": chat.id, "archived": archived }))
            .await?;
        Ok(json!({ "chatId": chat.id, "title": chat.title, "archived": archived }))
    }

    async fn interrupt_chat(&self, args: ChatArgs) -> anyhow::Result<Value> {
        let chat = self.zeron.resolve_chat(&args.chat).await?;
        let command_id = self
            .zeron
            .queue_command(&chat.id, &SessionCommandPayload::Interrupt {})
            .await?;
        Ok(json!({ "chatId": chat.id, "commandId": command_id }))
    }

    async fn respond_to_input(&self, args: RespondArgs) -> anyhow::Result<Value> {
        let chat = self.zeron.resolve_chat(&args.chat).await?;
        let request_id = match args.request_id {
            Some(id) => id,
            None => {
                let entries = self.zeron.transcript(&chat.id).await?;
                let rendered = render_entries(&entries, RenderOptions::default());
                last_pending_input(&rendered)
                    .and_then(|p| {
                        p.get("requestId")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .ok_or_else(|| {
                        anyhow::anyhow!("chat {} has no pending question", short(&chat.id))
                    })?
            }
        };
        if args.answers.is_empty() {
            anyhow::bail!("answers is empty");
        }
        let answers = args
            .answers
            .into_iter()
            .map(|a| UserInputAnswer {
                question_id: a.question_id,
                labels: a.labels,
            })
            .collect();
        let command_id = self
            .zeron
            .queue_command(
                &chat.id,
                &SessionCommandPayload::RespondInput {
                    request_id: request_id.clone(),
                    answers,
                },
            )
            .await?;
        Ok(json!({ "chatId": chat.id, "requestId": request_id, "commandId": command_id }))
    }

    /// `task_status`: the caller's delegation tree (or one task's reply).
    async fn task_status(&self, args: TaskStatusArgs) -> anyhow::Result<Value> {
        let (chats, sessions, delegations) = tokio::try_join!(
            self.zeron.chats(),
            self.zeron.sessions(),
            self.zeron.list_delegations(),
        )?;
        let local = self.zeron.local_device_id().await.unwrap_or_default();
        match args.chat.as_deref() {
            None => {
                let Some(origin) = self.zeron.origin().chat_id.clone() else {
                    return Ok(json!({ "tasks": [] }));
                };
                let caller = self.zeron.resolve_chat(&origin).await?;
                let tasks = self
                    .task_tree(&caller, &chats, &sessions, &delegations, &local)
                    .await;
                Ok(json!({ "tasks": tasks }))
            }
            Some(key) => {
                let chat = self.zeron.resolve_chat(key).await?;
                let mut node = self
                    .task_node(&chat, &chats, &sessions, &delegations, &local)
                    .await;
                let entries = self.zeron.transcript(&chat.id).await.unwrap_or_default();
                let rendered = render_entries(&entries, RenderOptions::default());
                let limit = args.reply_chars.unwrap_or(8_000).min(100_000);
                let full = rendered
                    .iter()
                    .rev()
                    .find(|m| m.role == zeron_doc::MessageRole::Assistant)
                    .map(|m| m.text.clone())
                    .unwrap_or_default();
                let truncated = full.chars().count() > limit;
                node["reply"] = json!(full.chars().take(limit).collect::<String>());
                node["replyTruncated"] = json!(truncated);
                node["pendingInput"] = last_pending_input(&rendered).unwrap_or(Value::Null);
                Ok(node)
            }
        }
    }

    /// One tree level of delegated children, recursively.
    fn task_tree<'a>(
        &'a self,
        delegator: &'a Chat,
        chats: &'a [Chat],
        sessions: &'a [Session],
        delegations: &'a [Value],
        local: &'a str,
    ) -> futures::future::BoxFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let mut nodes = Vec::new();
            for child in chats
                .iter()
                .filter(|c| c.delegation.as_ref().is_some_and(|d| d.by == delegator.id))
            {
                nodes.push(
                    self.task_node(child, chats, sessions, delegations, local)
                        .await,
                );
            }
            nodes
        })
    }

    /// One task's status row: registry + session + ledger, then the
    /// transcript only when the state needs the last assistant entry.
    async fn task_node(
        &self,
        chat: &Chat,
        chats: &[Chat],
        sessions: &[Session],
        delegations: &[Value],
        local: &str,
    ) -> Value {
        let tasks = self
            .task_tree(chat, chats, sessions, delegations, local)
            .await;
        let session = session_for(sessions, chat);
        let status_age = status_of(session.as_ref()).1;
        let state = match session.as_ref().map(|s| s.status) {
            Some(SessionStatus::Working) => "working".to_string(),
            Some(SessionStatus::AwaitingInput) => "awaitingInput".to_string(),
            Some(SessionStatus::Errored) => "errored".to_string(),
            _ => {
                let waiting = tasks.iter().any(|t| {
                    matches!(
                        t["state"].as_str(),
                        Some("working" | "awaitingInput" | "waitingForTasks")
                    )
                });
                if waiting {
                    "waitingForTasks".to_string()
                } else {
                    let entries = self.zeron.transcript(&chat.id).await.unwrap_or_default();
                    match entries
                        .iter()
                        .rev()
                        .find(|e| e.role == zeron_doc::MessageRole::Assistant)
                        .and_then(|e| e.status)
                    {
                        Some(zeron_doc::MessageStatus::Complete) => "completed",
                        Some(zeron_doc::MessageStatus::Aborted) => "interrupted",
                        _ => "idle",
                    }
                    .to_string()
                }
            }
        };
        // The ledger is host-local: a task hosted elsewhere reports unknown.
        let ledger_entry = if chat.device_id == local {
            delegations
                .iter()
                .find(|d| d["chatId"].as_str() == Some(chat.id.as_str()))
        } else {
            None
        };
        let notice = if chat.device_id != local {
            "unknown"
        } else {
            ledger_entry
                .and_then(|d| d["notice"].as_str())
                .unwrap_or("none")
        };
        json!({
            "chatId": chat.id,
            "title": chat.title,
            "harness": chat.config.as_ref().map(|c| c.harness),
            "model": chat.config.as_ref().and_then(|c| c.model.clone()),
            "delegatedBy": chat.delegation.as_ref().map(|d| d.by.clone()),
            "depth": chat.delegation.as_ref().map(|d| d.depth),
            "state": state,
            "statusAgeSecs": status_age,
            "notice": notice,
            "batch": ledger_entry.and_then(|d| d["batch"].as_str().map(str::to_owned)),
            "lastMessagePreview": chat.last_message_preview,
            "tasks": tasks,
        })
    }

    /// `task_cancel`: stop a task and its subtree, no notices.
    async fn task_cancel(&self, args: ChatArgs) -> anyhow::Result<Value> {
        let chat = self.zeron.resolve_chat(&args.chat).await?;
        let local = self.zeron.local_device_id().await?;
        anyhow::ensure!(
            chat.device_id == local,
            "task_cancel works only for tasks hosted on this device; use interrupt_chat instead"
        );
        // With a calling chat the target must sit below it in the
        // delegation tree (delegation.by, not parentChatId).
        if let Some(origin) = self.zeron.origin().chat_id.clone() {
            let chats = self.zeron.chats().await?;
            let mut cursor = chat.delegation.as_ref().map(|d| d.by.clone());
            let mut ok = false;
            while let Some(up) = cursor {
                if up == origin {
                    ok = true;
                    break;
                }
                cursor = chats
                    .iter()
                    .find(|c| c.id == up)
                    .and_then(|c| c.delegation.as_ref().map(|d| d.by.clone()));
            }
            anyhow::ensure!(ok, "task_cancel works only for tasks you delegated");
        }
        self.zeron.cancel_delegated_task(&chat.id).await
    }

    // ---- shared pieces -------------------------------------------------------

    /// Prefix the sender's identity when this server speaks for a chat, so
    /// the receiving agent (and the human reading that transcript) can tell
    /// an agent-to-agent message from a typed one.
    async fn attribute(&self, target: &Chat, text: &str) -> String {
        let Some(origin_id) = self.zeron.origin().chat_id.as_deref() else {
            return text.to_owned();
        };
        if origin_id == target.id {
            return text.to_owned();
        }
        let title = match self.zeron.resolve_chat(origin_id).await {
            Ok(chat) => chat.title,
            Err(_) => None,
        };
        let label = match title {
            Some(title) if !title.trim().is_empty() => {
                format!("{} ({})", title.trim(), short(origin_id))
            }
            _ => short(origin_id).to_owned(),
        };
        format!(
            "[Message from Zeron chat {label}. Reply to it with the Zeron `send_message` tool, chat {}.]\n\n{text}",
            short(origin_id)
        )
    }

    /// Pick and perform the delivery the composer would, with an optional
    /// `notify` batch armed on the queued command.
    #[allow(clippy::too_many_arguments)]
    async fn deliver_notify(
        &self,
        chat: &Chat,
        space: Option<&Space>,
        harnesses: &[HarnessInfo],
        session: Option<&Session>,
        text: String,
        mode: &str,
        notify: Option<(&str, bool)>,
    ) -> anyhow::Result<Value> {
        let harness = chat
            .config
            .as_ref()
            .map(|c| c.harness)
            .map_or_else(|| default_harness(harnesses), Ok)?;
        let (status, _) = status_of(session);
        // A quiet tool call can outlive the UI's stale-status window. Route
        // through steering and let the host decide whether a live run exists.
        let live = session.map(|s| s.status).unwrap_or(SessionStatus::Idle);
        let chosen = match mode {
            "auto" => match live {
                SessionStatus::Idle | SessionStatus::Errored => "run",
                SessionStatus::Working => "steer",
                SessionStatus::AwaitingInput => anyhow::bail!(
                    "chat {} is waiting for an answer; use respond_to_input (or mode 'queue' to hold this message for after the turn)",
                    short(&chat.id)
                ),
            },
            "run" | "steer" | "queue" => mode,
            other => anyhow::bail!("unknown mode {other:?}; expected auto, run, steer, or queue"),
        };
        let id = match chosen {
            "run" => {
                let config = chat.config.clone();
                let cwd = chat
                    .cwd
                    .clone()
                    .or_else(|| space.map(|s| s.path.clone()))
                    .unwrap_or_else(|| "~".into());
                let request = RunRequest {
                    mcp: None,
                    prompt: text,
                    harness: Some(harness),
                    model: config.as_ref().and_then(|c| c.model.clone()),
                    reasoning: config.as_ref().and_then(|c| c.reasoning),
                    model_options: config
                        .as_ref()
                        .map(|c| c.model_options.clone())
                        .unwrap_or_default(),
                    cwd,
                    sandbox: config
                        .as_ref()
                        .map(|c| c.sandbox)
                        .unwrap_or(SandboxLevel::WorkspaceWrite),
                    auto_approve: false,
                    resume: None,
                    attachments: Vec::new(),
                    worktree: None,
                };
                self.zeron
                    .queue_command_notify(
                        &chat.id,
                        &SessionCommandPayload::Run {
                            request,
                            message_id: uuid::Uuid::new_v4().to_string(),
                        },
                        notify,
                    )
                    .await?
            }
            "steer" => {
                self.zeron
                    .queue_command_notify(
                        &chat.id,
                        &SessionCommandPayload::Steer {
                            prompt: text,
                            message_id: Some(uuid::Uuid::new_v4().to_string()),
                        },
                        notify,
                    )
                    .await?
            }
            _ => self.zeron.queue_message(&chat.id, &text).await?,
        };
        Ok(json!({
            "delivery": chosen,
            "id": id,
            "chatStatusAtSend": status,
        }))
    }

    /// Wait, then report the outcome with the assistant messages that
    /// landed since `since_millis`.
    async fn await_turn(
        &self,
        chat: &Chat,
        baseline: Option<&Session>,
        expect_turn: bool,
        timeout: Duration,
        since_millis: i64,
    ) -> anyhow::Result<Value> {
        let (outcome, session) = self
            .zeron
            .wait_for_turn(chat, baseline, expect_turn, timeout)
            .await?;
        let entries = self.zeron.transcript(&chat.id).await.unwrap_or_default();
        let rendered = render_entries(&entries, RenderOptions::default());
        let replies: Vec<&RenderedMessage> = rendered
            .iter()
            .filter(|m| m.role == zeron_doc::MessageRole::Assistant)
            .filter(|m| m.created_at >= since_millis.saturating_sub(2_000))
            .collect();
        let replies: Vec<&RenderedMessage> = if replies.is_empty() {
            rendered
                .iter()
                .rev()
                .find(|m| m.role == zeron_doc::MessageRole::Assistant)
                .into_iter()
                .collect()
        } else {
            replies
        };
        let (status, _) = status_of(session.as_ref());
        Ok(json!({
            "outcome": outcome,
            "status": status,
            "timedOut": outcome == TurnOutcome::TimedOut,
            "pendingInput": last_pending_input(&rendered),
            "replies": replies,
        }))
    }
}

/// The engine's arm-side rejection texts need a small MCP-side suffix.
fn notify_error(err: anyhow::Error) -> anyhow::Error {
    let text = err.to_string();
    if text.contains("hosted on this device") {
        anyhow::anyhow!("{text}; use wait instead")
    } else {
        err
    }
}

/// claude-code when it is offered here, else the first available harness.
fn default_harness(harnesses: &[HarnessInfo]) -> anyhow::Result<HarnessId> {
    if harnesses.is_empty() {
        return Ok(HarnessId::ClaudeCode);
    }
    harnesses
        .iter()
        .find(|h| h.id == HarnessId::ClaudeCode && h.available())
        .or_else(|| {
            harnesses
                .iter()
                .find(|h| h.available() && h.id != HarnessId::Mock)
        })
        .map(|h| h.id)
        .ok_or_else(|| {
            anyhow::anyhow!("no harness is available on this device (see list_harnesses)")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zeron::Origin;
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::sync::Mutex;
    use zeron_rpc::{RpcError, RpcReply, RpcService, memory_client, methods};

    /// A fixed little workspace: one device, one project, a configurable set
    /// of chat rows, and a two-message transcript. Writes are recorded for
    /// assertions; `createChat` mirrors the engine's delegation rules.
    #[derive(Default)]
    struct World {
        writes: Mutex<Vec<(String, Value)>>,
        dispatch_barrier: Option<tokio::sync::Barrier>,
        /// Extra chat rows beyond the default Alpha/Beta pair.
        extra_chats: Vec<Value>,
        /// `beta`'s parent; set without `delegation` to make it a side chat.
        beta_parent: Option<String>,
        /// `beta`'s `delegation` object, when it is a task.
        beta_delegation: Option<Value>,
        /// `beta`'s config sandbox (the delegated task's own level).
        beta_sandbox: Option<&'static str>,
        sessions: Vec<Value>,
        delegations: Vec<Value>,
        /// Canned CancelDelegatedTask reply.
        cancel_reply: Value,
    }

    /// The engine's createChat behavior the tools now surface instead of
    /// checking themselves: delegator rules evaluated against the row.
    fn create_chat_check(chats: &[Value], params: &Value) -> Option<String> {
        let by = params["delegatedBy"].as_str()?;
        let row = chats.iter().find(|c| c["id"].as_str() == Some(by))?;
        let delegation = row.get("delegation").filter(|d| !d.is_null());
        match delegation {
            // A row with a parent and no delegation is a side chat.
            None if !row.get("parentChatId").is_none_or(Value::is_null) => Some(
                "Side chats cannot create chats. Ask your parent chat to create another side chat."
                    .into(),
            ),
            None => None,
            Some(d) => {
                if d["depth"].as_u64().unwrap_or(0) >= 2 {
                    return Some("Delegation depth limit reached (2). Do this work yourself, or return the subtasks to your delegator in your final message.".into());
                }
                // The sandbox cap: a task's child may not exceed its level.
                let rank = |s: &str| match s {
                    "read-only" => 0,
                    "danger-full-access" => 2,
                    _ => 1,
                };
                let own = row["config"]["sandbox"]
                    .as_str()
                    .unwrap_or("workspace-write");
                let requested = params["config"]["sandbox"]
                    .as_str()
                    .unwrap_or("workspace-write");
                if rank(requested) > rank(own) {
                    return Some(format!(
                        "A delegated task cannot create a task with a higher sandbox level than its own. This task is {own} and the request asked for {requested}."
                    ));
                }
                None
            }
        }
    }

    fn stream(item: Value) -> RpcReply {
        RpcReply::Stream(futures::stream::iter(vec![item]).boxed())
    }

    #[async_trait]
    impl RpcService for World {
        async fn handle(&self, method: &str, params: Value) -> Result<RpcReply, RpcError> {
            Ok(match method {
                methods::LOCAL_DEVICE => RpcReply::Value(json!({ "deviceId": "dev-local" })),
                methods::ENGINE_INFO => RpcReply::Value(json!({
                    "deviceId": "dev-local", "workspaceScope": "local"
                })),
                methods::WATCH_DEVICES => stream(json!([{
                    "id": "dev-local", "name": "Laptop", "platform": "linux",
                    "lastSeenAt": null
                }])),
                methods::WATCH_SPACES => stream(json!([{
                    "id": "space-1", "deviceId": "dev-local", "path": "/repo/comet",
                    "gitDetected": true, "createdAt": "2026-09-01T00:00:00Z"
                }])),
                methods::WATCH_CHATS => {
                    let mut chats = vec![
                        json!({
                            "id": "chat-alpha-1", "deviceId": "dev-local", "title": "Alpha",
                            "archived": false, "spaceId": "space-1",
                            "config": { "harness": "claude-code", "model": "opus", "reasoning": null, "sandbox": "workspace-write" },
                            "createdAt": "2026-09-01T00:00:00Z"
                        }),
                        json!({
                            "id": "chat-beta-2", "deviceId": "dev-local", "title": "Beta",
                            "parentChatId": self.beta_parent,
                            "delegation": self.beta_delegation,
                            "archived": false, "spaceId": "space-1",
                            "config": { "harness": "claude-code", "model": "opus", "reasoning": null, "sandbox": self.beta_sandbox.unwrap_or("workspace-write") },
                            "createdAt": "2026-09-02T00:00:00Z"
                        }),
                    ];
                    chats.extend(self.extra_chats.iter().cloned());
                    stream(json!(chats))
                }
                methods::WATCH_SESSIONS => stream(json!(self.sessions)),
                methods::LIST_HARNESSES => RpcReply::Value(json!([
                    { "id": "claude-code", "name": "Claude Code", "supportsSteering": true,
                      "steeringMode": "step-boundary", "reasoningLevels": [], "installed": true, "enabled": true },
                    { "id": "codex", "name": "Codex", "supportsSteering": true,
                      "steeringMode": "turn-boundary", "reasoningLevels": [], "installed": false, "enabled": true }
                ])),
                methods::LIST_MODELS => RpcReply::Value(json!([
                    { "id": "opus", "label": "Opus" }, { "id": "sonnet", "label": "Sonnet" }
                ])),
                methods::WATCH_DOC_MESSAGES => stream(json!({ "reset": [
                    { "id": "u1", "role": "user", "createdAt": 1, "deviceId": "dev-local",
                      "parts": [{ "kind": "text", "id": "t", "text": "hi" }] },
                    { "id": "a1", "role": "assistant", "createdAt": 2, "deviceId": "dev-local",
                      "status": "complete",
                      "parts": [{ "kind": "text", "id": "t", "text": "hello back" }] }
                ]})),
                methods::LIST_DELEGATIONS => RpcReply::Value(json!({ "tasks": self.delegations })),
                methods::SEAL_DELEGATION_BATCH => {
                    self.writes
                        .lock()
                        .unwrap()
                        .push((method.to_owned(), params));
                    RpcReply::Value(json!({}))
                }
                methods::CANCEL_DELEGATED_TASK => {
                    self.writes
                        .lock()
                        .unwrap()
                        .push((method.to_owned(), params));
                    RpcReply::Value(self.cancel_reply.clone())
                }
                methods::MUTATE | methods::QUEUE_COMMAND | methods::QUEUE_MESSAGE => {
                    if method == methods::MUTATE
                        && params["op"].as_str() == Some("createChat")
                        && let Some(err) = {
                            let mut chats = vec![
                                json!({"id":"chat-alpha-1","deviceId":"dev-local","title":"Alpha"}),
                                json!({"id":"chat-beta-2","deviceId":"dev-local","title":"Beta","parentChatId":self.beta_parent,"delegation":self.beta_delegation,"config":{"sandbox":self.beta_sandbox.unwrap_or("workspace-write")}}),
                            ];
                            chats.extend(self.extra_chats.iter().cloned());
                            create_chat_check(&chats, &params)
                        }
                    {
                        return Err(RpcError::Failed(err));
                    }
                    self.writes
                        .lock()
                        .unwrap()
                        .push((method.to_owned(), params));
                    if method == methods::QUEUE_COMMAND
                        && let Some(barrier) = &self.dispatch_barrier
                    {
                        // No dispatch can finish until both chats have work.
                        // A sequential batch deadlocks here, before any waits.
                        barrier.wait().await;
                    }
                    RpcReply::Value(json!({ "commandId": "cmd-1", "id": "q-1" }))
                }
                other => return Err(RpcError::UnknownMethod(other.into())),
            })
        }
    }

    fn tools(world: Arc<World>, origin: Origin) -> Tools {
        let client = memory_client(world);
        Tools::new(Arc::new(Zeron::with_client(client, origin)))
    }

    #[tokio::test]
    async fn catalog_is_well_formed() {
        let defs = catalog();
        let mut names: Vec<&str> = defs.iter().map(|d| d.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), defs.len(), "tool names must be unique");
        for def in &defs {
            assert_eq!(def.input_schema["type"], "object", "{}", def.name);
            assert!(!def.description.is_empty());
        }
    }

    #[tokio::test]
    async fn list_and_read_chats() {
        let world = Arc::new(World::default());
        let tools = tools(world, Origin::default());
        let listed = tools.call("list_chats", json!({})).await.unwrap();
        assert_eq!(listed["total"], 2);
        // Newest activity first: Beta was created later.
        assert_eq!(listed["chats"][0]["title"], "Beta");
        assert_eq!(listed["chats"][1]["project"]["name"], "comet");
        assert_eq!(listed["chats"][1]["status"], "idle");

        let read = tools
            .call("read_chat", json!({ "chat": "alpha" }))
            .await
            .unwrap();
        assert_eq!(read["total"], 2);
        assert_eq!(read["messages"][1]["text"], "hello back");

        let read = tools
            .call("read_chat", json!({ "chat": "chat-alpha", "limit": 1 }))
            .await
            .unwrap();
        assert_eq!(read["returned"], 1);
        assert_eq!(read["olderRemaining"], 1);
        assert_eq!(read["messages"][0]["id"], "a1");
    }

    #[tokio::test]
    async fn send_attributes_and_refuses_self() {
        let world = Arc::new(World::default());
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: Some("dev-local".into()),
            },
        );
        let err = tools
            .call("send_message", json!({ "chat": "beta", "text": "loop" }))
            .await
            .unwrap_err();
        assert!(err.contains("own chat"), "{err}");

        let sent = tools
            .call(
                "send_message",
                json!({ "chat": "alpha", "text": "please review" }),
            )
            .await
            .unwrap();
        assert_eq!(sent["sent"]["delivery"], "run");
        let writes = world.writes.lock().unwrap();
        let (method, params) = writes.last().expect("a queued command");
        assert_eq!(method, methods::QUEUE_COMMAND);
        assert_eq!(params["chatId"], "chat-alpha-1");
        assert_eq!(params["command"]["kind"], "run");
        let prompt = params["command"]["request"]["prompt"].as_str().unwrap();
        assert!(
            prompt.starts_with("[Message from Zeron chat Beta (chat-bet)"),
            "{prompt}"
        );
        assert!(prompt.ends_with("please review"));
        assert_eq!(params["command"]["request"]["cwd"], "/repo/comet");
        assert_eq!(params["command"]["request"]["harness"], "claude-code");
        assert_eq!(params["command"]["request"]["model"], "opus");
    }

    #[tokio::test]
    async fn auto_steers_busy_chats_even_at_turn_boundaries_or_after_long_quiet_tools() {
        let world = Arc::new(World::default());
        let tools = tools(world.clone(), Origin::default());
        let chats = tools.zeron.chats().await.unwrap();
        let session = Session {
            chat_id: chats[0].id.clone(),
            device_id: "dev-local".into(),
            status: SessionStatus::Working,
            started_at: None,
            updated_at: chrono::Utc::now() - chrono::Duration::minutes(10),
            last_completed_turn: None,
        };
        // No mid-turn capability is required for a live mailbox delivery.
        let sent = tools
            .deliver_notify(
                &chats[0],
                None,
                &[],
                Some(&session),
                "follow up".into(),
                "auto",
                None,
            )
            .await
            .unwrap();
        assert_eq!(sent["delivery"], "steer");
        let writes = world.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, methods::QUEUE_COMMAND);
        assert_eq!(writes[0].1["command"]["kind"], "steer");
    }

    #[tokio::test]
    async fn create_chat_writes_the_row_and_validates_model() {
        let world = Arc::new(World::default());
        let tools = tools(world.clone(), Origin::default());
        let err = tools
            .call(
                "create_chat",
                json!({ "project": "comet", "model": "nope" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("not offered"), "{err}");

        let created = tools
            .call(
                "create_chat",
                json!({ "project": "/repo/comet", "model": "sonnet", "title": "Review", "prompt": "go" }),
            )
            .await
            .unwrap();
        let chat_id = created["chatId"].as_str().unwrap().to_owned();
        let writes = world.writes.lock().unwrap();
        assert_eq!(writes.len(), 3, "createChat, renameChat, run");
        assert_eq!(writes[0].1["op"], "createChat");
        assert_eq!(writes[0].1["chatId"], chat_id);
        assert_eq!(writes[0].1["spaceId"], "space-1");
        assert!(
            writes[0].1.get("parentChatId").is_none(),
            "no origin, no parent"
        );
        assert_eq!(writes[0].1["config"]["harness"], "claude-code");
        assert_eq!(writes[0].1["config"]["model"], "sonnet");
        assert_eq!(writes[1].1["op"], "renameChat");
        assert_eq!(writes[2].0, methods::QUEUE_COMMAND);
        assert_eq!(writes[2].1["command"]["request"]["prompt"], "go");
    }

    #[tokio::test]
    async fn create_chat_sends_delegated_by_for_a_calling_chat() {
        let world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
            beta_parent: Some("chat-alpha-1".into()),
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let created = tools
            .call("create_chat", json!({ "project": "/repo/comet" }))
            .await
            .unwrap();
        let writes = world.writes.lock().unwrap();
        assert_eq!(writes[0].1["delegatedBy"], "chat-beta-2");
        assert_eq!(created["task"]["delegatedBy"], "chat-beta-2");
        assert_eq!(created["task"]["depth"], 2);
        // The task lists under the delegator's root chat.
        assert_eq!(created["parentChatId"], "chat-alpha-1");
    }

    #[tokio::test]
    async fn create_chat_with_notify_arms_and_adds_the_header() {
        let world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
            beta_parent: Some("chat-alpha-1".into()),
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let created = tools
            .call(
                "create_chat",
                json!({ "project": "/repo/comet", "prompt": "do it", "notify": true }),
            )
            .await
            .unwrap();
        let batch = created["task"]["batch"].as_str().unwrap().to_owned();
        assert_eq!(created["task"]["notify"], true);
        let writes = world.writes.lock().unwrap();
        let (method, command) = writes
            .iter()
            .find(|(m, _)| m == methods::QUEUE_COMMAND)
            .expect("an armed run command");
        assert_eq!(method, methods::QUEUE_COMMAND);
        assert_eq!(command["notify"]["batch"], batch);
        let prompt = command["command"]["request"]["prompt"].as_str().unwrap();
        assert!(
            prompt.starts_with("[Delegated task from Zeron chat Beta (chat-bet)"),
            "{prompt}"
        );
        assert!(
            prompt.ends_with(
                "

do it"
            ),
            "{prompt}"
        );
    }

    #[tokio::test]
    async fn notify_and_wait_cannot_be_combined() {
        let world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let err = tools
            .call(
                "create_chat",
                json!({ "project": "/repo/comet", "prompt": "x", "notify": true, "wait": true }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("notify and wait"), "{err}");
        assert!(world.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn notify_needs_a_prompt_and_a_calling_chat() {
        let world = Arc::new(World::default());
        // No origin: probes speak for no chat.
        let tools1 = tools(world.clone(), Origin::default());
        let err = tools1
            .call(
                "create_chat",
                json!({ "project": "/repo/comet", "prompt": "x", "notify": true }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("needs a calling chat"), "{err}");

        // With a chat but no prompt.
        let tools2 = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        let err = tools2
            .call(
                "create_chat",
                json!({ "project": "/repo/comet", "notify": true }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("notify needs a prompt"), "{err}");
        assert!(world.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn delegated_tasks_can_create_chats_and_forks_cannot() {
        // beta is a delegated task (delegation set): it may delegate.
        let task_world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
            ..Default::default()
        });
        let task = tools(
            task_world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        assert!(
            task.call("create_chat", json!({ "project": "/repo/comet" }))
                .await
                .is_ok()
        );

        // beta is a fork (parent, no delegation): the engine's message
        // surfaces through the tool unchanged.
        let fork_world = Arc::new(World {
            beta_parent: Some("chat-alpha-1".into()),
            ..Default::default()
        });
        let side = tools(
            fork_world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let err = side
            .call("create_chat", json!({ "project": "/repo/comet" }))
            .await
            .unwrap_err();
        assert!(
            err.contains(
                "Side chats cannot create chats. Ask your parent chat to create another side chat."
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn the_engine_depth_error_reaches_the_agent() {
        // beta is already a depth-2 task: the engine rejects with the
        // depth message and it reaches the caller as isError text.
        let world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-task-9", "depth": 2 })),
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let err = tools
            .call("create_chat", json!({ "project": "/repo/comet" }))
            .await
            .unwrap_err();
        assert!(err.contains("Delegation depth limit reached (2)"), "{err}");
        assert!(world.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_engine_sandbox_error_reaches_the_agent() {
        let world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
            beta_sandbox: Some("read-only"),
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let err = tools
            .call(
                "create_chat",
                json!({ "project": "/repo/comet", "sandbox": "workspace-write" }),
            )
            .await
            .unwrap_err();
        assert!(
            err.contains("read-only") && err.contains("workspace-write"),
            "{err}"
        );
        assert!(world.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_task_that_omits_sandbox_defaults_to_its_own_level_at_most() {
        for (caller_sandbox, sent) in [
            ("read-only", "read-only"),
            ("danger-full-access", "workspace-write"),
        ] {
            let world = Arc::new(World {
                beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
                beta_sandbox: Some(caller_sandbox),
                ..Default::default()
            });
            let tools = tools(
                world.clone(),
                Origin {
                    chat_id: Some("chat-beta-2".into()),
                    device_id: None,
                },
            );
            tools
                .call("create_chat", json!({ "project": "/repo/comet" }))
                .await
                .unwrap();
            let writes = world.writes.lock().unwrap();
            assert_eq!(
                writes[0].1["config"]["sandbox"], sent,
                "caller {caller_sandbox} defaults to {sent}"
            );
        }
        // A top-level caller keeps workspace-write.
        let world = Arc::new(World::default());
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        tools
            .call("create_chat", json!({ "project": "/repo/comet" }))
            .await
            .unwrap();
        let writes = world.writes.lock().unwrap();
        assert_eq!(writes[0].1["config"]["sandbox"], "workspace-write");
        assert_eq!(writes[0].1["delegatedBy"], "chat-alpha-1");
    }

    #[tokio::test]
    async fn create_chats_shares_one_batch_across_notify_requests() {
        let world = Arc::new(World::default());
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        let reply = tools
            .call(
                "create_chats",
                json!({ "requests": [
                    { "project": "/repo/comet", "prompt": "one", "notify": true },
                    { "project": "/repo/comet", "harness": "not-a-harness", "prompt": "bad", "notify": true },
                    { "project": "/repo/comet", "prompt": "two", "notify": true },
                ]}),
            )
            .await
            .unwrap();
        let results = reply["results"].as_array().unwrap();
        assert!(results[1]["isError"].as_bool().unwrap(), "{reply}");
        let batch = reply["batch"].as_str().expect("top-level batch");
        let writes = world.writes.lock().unwrap();
        let batches: Vec<_> = writes
            .iter()
            .filter(|(m, _)| m == methods::QUEUE_COMMAND)
            .filter_map(|(_, p)| p["notify"]["batch"].as_str().map(str::to_owned))
            .collect();
        assert_eq!(batches.len(), 2, "the failed request is not armed");
        assert!(batches.iter().all(|b| b == batch));
    }

    #[tokio::test]
    async fn the_live_task_limit_rejects_the_seventeenth_task() {
        // Sixteen live tasks under the root the caller's tasks list under.
        let world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
            beta_parent: Some("chat-alpha-1".into()),
            extra_chats: (0..16)
                .map(|i| {
                    json!({
                        "id": format!("task-{i}"), "deviceId": "dev-local",
                        "archived": false,
                        "delegation": { "by": "chat-alpha-1", "depth": 1 },
                        "parentChatId": "chat-alpha-1",
                        "createdAt": "2026-09-03T00:00:00Z"
                    })
                })
                .collect(),
            sessions: (0..16)
                .map(|i| {
                    json!({
                        "chatId": format!("task-{i}"), "deviceId": "dev-local",
                        "status": "working", "updatedAt": "2999-01-01T00:00:00Z"
                    })
                })
                .collect(),
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let err = tools
            .call(
                "create_chat",
                json!({ "project": "/repo/comet", "prompt": "seventeenth" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("16 delegated tasks"), "{err}");
        assert!(world.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn send_message_notify_requires_a_task_you_delegated() {
        // mine is a task the caller delegated; theirs is someone else's.
        let world = Arc::new(World {
            extra_chats: vec![
                json!({
                    "id": "chat-task-mine", "deviceId": "dev-local", "title": "Mine", "archived": false,
                    "delegation": { "by": "chat-alpha-1", "depth": 1 },
                    "parentChatId": "chat-alpha-1",
                    "config": { "harness": "claude-code", "sandbox": "workspace-write" },
                    "createdAt": "2026-09-03T00:00:00Z"
                }),
                json!({
                    "id": "chat-task-other", "deviceId": "dev-local", "title": "Other", "archived": false,
                    "delegation": { "by": "chat-beta-2", "depth": 1 },
                    "parentChatId": "chat-alpha-1",
                    "config": { "harness": "claude-code", "sandbox": "workspace-write" },
                    "createdAt": "2026-09-03T00:00:00Z"
                }),
            ],
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        let err = tools
            .call(
                "send_message",
                json!({ "chat": "chat-task-other", "text": "go", "notify": true }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("only for tasks you delegated"), "{err}");

        let sent = tools
            .call(
                "send_message",
                json!({ "chat": "chat-task-mine", "text": "keep going", "notify": true }),
            )
            .await
            .unwrap();
        assert_eq!(sent["task"]["notify"], true);
        let batch = sent["task"]["batch"].as_str().unwrap().to_owned();
        let writes = world.writes.lock().unwrap();
        let command = writes
            .iter()
            .find(|(m, _)| m == methods::QUEUE_COMMAND)
            .unwrap();
        assert_eq!(command.1["notify"]["batch"], batch);
    }

    #[tokio::test]
    async fn send_message_notify_rejects_queue_mode_and_wait() {
        let world = Arc::new(World {
            extra_chats: vec![json!({
                "id": "chat-task-mine", "deviceId": "dev-local", "title": "Mine", "archived": false,
                "delegation": { "by": "chat-alpha-1", "depth": 1 },
                "parentChatId": "chat-alpha-1",
                "config": { "harness": "claude-code", "sandbox": "workspace-write" },
                "createdAt": "2026-09-03T00:00:00Z"
            })],
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        for extra in [json!({ "wait": true }), json!({ "mode": "queue" })] {
            let mut call = json!({ "chat": "chat-task-mine", "text": "x", "notify": true });
            call.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert!(tools.call("send_message", call).await.is_err());
        }
        assert!(world.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn task_status_lists_the_tree_with_states() {
        // alpha's task task-1 is idle with a live grandchild → waitingForTasks.
        let world = Arc::new(World {
            extra_chats: vec![
                json!({
                    "id": "chat-task-1", "deviceId": "dev-local", "title": "T1", "archived": false,
                    "delegation": { "by": "chat-alpha-1", "depth": 1 },
                    "parentChatId": "chat-alpha-1",
                    "config": { "harness": "claude-code", "model": "opus", "sandbox": "workspace-write" },
                    "createdAt": "2026-09-03T00:00:00Z"
                }),
                json!({
                    "id": "chat-task-2", "deviceId": "dev-local", "title": "T2", "archived": false,
                    "delegation": { "by": "chat-task-1", "depth": 2 },
                    "parentChatId": "chat-alpha-1",
                    "config": { "harness": "claude-code", "sandbox": "workspace-write" },
                    "createdAt": "2026-09-04T00:00:00Z"
                }),
            ],
            sessions: vec![json!({
                "chatId": "chat-task-2", "deviceId": "dev-local",
                "status": "working", "updatedAt": "2999-01-01T00:00:00Z"
            })],
            delegations: vec![
                json!({ "chatId": "chat-task-1", "delegator": "chat-alpha-1", "batch": "b1", "notice": "armed" }),
                json!({ "chatId": "chat-task-2", "delegator": "chat-task-1", "batch": "b2", "notice": "armed" }),
            ],
            ..Default::default()
        });
        let tools = tools(
            world,
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        let listed = tools.call("task_status", json!({})).await.unwrap();
        let tasks = listed["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 1);
        let t1 = &tasks[0];
        assert_eq!(t1["chatId"], "chat-task-1");
        assert_eq!(t1["state"], "waitingForTasks");
        assert_eq!(t1["notice"], "armed");
        assert_eq!(t1["batch"], "b1");
        assert_eq!(t1["depth"], 1);
        let nested = t1["tasks"].as_array().unwrap();
        assert_eq!(nested[0]["chatId"], "chat-task-2");
        assert_eq!(nested[0]["state"], "working");
        assert_eq!(nested[0]["delegatedBy"], "chat-task-1");
    }

    #[tokio::test]
    async fn task_status_for_one_task_returns_the_reply() {
        let world = Arc::new(World {
            extra_chats: vec![json!({
                "id": "chat-task-1", "deviceId": "dev-local", "title": "T1", "archived": false,
                "delegation": { "by": "chat-alpha-1", "depth": 1 },
                "parentChatId": "chat-alpha-1",
                "config": { "harness": "claude-code", "sandbox": "workspace-write" },
                "createdAt": "2026-09-03T00:00:00Z"
            })],
            delegations: vec![json!({
                "chatId": "chat-task-1", "delegator": "chat-alpha-1",
                "batch": "b1", "notice": "settled", "outcome": "completed"
            })],
            ..Default::default()
        });
        let tools = tools(
            world,
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        let one = tools
            .call(
                "task_status",
                json!({ "chat": "chat-task-1", "reply_chars": 5 }),
            )
            .await
            .unwrap();
        assert_eq!(one["reply"], "hello");
        assert_eq!(one["replyTruncated"], true);
        assert!(one.get("pendingInput").is_some());
        assert_eq!(one["notice"], "settled");
        assert_eq!(one["state"], "completed");
    }

    #[tokio::test]
    async fn task_cancel_calls_the_engine_and_reports_both_lists() {
        let world = Arc::new(World {
            extra_chats: vec![json!({
                "id": "chat-task-1", "deviceId": "dev-local", "title": "T1", "archived": false,
                "delegation": { "by": "chat-alpha-1", "depth": 1 },
                "parentChatId": "chat-alpha-1",
                "createdAt": "2026-09-03T00:00:00Z"
            })],
            cancel_reply: json!({
                "chatId": "chat-task-1",
                "interrupted": [{ "chatId": "chat-task-1", "title": "T1", "wasState": "working" }],
                "notRunning": [],
            }),
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        let result = tools
            .call("task_cancel", json!({ "chat": "chat-task-1" }))
            .await
            .unwrap();
        assert_eq!(result["interrupted"][0]["wasState"], "working");
        let writes = world.writes.lock().unwrap();
        let cancel = writes
            .iter()
            .find(|(m, _)| m == methods::CANCEL_DELEGATED_TASK)
            .expect("one CancelDelegatedTask call");
        assert_eq!(cancel.1["chatId"], "chat-task-1");
    }

    #[tokio::test]
    async fn task_cancel_rejects_a_chat_outside_your_subtree() {
        let world = Arc::new(World {
            extra_chats: vec![json!({
                "id": "chat-task-other", "deviceId": "dev-local", "title": "Other", "archived": false,
                "delegation": { "by": "chat-beta-2", "depth": 1 },
                "createdAt": "2026-09-03T00:00:00Z"
            })],
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        let err = tools
            .call("task_cancel", json!({ "chat": "chat-task-other" }))
            .await
            .unwrap_err();
        assert!(err.contains("tasks you delegated"), "{err}");
        assert!(world.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_chats_parent_matches_the_delegator() {
        // task-1's children have parentChatId == root; a task listing its own
        // children must match delegation.by.
        let world = Arc::new(World {
            extra_chats: vec![
                json!({
                    "id": "chat-task-1", "deviceId": "dev-local", "title": "T1", "archived": false,
                    "delegation": { "by": "chat-alpha-1", "depth": 1 },
                    "parentChatId": "chat-alpha-1",
                    "createdAt": "2026-09-03T00:00:00Z"
                }),
                json!({
                    "id": "chat-task-2", "deviceId": "dev-local", "title": "T2", "archived": false,
                    "delegation": { "by": "chat-task-1", "depth": 2 },
                    "parentChatId": "chat-alpha-1",
                    "createdAt": "2026-09-04T00:00:00Z"
                }),
            ],
            ..Default::default()
        });
        let tools = tools(world, Origin::default());
        let listed = tools
            .call("list_chats", json!({ "parent": "chat-task-1" }))
            .await
            .unwrap();
        let ids: Vec<_> = listed["chats"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["chat-task-2"]);
    }

    #[tokio::test]
    async fn whoami_reports_delegation() {
        let world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
            ..Default::default()
        });
        let tools1 = tools(
            world,
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let me = tools1.call("whoami", json!({})).await.unwrap();
        assert_eq!(me["delegation"]["delegatedBy"], "chat-alpha-1");
        assert_eq!(me["delegation"]["depth"], 1);
        assert_eq!(me["delegation"]["maxDepth"], 2);
        assert_eq!(me["delegation"]["canDelegate"], true);

        // A fork cannot delegate.
        let world = Arc::new(World {
            beta_parent: Some("chat-alpha-1".into()),
            ..Default::default()
        });
        let tools2 = tools(
            world,
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        let me = tools2.call("whoami", json!({})).await.unwrap();
        assert_eq!(me["delegation"]["canDelegate"], false);
    }

    #[tokio::test]
    async fn wait_on_an_idle_chat_returns_immediately() {
        let world = Arc::new(World::default());
        let tools = tools(world, Origin::default());
        let waited = tools
            .call(
                "wait_for_turn",
                json!({ "chat": "alpha", "timeout_secs": 5 }),
            )
            .await
            .unwrap();
        assert_eq!(waited["turn"]["outcome"], "completed");
        assert_eq!(waited["turn"]["replies"][0]["text"], "hello back");
    }

    #[tokio::test]
    async fn send_with_wait_keeps_waiting_until_a_turn_lands() {
        // The stub never grows a session row, so a send-then-wait must run
        // to its deadline rather than declaring the (unstarted) run done.
        let world = Arc::new(World::default());
        let tools = tools(world, Origin::default());
        let started = std::time::Instant::now();
        let sent = tools
            .call(
                "send_message",
                json!({ "chat": "alpha", "text": "hi", "wait": true, "timeout_secs": 1 }),
            )
            .await
            .unwrap();
        assert_eq!(sent["turn"]["outcome"], "timedOut");
        assert!(started.elapsed() >= Duration::from_millis(900));
    }

    #[tokio::test]
    async fn batches_dispatch_all_chats_before_waiting_and_keep_partial_results() {
        for (name, requests) in [
            (
                "create_chats",
                json!([
                    {"prompt": "first", "wait": true, "timeout_secs": 1},
                    {"harness": "not-a-harness"},
                    {"prompt": "second", "wait": true, "timeout_secs": 1},
                ]),
            ),
            (
                "send_messages",
                json!([
                    {"chat": "alpha", "text": "first", "wait": true, "timeout_secs": 1},
                    {"chat": "missing", "text": "invalid"},
                    {"chat": "beta", "text": "second", "wait": true, "timeout_secs": 1},
                ]),
            ),
        ] {
            let world = Arc::new(World {
                dispatch_barrier: Some(tokio::sync::Barrier::new(2)),
                ..Default::default()
            });
            let tools = tools(world.clone(), Origin::default());
            let reply = tokio::time::timeout(
                Duration::from_secs(3),
                crate::jsonrpc::handle_request(
                    &tools,
                    json!(42),
                    "tools/call",
                    json!({"name": name, "arguments": {"requests": requests}}),
                ),
            )
            .await
            .expect("both dispatches must proceed while other requests are waiting");
            assert_eq!(reply["result"]["isError"], false, "{reply}");
            let results = &reply["result"]["structuredContent"]["results"];
            for i in [0, 2] {
                assert_eq!(results[i]["index"], i);
                assert_eq!(results[i]["isError"], false, "{reply}");
                assert_eq!(results[i]["result"]["turn"]["outcome"], "timedOut");
            }
            assert_eq!(results[1]["index"], 1);
            assert_eq!(results[1]["isError"], true);
            let writes = world.writes.lock().unwrap();
            let dispatched: Vec<_> = writes
                .iter()
                .filter(|(method, _)| method == methods::QUEUE_COMMAND)
                .collect();
            assert_eq!(dispatched.len(), 2);
            assert_ne!(dispatched[0].1["chatId"], dispatched[1].1["chatId"]);
        }
    }

    #[tokio::test]
    async fn invalid_batch_sizes_have_no_side_effects() {
        let world = Arc::new(World::default());
        let tools = tools(world.clone(), Origin::default());
        for name in ["create_chats", "send_messages"] {
            for requests in [vec![], vec![json!({"prompt": "hello"}); MAX_BATCH + 1]] {
                assert!(
                    tools
                        .call(name, json!({"requests": requests}))
                        .await
                        .is_err()
                );
            }
        }
        assert!(world.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn initialize_and_list_over_jsonrpc() {
        let world = Arc::new(World::default());
        let tools = tools(world, Origin::default());
        let init = crate::jsonrpc::handle_request(
            &tools,
            json!(1),
            "initialize",
            json!({ "protocolVersion": "2025-03-26" }),
        )
        .await;
        assert_eq!(init["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(init["result"]["serverInfo"]["name"], "zeron");
        let list =
            crate::jsonrpc::handle_request(&tools, json!(2), "tools/list", Value::Null).await;
        assert!(list["result"]["tools"].as_array().unwrap().len() >= 10);
        let bad = crate::jsonrpc::handle_request(
            &tools,
            json!(3),
            "tools/call",
            json!({ "name": "nope" }),
        )
        .await;
        assert_eq!(bad["error"]["code"], -32602);
        let whoami = crate::jsonrpc::handle_request(
            &tools,
            json!(4),
            "tools/call",
            json!({ "name": "whoami" }),
        )
        .await;
        assert_eq!(whoami["result"]["isError"], false);
        assert_eq!(
            whoami["result"]["structuredContent"]["localDeviceId"],
            "dev-local"
        );
    }

    #[tokio::test]
    async fn create_chats_seals_its_batch_once_after_all_requests() {
        // One seal call after every arm lands, even with a failed request —
        // the unsealed batch must never release on a fast finisher alone.
        let world = Arc::new(World::default());
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-alpha-1".into()),
                device_id: None,
            },
        );
        let reply = tools
            .call(
                "create_chats",
                json!({ "requests": [
                    { "project": "/repo/comet", "prompt": "one", "notify": true },
                    { "project": "/repo/comet", "harness": "not-a-harness", "prompt": "bad", "notify": true },
                    { "project": "/repo/comet", "prompt": "two", "notify": true },
                ]}),
            )
            .await
            .unwrap();
        let batch = reply["batch"].as_str().expect("top-level batch");
        let writes = world.writes.lock().unwrap();
        let arms: Vec<_> = writes
            .iter()
            .filter(|(m, _)| m == methods::QUEUE_COMMAND)
            .collect();
        assert_eq!(arms.len(), 2);
        assert!(arms.iter().all(|(_, p)| p["notify"]["seal"] == false));
        let seals: Vec<_> = writes
            .iter()
            .filter(|(m, _)| m == methods::SEAL_DELEGATION_BATCH)
            .collect();
        assert_eq!(seals.len(), 1, "exactly one seal, after the arms");
        assert_eq!(seals[0].1["batch"], batch);
        assert_eq!(seals[0].1["delegator"], "chat-alpha-1");
        let seal_pos = writes
            .iter()
            .position(|(m, _)| m == methods::SEAL_DELEGATION_BATCH)
            .unwrap();
        let last_arm = writes
            .iter()
            .rposition(|(m, _)| m == methods::QUEUE_COMMAND)
            .unwrap();
        assert!(seal_pos > last_arm, "seal lands after every arm");
    }

    #[tokio::test]
    async fn a_single_notify_create_chat_seals_with_the_arm() {
        let world = Arc::new(World {
            beta_delegation: Some(json!({ "by": "chat-alpha-1", "depth": 1 })),
            ..Default::default()
        });
        let tools = tools(
            world.clone(),
            Origin {
                chat_id: Some("chat-beta-2".into()),
                device_id: None,
            },
        );
        tools
            .call(
                "create_chat",
                json!({ "project": "/repo/comet", "prompt": "go", "notify": true }),
            )
            .await
            .unwrap();
        let writes = world.writes.lock().unwrap();
        let (m, p) = writes
            .iter()
            .find(|(m, _)| m == methods::QUEUE_COMMAND)
            .unwrap();
        assert_eq!(m, methods::QUEUE_COMMAND);
        assert_eq!(p["notify"]["seal"], true);
        assert!(
            writes
                .iter()
                .all(|(m, _)| m != methods::SEAL_DELEGATION_BATCH)
        );
    }
}

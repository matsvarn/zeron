use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use std::sync::Arc;
use zeron_doc::{MessagePart, MessageRole, MessageStatus, SessionMessageEntry};
use zeron_engine::{EngineCore, HarnessRegistry};
use zeron_harness::{Harness, HarnessError, RunControls};
use zeron_proto::{
    AgentEvent, Delegation, DoneStatus, HarnessId, Model, ReasoningLevel, RunRequest, SandboxLevel,
    SteeringMode,
};
use zeron_rpc::{RpcClient, methods};

struct Stub;
#[async_trait]
impl Harness for Stub {
    fn id(&self) -> HarnessId {
        HarnessId::Mock
    }
    fn display_name(&self) -> &str {
        "Stub"
    }
    fn supports_steering(&self) -> bool {
        false
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::TurnBoundary
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        &[]
    }
    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        Ok(vec![])
    }
    async fn run(
        &self,
        _: RunRequest,
        _: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        Ok(futures::stream::iter(vec![Ok(AgentEvent::Done {
            status: DoneStatus::Completed,
            result: None,
            error: None,
            session_id: Some("stub-session".into()),
        })])
        .boxed())
    }
}

fn assemble_at(path: &std::path::Path) -> EngineCore {
    let registry = HarnessRegistry::new();
    registry.register(Arc::new(Stub));
    EngineCore::assemble(path, Arc::new(registry), HarnessId::Mock, None)
        .expect("engine core assembles")
}

/// A top-level chat, pre-titled so the auto-titler never runs the stub.
fn root(core: &EngineCore, id: &str) {
    core.workspace
        .create_chat(id, None, Some(&core.device_id), None, Some("/tmp".into()))
        .unwrap();
    core.workspace.rename_chat(id, id).unwrap();
}

fn chat_config(sandbox: &str) -> serde_json::Value {
    serde_json::json!({
        "harness": "mock",
        "model": "mock-1",
        "reasoning": null,
        "sandbox": sandbox,
    })
}

/// `Mutate createChat` with `delegatedBy`. `sandbox == None` sends no config.
async fn delegate(
    client: &RpcClient,
    device_id: &str,
    by: &str,
    id: &str,
    sandbox: Option<&str>,
) -> Result<(), zeron_rpc::RpcError> {
    let mut params = serde_json::json!({
        "op": "createChat",
        "chatId": id,
        "deviceId": device_id,
        "delegatedBy": by,
    });
    if let Some(sandbox) = sandbox {
        params["config"] = chat_config(sandbox);
    }
    client.call(methods::MUTATE, params).await.map(|_| ())
}

fn message(id: &str, role: MessageRole, text: &str, status: MessageStatus) -> SessionMessageEntry {
    SessionMessageEntry {
        duration_ms: None,
        id: id.into(),
        role,
        parts: vec![MessagePart::Text {
            id: format!("{id}-text"),
            text: text.into(),
        }],
        created_at: 1,
        device_id: "device".into(),
        status: Some(status),
        continuation_of: None,
    }
}

/// Give a chat one finished turn so `ForkSideChat` finds a boundary.
fn finished_turn(core: &EngineCore, id: &str) {
    let doc = core.doc_host.open(id).unwrap();
    doc.doc()
        .push_message(&message(
            &format!("u-{id}"),
            MessageRole::User,
            "question",
            MessageStatus::Complete,
        ))
        .unwrap();
    doc.doc()
        .push_message(&message(
            &format!("a-{id}"),
            MessageRole::Assistant,
            "answer",
            MessageStatus::Complete,
        ))
        .unwrap();
}

async fn fork(
    client: &RpcClient,
    device_id: &str,
    source: &str,
    id: &str,
) -> Result<zeron_proto::Chat, zeron_rpc::RpcError> {
    client
        .call_as::<zeron_proto::Chat>(
            methods::FORK_SIDE_CHAT,
            serde_json::json!({
                "chatId": id,
                "sourceChatId": source,
                "targetDeviceId": device_id,
            }),
        )
        .await
}

#[tokio::test]
async fn create_chat_records_the_delegator_and_lists_the_task_under_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    let client = zeron_rpc::memory_client(core.rpc_service());
    root(&core, "root");

    delegate(&client, &core.device_id, "root", "task-1", None)
        .await
        .unwrap();
    let task = core.workspace.chat("task-1").unwrap().unwrap();
    assert_eq!(
        task.delegation,
        Some(Delegation {
            by: "root".into(),
            depth: 1
        })
    );
    assert_eq!(task.parent_chat_id.as_deref(), Some("root"));

    delegate(&client, &core.device_id, "task-1", "task-2", None)
        .await
        .unwrap();
    let nested = core.workspace.chat("task-2").unwrap().unwrap();
    assert_eq!(
        nested.delegation,
        Some(Delegation {
            by: "task-1".into(),
            depth: 2
        })
    );
    assert_eq!(nested.parent_chat_id.as_deref(), Some("root"));
    core.shutdown().await;
}

#[tokio::test]
async fn the_engine_computes_depth_and_rejects_the_third_level() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    let client = zeron_rpc::memory_client(core.rpc_service());
    root(&core, "root");
    delegate(&client, &core.device_id, "root", "task-1", None)
        .await
        .unwrap();
    delegate(&client, &core.device_id, "task-1", "task-2", None)
        .await
        .unwrap();

    let err = delegate(&client, &core.device_id, "task-2", "task-3", None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Delegation depth limit reached (2)"),
        "unexpected error: {err}"
    );
    assert!(core.workspace.chat("task-3").unwrap().is_none());
    core.shutdown().await;
}

#[tokio::test]
async fn forks_and_old_children_cannot_delegate() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    let client = zeron_rpc::memory_client(core.rpc_service());
    root(&core, "root");
    finished_turn(&core, "root");

    // A fork has a parent and no delegation: today's rule still applies.
    fork(&client, &core.device_id, "root", "fork")
        .await
        .unwrap();
    // A child row written before `delegation` existed: parent, no field.
    client
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createChat",
                "chatId": "old-child",
                "deviceId": core.device_id,
                "parentChatId": "root",
            }),
        )
        .await
        .unwrap();
    for by in ["fork", "old-child"] {
        let err = delegate(&client, &core.device_id, by, &format!("task-of-{by}"), None)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "Side chats cannot create chats. Ask your parent chat to create another side chat."
        );
        assert!(
            core.workspace
                .chat(&format!("task-of-{by}"))
                .unwrap()
                .is_none()
        );
    }
    core.shutdown().await;
}

#[tokio::test]
async fn a_fork_of_a_task_is_not_a_task() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    let client = zeron_rpc::memory_client(core.rpc_service());
    root(&core, "root");
    delegate(&client, &core.device_id, "root", "task-1", None)
        .await
        .unwrap();
    finished_turn(&core, "task-1");

    // The fork button on a task passes the root so the copy lists as a sibling.
    let forked = client
        .call_as::<zeron_proto::Chat>(
            methods::FORK_SIDE_CHAT,
            serde_json::json!({
                "chatId": "fork-of-task",
                "sourceChatId": "task-1",
                "parentChatId": "root",
                "targetDeviceId": core.device_id,
            }),
        )
        .await
        .unwrap();
    assert_eq!(forked.delegation, None);
    assert_eq!(forked.parent_chat_id.as_deref(), Some("root"));
    core.shutdown().await;
}

#[tokio::test]
async fn a_read_only_task_cannot_create_a_workspace_write_task() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    let client = zeron_rpc::memory_client(core.rpc_service());
    root(&core, "root");
    delegate(
        &client,
        &core.device_id,
        "root",
        "ro-task",
        Some("read-only"),
    )
    .await
    .unwrap();

    for level in ["workspace-write", "danger-full-access"] {
        let err = delegate(
            &client,
            &core.device_id,
            "ro-task",
            &format!("child-{level}"),
            Some(level),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("read-only") && err.contains(level),
            "unexpected error: {err}"
        );
        assert!(
            core.workspace
                .chat(&format!("child-{level}"))
                .unwrap()
                .is_none()
        );
    }
    core.shutdown().await;
}

#[tokio::test]
async fn a_workspace_write_task_can_create_read_only_and_workspace_write_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    let client = zeron_rpc::memory_client(core.rpc_service());
    root(&core, "root");
    delegate(
        &client,
        &core.device_id,
        "root",
        "ww-task",
        Some("workspace-write"),
    )
    .await
    .unwrap();

    for level in ["read-only", "workspace-write"] {
        delegate(
            &client,
            &core.device_id,
            "ww-task",
            &format!("child-{level}"),
            Some(level),
        )
        .await
        .unwrap();
        let child = core
            .workspace
            .chat(&format!("child-{level}"))
            .unwrap()
            .unwrap();
        let sandbox = child.config.as_ref().unwrap().sandbox;
        assert_eq!(sandbox.label(), level);
        assert_eq!(child.delegation.unwrap().depth, 2);
    }
    let err = delegate(
        &client,
        &core.device_id,
        "ww-task",
        "child-danger",
        Some("danger-full-access"),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("workspace-write") && err.contains("danger-full-access"),
        "unexpected error: {err}"
    );
    assert!(core.workspace.chat("child-danger").unwrap().is_none());
    core.shutdown().await;
}

#[tokio::test]
async fn a_top_level_read_only_chat_can_still_create_a_danger_full_access_task() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    let client = zeron_rpc::memory_client(core.rpc_service());
    client
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createChat",
                "chatId": "ro-root",
                "deviceId": core.device_id,
                "config": chat_config("read-only"),
            }),
        )
        .await
        .unwrap();
    core.workspace.rename_chat("ro-root", "ro-root").unwrap();

    delegate(
        &client,
        &core.device_id,
        "ro-root",
        "task-1",
        Some("danger-full-access"),
    )
    .await
    .unwrap();
    let task = core.workspace.chat("task-1").unwrap().unwrap();
    assert_eq!(
        task.config.as_ref().unwrap().sandbox,
        SandboxLevel::DangerFullAccess
    );
    core.shutdown().await;
}

#[tokio::test]
async fn a_task_without_a_config_counts_as_workspace_write() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    let client = zeron_rpc::memory_client(core.rpc_service());
    root(&core, "root");
    delegate(&client, &core.device_id, "root", "cfg-free", None)
        .await
        .unwrap();
    assert!(
        core.workspace
            .chat("cfg-free")
            .unwrap()
            .unwrap()
            .config
            .is_none()
    );

    delegate(
        &client,
        &core.device_id,
        "cfg-free",
        "child-ww",
        Some("workspace-write"),
    )
    .await
    .unwrap();
    let err = delegate(
        &client,
        &core.device_id,
        "cfg-free",
        "child-danger",
        Some("danger-full-access"),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("workspace-write") && err.contains("danger-full-access"),
        "unexpected error: {err}"
    );
    assert!(core.workspace.chat("child-danger").unwrap().is_none());
    core.shutdown().await;
}

#[tokio::test]
async fn delegation_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble_at(dir.path());
    {
        let client = zeron_rpc::memory_client(core.rpc_service());
        root(&core, "root");
        delegate(&client, &core.device_id, "root", "task-1", None)
            .await
            .unwrap();
    }
    core.shutdown().await;
    drop(core); // releases the instance lock on the data dir

    let core = assemble_at(dir.path());
    let task = core.workspace.chat("task-1").unwrap().unwrap();
    assert_eq!(
        task.delegation,
        Some(Delegation {
            by: "root".into(),
            depth: 1
        })
    );
    assert_eq!(task.parent_chat_id.as_deref(), Some("root"));
    core.shutdown().await;
}

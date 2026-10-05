Implements #768: agents can launch delegated tasks that report back on their own, nest one level deeper, and be inspected or cancelled.

## What changes

- **`notify: true`** on `create_chat(s)` / `send_message(s)`. The call returns at once. When the task settles (completed, errored, interrupted), the host engine delivers its final message to the delegator as a quoted, untrusted task notice: a new turn if the delegator is idle, a mid-turn steer where the harness supports it, otherwise the queue. It never interrupts a turn, and a delegator the user stopped keeps the notice in its frozen queue. Tasks launched in one call report together; if some are still working after 15 minutes, the finished ones are reported early.
- **Needs-input notices**: a task blocked on an approval or question reports it to its delegator immediately; `respond_to_input` answers it (and now rejects unknown request/question ids).
- **Nesting**: MCP-created chats carry `Chat.delegation { by, depth }` and may delegate down to depth 2. A task that delegates reports only after its own tasks have reported to it. `parent_chat_id` stays at the root, so nested tasks keep the existing root-level placement in the Chats footer.
- **`task_status` / `task_cancel`**: the caller's task tree with states and results; cancel stops a task and everything below it and sends no notice. It first attempts to save the cancellation; then running turns are interrupted, pending Run/Steer commands rejected (control commands are left alone) and queued messages removed. Every wait in that cleanup is bounded. If the save or the cleanup fails, `task_cancel` returns a retryable error, and the cancellation may already be saved; retrying is safe.
- **Later results (change from #768)**: the issue proposed holding a task open while Claude Code background shells ran. I dropped that: it changed Stop and idle-reap behaviour for every Claude chat and kept producing races. Instead the first turn's answer is the task's result, and if the task later completes a turn on its own (e.g. Claude's wake turn after a background build), the delegator gets a "later result" notice. Later results are delivered in order, only after the first result, once each across restarts (outside the crash windows listed under Limitations), and stop once someone sends the task a real message. Engine notices don't end eligibility, so a nested task's improved answer propagates up the chain.
- **Durability**: owed notices live in a host-local ledger (`{store_root}/delegations.json`, atomic writes, corrupt-file backup, not synced) with deterministic notice ids. An obligation is retired only after its notice is durably in the delegator's transcript or queue (or the delegator was archived or deleted, which deliberately gets no notice). Shutdown stops the watcher, lets in-flight deliveries finish before queues freeze, and leaves remaining obligations in the ledger for the next start.

## Behaviour changes for chats that don't use `notify`

- MCP-created children are recorded as delegated tasks even with `notify: false` (affects nesting eligibility and sandbox requests). Plain engine creation under a side-chat parent is rejected.
- Chat summaries and `whoami` include delegation info; registry writes include `delegation` (null for ordinary chats).
- Client-supplied Run/Steer message ids in the `notice-*` namespace are rejected at the RPC and relay boundaries. Synced peers are trusted as before.
- The transcript renders task-notice headers as a one-line attribution.
- Every engine loads the ledger and starts the delegation watcher; with nothing owed it only keeps a session-status map and a 1 s tick that returns immediately. Shutdown stops the watcher and coordinates with in-flight deliveries before freezing queues.
- Codex async questions become pending input, and `respond_to_input` validates ids against the transcript.
- Public Rust surface: `Chat.delegation`, `AgentEvent::Steered.internal`, `EngineCore.delegation`, an extra `create_chat_with_parent` argument, `DocHost::pause_all_queues` is now async. RPC adds `delegatedBy`, optional `QueueCommand.notify`, and three delegation methods.

## Independent fixes (separate commits; happy to split into their own PRs)

1. `respond_to_input` rejects unknown question and request ids.
2. Codex `request_user_input_async` questions are routed through the input bridge.
3. `RLIMIT_NOFILE` is raised at engine startup.
4. Quit-raced streaming entries are stamped from the run journal at boot (`Done{interrupted}` → aborted).
5. Crash-revival dispatch waits for the IPC port so revived runs get the Zeron MCP server.
6. Transcript writes and reads are serialized per session doc, so a concurrent reader (or a snapshot export) never sees a half-written row, and a streaming reply can't claim a row index another writer just took. This affects every chat: typed transcript/command reads and whole-row writes on one shared session handle now take a short per-chat (reentrant) lock, and snapshot persistence never waits inside a document callback: a `dirty` signal raised while a flush is running is picked up by that flush's post-release recheck, while an explicit `flush_sync` from inside a running flush on the same thread returns a retryable error instead of deadlocking.

## Limitations

- `notify` requires both the task and the delegator to be hosted on the engine handling the call; cancelling a remote descendant leaves it running and reports it under `notStopped`.
- Sandbox levels are recorded and enforced on creation, but the Codex and Claude adapters don't enforce them at run time (finding 1 in #768).
- The ledger keeps the latest outcome per chat and compacts toward 1,000 records host-wide; records still owed a delivery are protected and can exceed that target. Cancellation suppresses any further delivery.
- A notify re-arm whose command then fails to queue is rolled back in memory and persisted on the next successful ledger write; a crash before that write leaves the new arm on disk, which settles as never-started, and the earlier task's pending result is not reported.
- The cancel stop fence is in-memory and does not survive a restart; boot-time crash revival does not consult the ledger, so a chat whose task is cancelled can still be revived if it had an open journal — its result is never delivered.
- Another writer's unguarded commit can publish a row that is still being written to the sync outbox; a crash in that window can persist a notice without its text, which restart recovery treats as delivered.
- If the engine dies after a turn's journal `Done` but before its final transcript is durable, the reply recovered at the next start can be incomplete; its notice says so.
- Not verified with mixed-version clients.

## Testing

- `crates/engine/tests/delegated_tasks.rs`: 153 tests covering batching, progress release, needs-input, nesting, cancel, crash/quit/restart windows, later-result ordering and replay, and reserved ids. Loaded stress: 108 full-suite runs at `--test-threads=16`, six in parallel, with no failures.
- Engine, MCP and doc libraries, the UI task-header test, and the side_chats, restart_resume, message_queue, workspace_sync and subagent_idle_reap suites pass. Each commit passes `cargo check --workspace --all-targets` on its own.
- Manual live runs on my machine (macOS, based on v0.2.102) with Claude Code, Codex, Devin and Pi as lead and as task, including engine crash, graceful quit and restart mid-task. These predate the switch to later results; the later-result path is covered by the engine tests.

The commits are ordered so the prerequisite fixes come first, then the data model, the delivery engine, and the MCP surface.

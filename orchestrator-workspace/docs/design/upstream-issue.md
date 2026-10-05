# Delegated tasks: async results for agent-created chats, and one more level of nesting

## Summary

Agents in Zeron can already delegate across harnesses: the injected `zeron mcp` server lets a Claude Code chat create a Codex chat (or any other harness), send it a prompt, and wait for the reply. That works well for short tasks. Two gaps stop it from working as orchestration:

1. **No result delivery.** A lead that launches a child with `wait: false` is never told when the child finishes. The only way to get the result is to block in `wait: true` / `wait_for_turn`, and on Codex that silently fails for long tasks (evidence below).
2. **No nesting.** Any chat with a parent is rejected by `create_chat` ("Side chats cannot create chats", `crates/mcp/src/tools.rs:662` on `main`). A forked side chat and an agent-created worker are the same record (`Chat.parent_chat_id`, `crates/proto/src/entities.rs:234`), so the engine can't give them different rules.

I'm proposing a small, opt-in change that closes both, and I'll open a PR that implements it. If you'd rather take it in smaller pieces, I'm happy to split it (data model → delivery → nesting/cancel/restart → MCP surface), and the independent fixes listed further down can go in separately.

## Evidence (v0.2.102, macOS, one device)

Leads were top-level chats; each child was asked to reply with a token.

| Lead → child | `wait: true`, short task | `wait: false` | `wait: true`, child runs `sleep 180` |
|---|---|---|---|
| Claude Code → Codex | reply returned | lead never notified | held ~3m10s, reply returned |
| Codex → Claude Code | reply returned | — | — |
| Codex → Codex | — | — | **failed in 2 of 3 runs:** the call was backgrounded by Codex code mode ("Script running with cell ID 1"), the lead ended its turn ~40–43 s after the child started, the child finished about 2.5 min later, and the reply never reached the lead, with no error anywhere. The third run held ~3m25s and returned the reply. |
| Devin → Claude Code / Codex | reply returned | — | held ~3m30s, reply returned |
| Pi → Codex | reply returned | — | held ~3m30s, reply returned |

So blocking waits are not a dependable fallback: at least one major harness intermittently drops long tool calls, and the agent can't tell it happened.

## Proposal

- **Mark delegated tasks.** One optional, additive field on the chat row: `delegation: { by, depth }`, set by the engine in `createChat` when an agent creates a chat. Forks and existing children don't have it and keep today's rules. No migration, no edge change (the registry room only validates field names and op size). `parent_chat_id` keeps pointing at the top-level chat, so nested tasks list in the existing **Chats** footer and no UI change is needed.
- **`notify: true`** on `create_chat(s)` / `send_message(s)`. The call returns at once. When a task settles (completed, errored, interrupted), the host engine delivers its final message to the delegator through the existing `deliver_prompt` path: a new turn if idle, a mid-turn steer where the harness supports it, otherwise the queue. It never interrupts a turn. A delegator the user stopped keeps the notice in its frozen queue rather than waking. Tasks launched in one call report together in one notice; if some of them are still working after 15 minutes, the finished ones are reported early and the rest follow.
- **Needs-input notice.** MCP runs start with `auto_approve: false`, so a task can block on an approval or question nobody is watching. The delegator gets an immediate notice with the question and its ids, and can answer via `respond_to_input`.
- **Nesting with a cap.** Delegated tasks may delegate up to depth 2. A task that delegates reports only after its own tasks have reported to it. A task can't create a task with a higher sandbox level than its own; top-level chats are unchanged. (The cap is recorded and enforced on the chat row, but see finding 1: the Codex and Claude adapters run with full access regardless.)
- **Background work holds a task open.** Claude Code moves long commands to the background and ends its turn early, then runs a wake turn when they finish. A delegated task only settles once its background shells are done, so the delegator gets the real answer instead of "waiting for it to finish".
- **`task_status` and `task_cancel`.** Status lists the caller's task tree with states and results; cancel stops a task and everything below it and sends no notice.
- **Task output is quoted as untrusted data** inside the notice (a delimiter the task text can't close), since the notice arrives as a user-role message.
- **State** for owed notices lives in a host-local ledger file with deterministic notice ids, so restarts neither lose nor duplicate a notice. `notify` is limited to chats hosted on the engine that handles the call in this first version.

Most of the implementation is a new `crates/engine/src/delegation.rs` plus `crates/mcp/src/tools.rs`, with engine tests following the patterns in `crates/engine/tests/side_chats.rs`, `message_queue.rs` and `restart_resume.rs`. It passes live cross-harness runs (Claude Code, Codex, Devin and Pi as lead and as task), including engine crash, graceful quit and restart mid-task.

## Findings from building it (independent of delegation)

1. **The `sandbox` argument has no effect for Codex and Claude Code.** The Codex adapter deliberately forces `danger-full-access` and approval policy `never` for every non-title run (`crates/harness/src/codex/mod.rs:696-707`), and the Claude adapter auto-allows every tool call. A chat created with `sandbox: "read-only"` ran `touch` in the project folder successfully. If that's intended, the `create_chat` tool description probably shouldn't suggest otherwise.
2. **Archived chats keep their harness process warm.** `set_chat_archived` (`crates/engine/src/workspace_host.rs:1102`) is a registry change only; nothing stops the parked process. Combined with the default 256 open-file limit on macOS (each warm process holds ~3 pipes), the engine hit `Too many open files (os error 24)` after about 50 chats in 30 minutes, and new runs failed to start. Raising `RLIMIT_NOFILE` at startup fixes the symptom; reaping on archive would be the other half.
3. **`respond_to_input` accepts unknown ids silently.** An answer with a `question_id` that isn't in the pending request returns success, and the agent's question tool receives no answer.
4. **A graceful quit can leave a reply "streaming" forever.** On Cmd-Q the run is interrupted and `Done{interrupted}` reaches the run journal, but the app can exit (`timed out waiting on app_will_quit`) before the doc entry is stamped `aborted`. At the next boot `recover_stale` only visits journals that don't end in `Done`, so the entry is never corrected: the transcript shows the reply as still running, and anything reading the entry's status sees neither completed nor interrupted.
5. **Codex's `request_user_input_async` isn't recognized as a question.** Codex 0.160 posts the question on a completed `agentMessage` item (no `item/tool/requestUserInput` server request) and then waits for the answer as a user message, sleeping in a loop. Zeron sees ordinary text, so the chat stays "working" with no pending input.
6. **Runs dispatched during engine assembly have no Zeron MCP server.** `zeron_mcp` (`crates/engine/src/sessions.rs:1197`) only injects the server once `ipc_port` is set, which happens after `EngineCore::assemble` returns. A crash-revival re-dispatch from `recover_stale` can start in that window, so the revived agent runs without Zeron tools.

## Questions for you

1. Is the one-level limit there for a reason beyond flat UI and runaway spawning?
2. Is it OK that `parent_chat_id` stays at the root and `delegation.by` holds the real delegator, or would you rather have a true tree plus UI work?
3. Is a host-local ledger (and `notify` on one device) acceptable for a first version?
4. Text header on notices, or would you rather add a structured sender to messages?
5. Separate from this: the loopback IPC port accepts connections without a credential, so any local process (including an agent with a shell) can call `setChatConfig` or raise a chat's sandbox. Worth its own issue?

Small unrelated finding: `list_harnesses` reports `steersMidTurn: false` for Grok, Devin and OpenCode until first use, because the lazy descriptors in `crates/engine/src/registry.rs` (652/670/726) say `TurnBoundary` while the resolved harnesses say `StepBoundary`. Delivery reads the live run, so it isn't affected.

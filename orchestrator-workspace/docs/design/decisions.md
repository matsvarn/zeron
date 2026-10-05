# Decisions for Mats

Each item names a choice in [delegated-tasks.md](delegated-tasks.md). The recommendation comes first. Items 1 to 4 change the shape of the work. The rest are defaults that are cheap to change later.

## Status

Recorded on 2026-10-02 from review feedback relayed by the lead chat "Xeron Agent Orchestration Design".

| Items | Status |
|---|---|
| 1 to 11 | Approved as recommended. |
| 12 | Approved and done. Probe S14 ran on the stock app. Result below. |
| 13 | Decided in review. Sandbox cap for tasks. |
| 14 | Decided in review. Task output in a notice is untrusted data. |

## 1. What a notice contains

**Recommend: the task's final message, capped at 8,000 characters per task.**

T3 sends a pointer and makes the parent call `task_status`. That keeps the wake message small and gives T3 an explicit acknowledge step. It also costs a tool call per task and depends on the lead doing the follow-up. Your brief asks for automatic result delivery, and inline text works on a harness whose agent ignores instructions. The cap protects the lead's context.

Alternative: pointer only, T3 style. Choose it if you expect results to be long by default.

Inline delivery has one cost that the pointer design avoids. It puts task output into a user-role message. Item 14 is the mitigation.

## 2. How tasks launched together are batched

**Recommend: one notice per tool call, sent when every task in that call has settled.**

A `create_chats` call is the async form of `create_chats` with `wait: true`, which already returns when all tasks finish. A lead that fans out three reviewers wants one turn with three results. Separate `create_chat` calls give separate notices, so the lead can still choose.

The cost is that the slowest task sets the latency, and a task that never settles holds the notice back. The attention notice, Stop, and `task_status` limit that.

Alternative: T3's rule. Wake on the first finisher, merge later finishers into a wake that is still queued, and stop waking after two. It reacts sooner and wastes turns on "still waiting" replies.

Amendment (approved by Mats, after live probing): a sealed batch also releases *partially*. Once a batch has settled-but-undelivered members and at least one member still working, a progress notice delivers the settled results and lists the still-working members (`needs input` marked) — at most every 15 minutes, only when new results exist. One stuck member no longer holds the batch hostage; the final notice carries only the remainder.

## 3. How a task is told apart from a fork, and where nested tasks hang

**Recommend: one object field on the chat row, `delegation { by, depth }`. Keep `parent_chat_id` pointing at the top-level chat for every task in a tree.**

The field is the smallest thing that lets the engine give tasks and forks different rules. Keeping `parent_chat_id` at the root means the explorer footer lists every task with no UI change, and the upstream invariant "a parent is always a top-level chat" still holds.

Alternative: a true tree in `parent_chat_id`. It is the more honest model and it needs UI work before a grandchild is visible. I would raise it with the maintainers as a follow-up, not ship it first.

## 4. Where the notice state lives, and the one-device limit

**Recommend: a ledger file on the host engine. `notify` works only when the lead and the task are hosted on the engine that receives the call.**

One writer, no merge rules, nothing new in the synced registry beyond the `delegation` field. Cross-harness delegation is unaffected. Only a task on another machine loses `notify`, and it can still be waited on.

Alternative: put the state on the synced row, or send notices through the command ledger, so that tasks on other devices can notify. That needs rules for two hosts that settle tasks of one batch at the same time. I would not take that on before the maintainers have seen the single-device version.

## 5. Whether `notify` is opt-in

**Recommend: opt-in. Default `false`.**

Existing callers that launch with `wait: false` and then call `wait_for_turn` keep working and get no surprise message. The instructions tell agents to prefer `notify`.

Alternative: make a prompt without `wait` imply `notify`. Better behavior from agents that skim, and a behavior change for everyone upstream.

## 6. Limits

**Recommend: depth 2, and 16 running tasks per root.**

Depth 2 covers lead, worker, and one more split. T3 has no limit and upstream has a limit of 1, so a small number is the easier request. Sixteen sits under the sync budget of 28 clients per profile in `docs/sync-capacity-calibration.md`.

Both are constants. Tell me if you already know you need depth 3.

## 7. Whether forks and user side chats can delegate

**Recommend: no, as today.**

Only chats that an agent created through `create_chat` gain the right to create chats. This keeps the change narrow and leaves the decision about user side chats to the maintainers. The downside is real. A user who opens a side chat and asks it to delegate still gets "Side chats cannot create chats."

Alternative: treat forks and user side chats as depth 0, like top-level chats.

## 8. What happens to a notice when the user has stopped the lead

**Recommend: put the notice in the lead's frozen queue and do not start a turn.**

Zeron already freezes a chat's queue after Stop until the user sends something. A notice that restarted a lead the user just stopped would break that rule. In the queue it is visible, and the user can send or delete it.

Alternatives: drop it, as T3 does, or wake the lead regardless.

## 9. Whether the first version includes the attention notice

**Recommend: yes.**

MCP runs start with `auto_approve: false`. A task that hits an approval prompt while its lead has ended its turn would hang with nobody watching. T3 has this gap and lists it as untested. The cost is about 20 lines and one test.

## 10. Whether a `notify` task gets a header on its first prompt

**Recommend: yes, one line.**

The task has to know that its final message is the result and that it must not also send it with `send_message`. Server instructions may not reach the model on every harness. The existing `send_message` header tells the receiver to reply with `send_message`, which is the wrong advice here, so the task needs its own header.

The header shows as plain text in the task's first user bubble. A UI tweak could hide it later.

## 11. Tool names

**Recommend: add `notify` to `create_chat`, `create_chats`, `send_message`, and `send_messages`, and add `task_status` and `task_cancel`.**

This is the smallest change to the tool list and keeps upstream's vocabulary. Agents that already use `create_chats` gain the feature with one flag.

Alternative: a separate `delegate_task` tool, T3 style. It is clearer for the agent and it duplicates most of `create_chat`'s arguments.

## 12. One probe before any code

**Recommend: run scenario S14 from the test plan on the stock app first.**

It measures whether each harness lets a blocking `wait: true` call run for three minutes. If a harness cuts the call off, the instructions should steer agents to `notify` more firmly, and the upstream issue gets a second piece of evidence for Gap 1. It needs no fork and takes a few minutes per harness.

Result, from `probes/2026-10-02-s14-blocking-wait.tsv`. Each lead created a Codex child that ran `sleep 180`, with `wait: true`. One run each for claude-code, devin, and pi. Three runs for codex.

| Lead | Outcome |
|---|---|
| claude-code, devin, pi | The call blocked for 3 to 3.5 minutes and returned the child's reply. No tool timeout at 3 minutes. |
| codex | 3 runs, 2 failures. In runs 1 and 3, Codex's code mode moved the call to the background. The lead read that as an error and ended its turn 40 to 43 seconds after the child started. The child finished later and its reply never reached the lead. In run 2 the call blocked for about 3 minutes 25 seconds and returned the reply. |

What follows from it:

- The Codex failure is intermittent. An agent cannot rely on a blocking wait on Codex and cannot work around the failure, because it does not choose which path Codex takes.
- The design note's Gap 1 now has a second failure, and assumption 1 is partly verified.
- The instruction text and the `wait` descriptions tell agents to use `wait: true` only for work that finishes in seconds, and tell Codex leads to use `notify`.
- Probe S17 in the test plan reruns the Codex case against the fork with `notify: true`.
- Still unknown: durations beyond 3 minutes, what makes Codex route a call through code mode, whether one run each is enough for claude-code, devin, and pi, and leads that were not probed.

## 13. Sandbox cap for tasks

**Decided: a delegated task cannot create a task with a higher sandbox level than its own. Top-level chats are not capped.**

The order is `read-only`, `workspace-write`, `danger-full-access`. The engine enforces the rule in `createChat` next to the depth check and rejects with an error that names both levels. Top-level chats keep today's behavior because the user controls them, which keeps the read-only planner that delegates writing.

Two things to know:

- When a task omits `sandbox`, the MCP default becomes the lower of `workspace-write` and the task's own level. Without that, a read-only task that omits the argument would be rejected, because today's default is `workspace-write`.
- The cap covers creation only. `Mutate setChatConfig` and the `sandbox` value on a `Run` command can still raise a task's level. No MCP tool exposes those paths. An agent with a shell could call them directly, because the engine's IPC port takes connections without a credential. Decided in review: all three stay out of this proposal. The design note states them as a limit of the cap. Open question 6 asks the maintainers about the two paths and about a cap for top-level chats. The open IPC port is a broader engine property, so it is its own open question, number 11, and should become its own upstream issue.

## 14. Task output in a notice is untrusted data

**Decided: every piece of task text in a notice sits inside a marked block that the text cannot close.**

The notice has the user role, and task text may come from pages or files that an attacker wrote. The rules are in the design note under "Task output is untrusted":

- Task text goes inside `<task_result_{nonce}>` blocks, and question text inside `<task_question_{nonce}>` blocks. Only Zeron's lines sit outside.
- The engine picks the nonce so that the tag name occurs nowhere in the quoted text. The task text is not altered.
- A line before the first block says that the blocks are task output and not instructions.
- The task title on Zeron's line is cut and cleaned, because the auto-titler can be influenced by the task.

I chose a tag the text cannot contain over escaping the text. Escaping has to anticipate every variant of the closing tag, and it changes what the lead reads.

One correction to the review note. T3 does not have the same gap. Its wake message carries no task output at all. The output reaches the parent as plain text in the `task_status` tool result, with nothing that marks it as untrusted. That is a related weakness in a less trusted channel. The comparison table says so.

The block is a marker and not a guarantee. A model can still be persuaded by quoted text. Probe S15 in the test plan checks what real leads do.

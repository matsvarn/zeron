"""Remaining live delegated-task scenarios (test-plan.md S2, S4, S6, S8-S13, S16).

Usage (same env as live.py):
  python3 -u probes/live2.py <scenario> [lead_harness] [task_harness] [runs]

S12 kills the forked engine with SIGKILL and restarts it; set ENGINE_CMD to
the command that starts it (run from comet/). Rows go to the same TSV.
"""

import datetime as dt
import os
import signal
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live  # noqa: E402
from live import ASYNC_TAIL, NOTICE_PREFIX, PROJECT, children, create_lead, messages, notices, record, task_prompt, wait_idle, watch_for_notice  # noqa: E402

HEAD = "This is a test of Zeron delegated tasks. Do not read or edit files unless told to.\n\n"


def now():
    return dt.datetime.now(dt.UTC).strftime("%H:%M:%SZ")


def tok(s, run):
    return f"{s}{run}{uuid.uuid4().hex[:4]}"


def create(task_h, title, prompt, notify=True, sandbox="workspace-write", wait=False):
    args = f'project "{PROJECT}", harness "{task_h}", sandbox "{sandbox}", title "{title}", prompt "{prompt}"'
    args += ", notify true" if notify else ""
    args += ", wait true" if wait else ""
    return args


def seq(msgs):
    """Compact role sequence, notices marked N."""
    return "".join("N" if (m["role"] == "user" and m["text"].startswith(NOTICE_PREFIX)) else m["role"][0].upper() for m in msgs)


def row(scenario, lead_h, task_h, run, ok, ns=(), reply=None, notes=""):
    record({"at": now(), "scenario": scenario, "lead": lead_h, "task": task_h, "run": run, "pass": "yes" if ok else "NO",
            "notice_count": len(ns), "lead_reply": (reply["text"][:240] if isinstance(reply, dict) else (reply or "none")), "notes": notes})


def s2(mcp, lead_h, task_h, run):
    """Busy lead: a quick task settles while the lead runs sleep 60."""
    t = tok("S2", run)
    prompt = (HEAD + f"1. Call the Zeron MCP tool create_chat with: {create(task_h, f'live {t} task', task_prompt(t))}.\n"
              "2. Immediately after it returns, run the shell command: sleep 60\n"
              "3. Then reply SLEPT. If at any point you receive a message that starts with "
              f"{NOTICE_PREFIX} reply (in the same or a later message) with exactly: NOTICE: <the PONG line>.")
    lead = create_lead(mcp, lead_h, f"live S2 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    msgs, ns, reply = watch_for_notice(mcp, lead, [t], 300)
    s = seq(msgs)
    all_text = " ".join(m["text"] for m in msgs if m["role"] == "assistant")
    path = "none"
    if ns:
        i = msgs.index(ns[0])
        before = [m for m in msgs[:i] if m["role"] == "assistant"]
        path = "steer (inside the busy turn)" if before and "SLEPT" not in " ".join(m["text"] for m in before) else "next turn"
    ok = len(ns) == 1 and f"PONG-{t}" in ns[0]["text"] and f"PONG-{t}" in all_text
    row("S2", lead_h, task_h, run, ok, ns, reply, f"path={path}; seq={s}; lead={lead}")
    return lead


def s4(mcp, lead_h, task_h, run):
    """Nest, blocking: the task waits on a short grandchild."""
    g = tok("S4G", run)
    inner = (f"Call the Zeron MCP tool create_chat with project {PROJECT}, harness {task_h}, sandbox workspace-write, title live S4 grandchild, "
             f"prompt: Reply with exactly PONG-{g}. Use no other tools. Set wait true. Then reply with exactly CHILD-RESULT: followed by the reply you got.")
    prompt = HEAD + f"Make exactly one call to the Zeron MCP tool create_chat with: {create(task_h, 'live S4 task', inner)}.\n\n" + ASYNC_TAIL
    lead = create_lead(mcp, lead_h, f"live S4 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    msgs, ns, reply = watch_for_notice(mcp, lead, [g], 600)
    ok = len(ns) == 1 and f"PONG-{g}" in ns[0]["text"]
    row("S4", lead_h, task_h, run, ok, ns, reply, f"lead={lead}")
    return lead


def s6(mcp, lead_h, task_h, run):
    """Depth limit: the depth-2 grandchild tries to create a chat."""
    g = (f"Call the Zeron MCP tool create_chat with project {PROJECT}, harness {task_h}, prompt: Reply OK. Set wait true. "
         "Then reply with exactly DEPTH: followed by the full result or error text of that call.")
    inner = (f"Call the Zeron MCP tool create_chat with project {PROJECT}, harness {task_h}, sandbox workspace-write, title live S6 grandchild, "
             f"prompt: {g} Set wait true. Then reply with exactly CHILD-RESULT: followed by the reply you got.")
    prompt = HEAD + f"Make exactly one call to the Zeron MCP tool create_chat with: {create(task_h, 'live S6 task', inner)}.\n\n" + ASYNC_TAIL.replace(
        "<copy the lines that contain PONG, errored, or interrupted>", "<the DEPTH line>")
    lead = create_lead(mcp, lead_h, f"live S6 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    msgs, ns, reply = watch_for_notice(mcp, lead, [], 600)
    ok = len(ns) == 1 and "Delegation depth limit reached" in ns[0]["text"]
    # no depth-3 chat may exist
    t = children(mcp, lead)
    g_chats = [c for x in t for c in children(mcp, x["id"])]
    deep = [c for x in g_chats for c in children(mcp, x["id"])]
    ok = ok and not deep
    row("S6", lead_h, task_h, run, ok, ns, reply, f"depth3_chats={len(deep)}; lead={lead}")
    return lead


def s8(mcp, lead_h, task_h, run):
    """Stop from outside: the probe interrupts the running task."""
    t = tok("S8", run)
    prompt = HEAD + f"Make exactly one call to the Zeron MCP tool create_chat with: {create(task_h, f'live {t} task', task_prompt(t, 120))}.\n\n" + ASYNC_TAIL
    lead = create_lead(mcp, lead_h, f"live S8 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    kid = children(mcp, lead)[0]["id"]
    deadline = time.time() + 120
    while mcp.call("get_chat", chat=kid)["status"] != "working" and time.time() < deadline:
        time.sleep(2)
    time.sleep(15)
    mcp.call("interrupt_chat", chat=kid)
    msgs, ns, reply = watch_for_notice(mcp, lead, [t], 180)
    ok = len(ns) == 1 and "interrupted" in ns[0]["text"] and f"PONG-{t}" not in ns[0]["text"]
    row("S8", lead_h, task_h, run, ok, ns, reply, f"lead={lead}")
    return lead


def s9(mcp, lead_h, task_h, run):
    """Task error: a harness that fails at run time (Cursor is not signed in here)."""
    t = tok("S9", run)
    prompt = HEAD + f"Make exactly one call to the Zeron MCP tool create_chat with: {create(task_h, f'live {t} task', task_prompt(t))}.\n\n" + ASYNC_TAIL
    lead = create_lead(mcp, lead_h, f"live S9 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    msgs, ns, reply = watch_for_notice(mcp, lead, [t], 300)
    ok = len(ns) == 1 and "errored" in ns[0]["text"]
    row("S9", lead_h, task_h, run, ok, ns, reply, f"lead={lead}")
    return lead


def s10(mcp, lead_h, task_h, run):
    """Task needs input: the task asks a question; the lead answers it."""
    t = tok("S10", run)
    task = (f"Before doing anything else, ask the user one question with your question tool (AskUserQuestion): question Which letter?, options A and B. "
            f"After you get the answer, reply with exactly PONG-{t}-<the letter you were given>.")
    prompt = (HEAD + f"Make exactly one call to the Zeron MCP tool create_chat with: {create(task_h, f'live {t} task', task)}. "
              "Then end your turn by replying STATUS: launched. Do not poll.\n"
              f"If you later receive a message that starts with {NOTICE_PREFIX} and says a task needs input, call the Zeron MCP tool respond_to_input "
              "for that chat with the answer A, then end your turn by replying ANSWERED.\n"
              f"If you later receive a message that starts with {NOTICE_PREFIX} with task results, reply with exactly: NOTICE: <the PONG line>.")
    lead = create_lead(mcp, lead_h, f"live S10 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    deadline = time.time() + 600
    ns, msgs = [], []
    while time.time() < deadline:
        msgs = messages(mcp, lead)
        ns = notices(msgs)
        if any(f"PONG-{t}" in n["text"] for n in ns) and mcp.call("get_chat", chat=lead)["status"] != "working":
            break
        time.sleep(5)
    attention = [n for n in ns if "needs input" in n["text"]]
    result = [n for n in ns if f"PONG-{t}" in n["text"]]
    ok = len(attention) == 1 and len(result) == 1 and f"PONG-{t}-A" in result[0]["text"]
    kid = children(mcp, lead)
    row("S10", lead_h, task_h, run, ok, ns, msgs[-1] if msgs else None,
        f"attention={len(attention)}; result={len(result)}; seq={seq(msgs)}; task_status={mcp.call('get_chat', chat=kid[0]['id'])['status'] if kid else 'none'}; lead={lead}")
    return lead


def s11(mcp, lead_h, task_h, run):
    """Stopped lead: the probe stops the lead mid-turn; the notice must not wake it."""
    t = tok("S11", run)
    prompt = (HEAD + f"1. Call the Zeron MCP tool create_chat with: {create(task_h, f'live {t} task', task_prompt(t, 30))}.\n"
              "2. Then run the shell command: sleep 120\n3. Then reply SLEPT. "
              f"If you receive a message that starts with {NOTICE_PREFIX} reply with exactly: NOTICE: <the PONG line>.")
    lead = create_lead(mcp, lead_h, f"live S11 lead {lead_h}->{task_h} #{run}", prompt)
    deadline = time.time() + 120
    while not children(mcp, lead) and time.time() < deadline:
        time.sleep(2)
    time.sleep(8)
    mcp.call("interrupt_chat", chat=lead)
    kid = children(mcp, lead)[0]["id"]
    mcp.call("wait_for_turn", chat=kid, timeout_secs=300)
    time.sleep(60)
    held = messages(mcp, lead)
    woke = notices(held) or mcp.call("get_chat", chat=lead)["status"] == "working"
    # Releasing the frozen queue: a user message must carry the held notice with it.
    mcp.call("send_message", chat=lead, text="Continue. If a Zeron task notice is waiting, handle it as instructed.")
    msgs, ns, reply = watch_for_notice(mcp, lead, [t], 300)
    ok = not woke and len(ns) == 1 and f"PONG-{t}" in ns[0]["text"]
    row("S11", lead_h, task_h, run, ok, ns, reply, f"woke_while_stopped={bool(woke)}; seq_before={seq(held)}; seq_after={seq(msgs)}; lead={lead}")
    return lead


def engine_pid():
    out = subprocess.run(["lsof", "-nP", "-tiTCP:" + live.PORT, "-sTCP:LISTEN"], capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else None


def s12(mcp, lead_h, task_h, run):
    """Restart: SIGKILL the engine while the task sleeps, start it again."""
    t = tok("S12", run)
    prompt = HEAD + f"Make exactly one call to the Zeron MCP tool create_chat with: {create(task_h, f'live {t} task', task_prompt(t, 60))}.\n\n" + ASYNC_TAIL
    lead = create_lead(mcp, lead_h, f"live S12 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    kid = children(mcp, lead)[0]["id"]
    deadline = time.time() + 120
    while mcp.call("get_chat", chat=kid)["status"] != "working" and time.time() < deadline:
        time.sleep(2)
    time.sleep(15)
    pid = engine_pid()
    os.kill(pid, signal.SIGKILL)
    time.sleep(3)
    log = open(os.path.join(live.HERE, "..", ".probe-engine", "engine.log"), "a")
    subprocess.Popen(os.environ["ENGINE_CMD"], shell=True, cwd=os.path.join(live.HERE, "..", "comet"), stdout=log, stderr=log, start_new_session=True)
    for _ in range(30):
        time.sleep(1)
        if engine_pid():
            break
    m2 = live.Mcp()
    msgs, ns, reply = watch_for_notice(m2, lead, [t], 400)
    time.sleep(30)
    ns = notices(messages(m2, lead))
    ok = len(ns) == 1 and (f"PONG-{t}" in ns[0]["text"] or "interrupted" in ns[0]["text"])
    outcome = "result" if ns and f"PONG-{t}" in ns[0]["text"] else ("interrupted" if ns else "none")
    row("S12", lead_h, task_h, run, ok, ns, reply, f"killed_pid={pid}; outcome={outcome}; lead={lead}")
    live.Mcp.__init__(mcp)  # reconnect the caller's client too
    return lead


def s13(mcp, lead_h, task_h, run):
    """Status: task_status while running, then after it ends."""
    t = tok("S13", run)
    prompt = (HEAD + f"1. Call the Zeron MCP tool create_chat with: {create(task_h, f'live {t} task', task_prompt(t, 40))}.\n"
              "2. Call the Zeron MCP tool task_status with no arguments. Note the task's state.\n"
              "3. Run the shell command: sleep 75\n"
              "4. Call task_status with chat set to the task's chatId. Note state and reply.\n"
              "5. Reply with exactly three lines: STATE1: <state from step 2>, STATE2: <state from step 4>, REPLY: <reply from step 4>. "
              f"Ignore any message that starts with {NOTICE_PREFIX} except to finish these steps.")
    lead = create_lead(mcp, lead_h, f"live S13 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    deadline = time.time() + 300
    while time.time() < deadline:
        msgs = messages(mcp, lead)
        text = " ".join(m["text"] for m in msgs if m["role"] == "assistant")
        if "STATE2" in text and mcp.call("get_chat", chat=lead)["status"] != "working":
            break
        time.sleep(5)
    import re
    s1 = re.search(r"STATE1:\s*(\w+)", text)
    s2_ = re.search(r"STATE2:\s*(\w+)", text)
    ok = bool(s1 and s2_) and s1.group(1) == "working" and s2_.group(1) == "completed" and f"PONG-{t}" in text.split("STATE2", 1)[-1]
    row("S13", lead_h, task_h, run, ok, notices(msgs), text[-240:], f"lead={lead}")
    return lead


def s16(mcp, lead_h, task_h, run):
    """Sandbox cap: a read-only task tries to create a workspace-write task, then one with no sandbox."""
    t = tok("S16", run)
    probe_file = os.path.abspath(os.path.join(live.HERE, "..", ".probe-engine", f"sbx-{t}"))
    child2 = (f"Run the shell command: touch {probe_file} . Reply with exactly WROTE if it succeeded or DENIED if it failed.")
    inner = (f"Step A: call the Zeron MCP tool create_chat with project {PROJECT}, harness {task_h}, sandbox workspace-write, prompt: Reply OK. Set wait true. "
             "Note the result or error text. "
             f"Step B: call create_chat with project {PROJECT}, harness {task_h}, no sandbox argument, prompt: {child2} Set wait true. Note the reply. "
             "Then reply with exactly two lines: A: <the full result or error text of step A>, B: <the reply from step B>.")
    prompt = HEAD + f"Make exactly one call to the Zeron MCP tool create_chat with: {create(task_h, 'live S16 task', inner, sandbox='read-only')}.\n\n" + ASYNC_TAIL.replace(
        "<copy the lines that contain PONG, errored, or interrupted>", "<the A and B lines>")
    lead = create_lead(mcp, lead_h, f"live S16 lead {lead_h}->{task_h} #{run}", prompt)
    wait_idle(mcp, lead, 600)
    msgs, ns, reply = watch_for_notice(mcp, lead, [], 600)
    n = ns[0]["text"] if ns else ""
    cap_msg = "read-only" in n and "workspace-write" in n and "higher sandbox level" in n
    wrote = os.path.exists(probe_file)
    ok = len(ns) == 1 and cap_msg and not wrote
    row("S16", lead_h, task_h, run, ok, ns, reply, f"cap_message={cap_msg}; file_written_by_default_child={wrote}; lead={lead}")
    return lead


SCENARIOS = {"S2": s2, "S4": s4, "S6": s6, "S8": s8, "S9": s9, "S10": s10, "S11": s11, "S12": s12, "S13": s13, "S16": s16}


def main():
    scenario, lead_h, task_h = sys.argv[1], sys.argv[2], sys.argv[3]
    runs = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    mcp = live.Mcp()
    leads = []
    try:
        for run in range(1, runs + 1):
            try:
                leads.append(SCENARIOS[scenario](mcp, lead_h, task_h, run))
            except Exception as e:  # record the failure and keep going
                row(scenario, lead_h, task_h, run, False, notes=f"probe exception: {e!r}"[:380])
    finally:
        if os.environ.get("KEEP") != "1":
            live.archive_tree(live.Mcp(), leads)


if __name__ == "__main__":
    main()

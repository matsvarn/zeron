import json
SYNC = """This is a test of cross-harness delegation. Do not read or edit files.

Using the Zeron MCP tools available to you (e.g. create_chat; the name may carry a prefix like mcp__zeron__), create exactly one child chat with these arguments: project "zeron-orchestrator", harness "{child}", sandbox "read-only", title "probe2 {k} child ({child})", prompt "Reply with exactly the text PONG-{tok} and nothing else. Do not use any tools.", wait true, timeout_secs 600.

Then reply with exactly these lines:
TOOLS_VISIBLE: yes|no (were Zeron MCP tools available to you?)
TOOL_NAME_USED: <exact tool name you called>
CHILD_CHAT_ID: <id or none>
CHILD_REPLY: <the child's reply verbatim, or the error text>
NOTES: <anything that went wrong, else none>"""
ASYNC = """This is a test of asynchronous cross-harness delegation. Do not read or edit files.

Using the Zeron MCP tools available to you, create exactly one child chat with these arguments: project "zeron-orchestrator", harness "codex", sandbox "read-only", title "probe2 F child (codex, async)", prompt "Reply with exactly the text PONG-F8W and nothing else. Do not use any tools.", wait false. Do NOT wait for it, do not poll it, do not call wait_for_turn or read_chat.

Then immediately end your turn by replying with exactly these lines:
CHILD_CHAT_ID: <id or none>
STATUS: launched

If you later receive a new message about the child's result, reply to it with: RECEIVED_RESULT: <the child's reply verbatim>"""
leads = [("A","claude-code","codex","A7Q"),("B","codex","claude-code","B3K"),("C","devin","claude-code","C9M"),("E","pi","codex","E5R")]
reqs=[]
for k,lead,child,tok in leads:
    r={"project":"zeron-orchestrator","harness":lead,"title":f"probe2 {k} lead: {lead} -> {child}","prompt":SYNC.format(child=child,k=k,tok=tok)}
    if lead in("claude-code","codex"): r["reasoning"]="low"
    reqs.append(r)
reqs.append({"project":"zeron-orchestrator","harness":"claude-code","reasoning":"low","title":"probe2 F lead: async claude-code -> codex","prompt":ASYNC})
json.dump({"requests":reqs},open("reqs.json","w"))

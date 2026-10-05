import json
P = """This is a test of how long a blocking MCP tool call can run in your harness. Do not read or edit files.

Make exactly ONE call to the Zeron MCP tool create_chat (the name may carry a prefix like mcp__zeron__) with these arguments: project "zeron-orchestrator", harness "codex", reasoning "low", sandbox "workspace-write", title "probe S14 child (lead {lead})", prompt "Run the shell command: sleep 180. After it finishes, reply with exactly SLEPT-{tok}.", wait true, timeout_secs 600.

Do not retry, do not call any other tool, and do not poll. When the call returns or fails, reply with exactly these lines:
CALL_RESULT: ok|error
CHILD_REPLY: <the child's reply text verbatim, or none>
ERROR_TEXT: <the error text verbatim, or none>"""
leads = [("claude-code","S14CC"),("codex","S14CX"),("devin","S14DV"),("pi","S14PI")]
reqs=[]
for lead,tok in leads:
    r={"project":"zeron-orchestrator","harness":lead,"title":f"probe S14 lead: {lead}","prompt":P.format(lead=lead,tok=tok)}
    if lead in ("claude-code","codex"): r["reasoning"]="low"
    reqs.append(r)
json.dump({"requests":reqs},open("/tmp/s14.json","w"))

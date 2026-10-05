import json, os, subprocess, sys
ZERON_BIN = os.environ.get("ZERON_BIN", "/Applications/Zeron.app/Contents/MacOS/zeron")
env = {"PATH": "/usr/bin:/bin"}
if "ZERON_IPC_PORT" in os.environ:
    env["ZERON_IPC_PORT"] = os.environ["ZERON_IPC_PORT"]
p = subprocess.Popen([ZERON_BIN, "mcp"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
def rpc(i, method, params):
    p.stdin.write(json.dumps({"jsonrpc":"2.0","id":i,"method":method,"params":params})+"\n"); p.stdin.flush()
    while True:
        line = p.stdout.readline()
        m = json.loads(line)
        if m.get("id") == i: return m
rpc(1,"initialize",{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"probe","version":"0"}})
p.stdin.write(json.dumps({"jsonrpc":"2.0","method":"notifications/initialized"})+"\n"); p.stdin.flush()
args = json.load(open(sys.argv[1]))
r = rpc(2,"tools/call",{"name":sys.argv[2],"arguments":args})
print(json.dumps(r.get("result",r).get("structuredContent", r), indent=1)[:4000])

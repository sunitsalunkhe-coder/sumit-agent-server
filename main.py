import asyncio
import json
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

app = FastAPI(title="Sumit Agent Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

AGENT_TOKEN = os.getenv("AGENT_TOKEN", "your-secret-token")

security = HTTPBearer(auto_error=False)

pc_connection: WebSocket = None
pending: dict = {}

def verify_token(creds: HTTPAuthorizationCredentials = Depends(security)):
    if not creds or creds.credentials != AGENT_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")
    return creds.credentials

@app.get("/")
async def root():
    return {"status": "Sumit Agent Server running", "pc_online": pc_connection is not None}

@app.websocket("/ws/agent")
async def agent_socket(ws: WebSocket):
    global pc_connection
    token = ws.headers.get("authorization", "").replace("Bearer ", "")
    if token != AGENT_TOKEN:
        await ws.close(code=4003)
        return
    await ws.accept()
    pc_connection = ws
    print("[Server] PC Agent connected.")
    try:
        while True:
            data = await ws.receive_json()
            cmd_id = data.get("id")
            if cmd_id and cmd_id in pending:
                fut = pending[cmd_id]
                if not fut.done():
                    fut.set_result(data.get("result", ""))
    except WebSocketDisconnect:
        pc_connection = None
        print("[Server] PC Agent disconnected.")

@app.post("/execute")
async def execute(body: dict):
    global pc_connection
    if not pc_connection:
        return {"error": "PC agent offline"}
    cmd_id = str(asyncio.get_event_loop().time())
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    pending[cmd_id] = fut
    try:
        await pc_connection.send_json({**body, "id": cmd_id})
        result = await asyncio.wait_for(fut, timeout=30)
        return {"result": result}
    except asyncio.TimeoutError:
        return {"error": "PC agent timed out"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        pending.pop(cmd_id, None)

@app.get("/status")
async def status():
    return {"pc_online": pc_connection is not None}

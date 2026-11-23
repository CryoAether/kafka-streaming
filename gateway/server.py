import asyncio
import os
import time
from collections import deque
from contextlib import asynccontextmanager
import orjson
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
POLL_INTERVAL = 0.1

clients: set[WebSocket] = set()
latest_snapshot = {
    "ts": 0.0,
    "redis_ok": False,
    "stats": {"msg_rate": 0, "total_processed": 0, "dlq_count": 0, "active_drones": 0},
    "drones": [],
}


async def poll_redis():
    global latest_snapshot
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
    rate_window = deque(maxlen=11)

    try:
        while True:
            now = time.time()
            try:
                keys = await r.keys("drone:state:*")
                drones = []
                if keys:
                    vals = await r.mget(keys)
                    for v in vals:
                        if v is not None:
                            d = orjson.loads(v)
                            d["stale"] = (now - float(d.get("ingest_ts", 0))) > 2.5
                            drones.append(d)

                # pin real drone first, then sort by id
                drones.sort(key=lambda x: (0 if x.get("source") == "mavlink" else 1, x.get("drone_id", "")))

                raw_proc, raw_dlq = await r.mget(["telemetry:processed", "telemetry:dlq"])
                total_proc = int(raw_proc) if raw_proc else 0
                dlq_count = int(raw_dlq) if raw_dlq else 0

                rate_window.append((now, total_proc))
                msg_rate = 0
                if len(rate_window) >= 2:
                    dt = rate_window[-1][0] - rate_window[0][0]
                    if dt > 0:
                        msg_rate = max(0, int((rate_window[-1][1] - rate_window[0][1]) / dt))

                latest_snapshot = {
                    "ts": now,
                    "redis_ok": True,
                    "stats": {
                        "msg_rate": msg_rate,
                        "total_processed": total_proc,
                        "dlq_count": dlq_count,
                        "active_drones": sum(1 for d in drones if not d["stale"]),
                    },
                    "drones": drones,
                }
            except Exception:
                latest_snapshot = {
                    "ts": now,
                    "redis_ok": False,
                    "stats": {"msg_rate": 0, "total_processed": 0, "dlq_count": 0, "active_drones": 0},
                    "drones": [],
                }

            if clients:
                payload = orjson.dumps(latest_snapshot).decode("utf-8")
                dead = []
                for ws in list(clients):
                    try:
                        await ws.send_text(payload)
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    clients.discard(ws)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await r.aclose()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.create_task(poll_redis())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"ok": True, "redis_ok": latest_snapshot["redis_ok"], "clients": len(clients)}


@app.websocket("/ws/telemetry")
async def ws_telemetry(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_text(orjson.dumps(latest_snapshot).decode("utf-8"))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
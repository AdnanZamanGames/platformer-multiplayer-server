import os
import json
import asyncio
import secrets
import string
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

app = FastAPI(title="2D Platformer Multiplayer")

rooms = {}
MAX_PLAYERS = 8
MAX_MESSAGE_SIZE = 4096


def create_room_code():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if code not in rooms:
            return code


async def send(ws, data):
    await ws.send_json(data)


async def broadcast(room, data, exclude=None):
    for player_id, ws in list(room["players"].items()):
        if player_id == exclude:
            continue
        try:
            await send(ws, data)
        except Exception:
            pass


@app.get("/")
async def home():
    return {
        "service": "2D Platformer Multiplayer",
        "status": "online",
        "health": "/health",
        "websocket": "/ws"
    }


@app.get("/health")
async def health():
    return {"ok": True, "rooms": len(rooms)}


@app.get("/rooms/{code}")
async def room_status(code: str):
    room = rooms.get(code.upper())
    if not room:
        return JSONResponse(
            {"exists": False},
            status_code=404
        )
    return {
        "exists": True,
        "players": len(room["players"]),
        "max_players": MAX_PLAYERS
    }


@app.websocket("/ws")
async def multiplayer(ws: WebSocket):
    await ws.accept()
    code = None
    player_id = None

    try:
        raw = await asyncio.wait_for(
            ws.receive_text(), timeout=20
        )

        if len(raw) > MAX_MESSAGE_SIZE:
            await ws.close(code=1009)
            return

        try:
            request = json.loads(raw)
        except json.JSONDecodeError:
            await ws.send_json({
                "type": "error",
                "message": "Invalid JSON"
            })
            await ws.close(code=1008)
            return

        action = request.get("action")
        player_id = secrets.token_hex(8)
        name = str(request.get("player_name", "Player"))[:24]

        if action == "create_room":
            code = create_room_code()
            rooms[code] = {
                "players": {},
                "names": {}
            }
        elif action == "join_room":
            code = str(
                request.get("room_code", "")
            ).upper().strip()

            if code not in rooms:
                await ws.send_json({
                    "type": "error",
                    "message": "Room not found"
                })
                await ws.close(code=1008)
                return
        else:
            await ws.send_json({
                "type": "error",
                "message": "Unknown room action"
            })
            await ws.close(code=1008)
            return

        room = rooms[code]

        if len(room["players"]) >= MAX_PLAYERS:
            await ws.send_json({
                "type": "error",
                "message": "Room is full"
            })
            await ws.close(code=1008)
            return

        room["players"][player_id] = ws
        room["names"][player_id] = name

        await send(ws, {
            "type": "room_joined",
            "room_code": code,
            "player_id": player_id,
            "players": [
                {"id": pid, "name": pname}
                for pid, pname in room["names"].items()
            ]
        })

        await broadcast(room, {
            "type": "player_joined",
            "player_id": player_id,
            "player_name": name
        }, exclude=player_id)

        while True:
            raw = await ws.receive_text()

            if len(raw) > MAX_MESSAGE_SIZE:
                await send(ws, {
                    "type": "error",
                    "message": "Message too large"
                })
                continue

            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue

            kind = message.get("type")

            if kind == "ping":
                await send(ws, {
                    "type": "pong",
                    "time": time.time()
                })

            elif kind in ("state", "game_event"):
                await broadcast(room, {
                    "type": kind,
                    "player_id": player_id,
                    "data": message.get("data", {})
                }, exclude=player_id)

            elif kind == "list_players":
                await send(ws, {
                    "type": "players",
                    "players": [
                        {"id": pid, "name": pname}
                        for pid, pname in room["names"].items()
                    ]
                })

            elif kind == "leave":
                break

    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass

    finally:
        if code in rooms and player_id:
            room = rooms[code]
            room["players"].pop(player_id, None)
            room["names"].pop(player_id, None)

            await broadcast(room, {
                "type": "player_left",
                "player_id": player_id
            })

            if not room["players"]:
                rooms.pop(code, None)

        try:
            await ws.close()
        except Exception:
            pass

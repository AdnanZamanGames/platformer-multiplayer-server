import asyncio
import json
import secrets
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

app = FastAPI(title="2D Platformer Multiplayer")
rooms = {}
MAX_PLAYERS = 8
MAX_MESSAGE_SIZE = 1_000_000
MAX_PLAYER_NAME = 24


def create_room_code():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if code not in rooms:
            return code


async def send(ws, data):
    await ws.send_json(data)


async def broadcast(room, data, exclude=None):
    """Send a message to all connected players except an optional sender."""
    disconnected = []
    for player_id, ws in list(room["players"].items()):
        if player_id == exclude:
            continue
        try:
            await send(ws, data)
        except Exception:
            disconnected.append(player_id)
    for player_id in disconnected:
        room["players"].pop(player_id, None)
        room["names"].pop(player_id, None)
        room["states"].pop(player_id, None)


def player_list(room):
    return [{"id": pid, "name": name, "state": room["states"].get(pid)}
            for pid, name in room["names"].items()]


@app.get("/")
async def home():
    return {"service": "2D Platformer Multiplayer", "status": "online",
            "health": "/health", "websocket": "/ws"}


@app.get("/health")
async def health():
    return {"ok": True, "rooms": len(rooms), "max_players_per_room": MAX_PLAYERS}


@app.get("/rooms/{code}")
async def room_status(code: str):
    code = code.upper().strip()
    room = rooms.get(code)
    if not room:
        return JSONResponse({"exists": False}, status_code=404)
    return {"exists": True, "room_code": code, "players": len(room["players"]),
            "max_players": MAX_PLAYERS,
            "selected_level": (room.get("selected_level") or {}).get("id")}


@app.websocket("/ws")
async def multiplayer(ws: WebSocket):
    await ws.accept()
    code = player_id = room = None
    try:
        raw = await asyncio.wait_for(ws.receive_text(), timeout=20)
        if len(raw) > MAX_MESSAGE_SIZE:
            await ws.close(code=1009)
            return
        try:
            request = json.loads(raw)
        except json.JSONDecodeError:
            await send(ws, {"type": "error", "message": "Invalid JSON"})
            await ws.close(code=1008)
            return
        if not isinstance(request, dict):
            await send(ws, {"type": "error", "message": "Invalid request"})
            await ws.close(code=1008)
            return

        action = request.get("action")
        name = str(request.get("player_name", "Player")).strip()[:MAX_PLAYER_NAME] or "Player"
        if action == "create_room":
            code = create_room_code()
            rooms[code] = {"players": {}, "names": {}, "states": {},
                           "owner_id": None, "selected_level": None}
        elif action == "join_room":
            code = str(request.get("room_code", "")).upper().strip()
            if code not in rooms:
                await send(ws, {"type": "error", "message": "Room not found"})
                await ws.close(code=1008)
                return
        else:
            await send(ws, {"type": "error", "message": "Unknown room action"})
            await ws.close(code=1008)
            return

        room = rooms[code]
        room.setdefault("states", {})
        room.setdefault("names", {})
        room.setdefault("players", {})
        room.setdefault("owner_id", None)
        room.setdefault("selected_level", None)
        room.setdefault("race_result", None)
        if len(room["players"]) >= MAX_PLAYERS:
            await send(ws, {"type": "error", "message": "Room is full"})
            await ws.close(code=1008)
            return

        player_id = secrets.token_hex(8)
        if action == "create_room":
            room["owner_id"] = player_id
        existing_players = player_list(room)
        room["players"][player_id] = ws
        room["names"][player_id] = name
        room["states"][player_id] = None
        await send(ws, {
            "type": "room_joined", "room_code": code, "player_id": player_id,
            "is_owner": player_id == room.get("owner_id"),
            "selected_level": room.get("selected_level"),
            "race_result": room.get("race_result"),
            "players": existing_players + [{"id": player_id, "name": name, "state": None}]
        })
        await broadcast(room, {"type": "player_joined", "player_id": player_id,
                               "player_name": name}, exclude=player_id)

        while True:
            raw = await ws.receive_text()
            if len(raw) > MAX_MESSAGE_SIZE:
                await send(ws, {"type": "error", "message": "Message too large"})
                continue
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(message, dict):
                continue
            kind = message.get("type")
            if kind == "ping":
                await send(ws, {"type": "pong", "time": time.time()})
            elif kind == "state":
                state = message.get("data", {})
                if not isinstance(state, dict):
                    continue
                room["states"][player_id] = state
                await broadcast(room, {"type": "state", "player_id": player_id,
                                       "data": state}, exclude=player_id)
            elif kind == "select_level":
                if player_id != room.get("owner_id"):
                    await send(ws, {"type": "error", "message": "Only the room owner can select the race level"})
                    continue
                level = message.get("level")
                if not isinstance(level, dict):
                    await send(ws, {"type": "error", "message": "Invalid level selection"})
                    continue
                level_data = level.get("level_data")
                if not isinstance(level_data, dict) or not (
                    isinstance(level_data.get("objects"), (list, dict)) or
                    isinstance(level_data.get("stages"), list)
                ):
                    await send(ws, {"type": "error", "message": "Selected level has no valid level data"})
                    continue
                selected = {
                    "id": str(level.get("id", ""))[:100],
                    "display_id": level.get("display_id"),
                    "name": str(level.get("name") or "Community Level")[:100],
                    "creator_username": str(level.get("creator_username") or "Guest")[:80],
                    "difficulty": str(level.get("difficulty") or "Easy")[:30],
                    "level_data": level_data,
                }
                room["selected_level"] = selected
                room["race_result"] = None
                await broadcast(room, {"type": "room_level_selected", "player_id": player_id,
                                       "level": selected})
                await broadcast(room, {"type": "race_reset", "level_id": selected.get("id")})
            elif kind == "game_event":
                # Race completion is the only gameplay event shared by this race-only build.
                # Switches, keys, doors, collectibles and trigger effects remain personal.
                event_data = message.get("data", {})
                if not isinstance(event_data, dict) or event_data.get("event") != "race_finish":
                    continue
                if room.get("race_result") is not None:
                    await send(ws, {"type": "race_result", **room["race_result"]})
                    continue
                winner_name = str(event_data.get("player_name") or room["names"].get(player_id) or "Player")[:24]
                result = {
                    "winner_id": player_id,
                    "winner_name": winner_name,
                    "stage_index": int(event_data.get("stage_index", 0) or 0),
                    "finished_at": time.time(),
                }
                room["race_result"] = result
                # Include the sender so every client follows the same authoritative result.
                await broadcast(room, {"type": "race_result", **result})
            elif kind == "list_players":
                await send(ws, {"type": "players", "players": player_list(room)})
            elif kind == "leave":
                break
            else:
                await send(ws, {"type": "error", "message": "Unknown message type"})

    except asyncio.TimeoutError:
        try:
            await send(ws, {"type": "error", "message": "Room join timed out"})
        except Exception:
            pass
    except WebSocketDisconnect:
        pass
    finally:
        if code in rooms and player_id:
            room = rooms[code]
            room["players"].pop(player_id, None)
            room["names"].pop(player_id, None)
            room["states"].pop(player_id, None)
            await broadcast(room, {"type": "player_left", "player_id": player_id})
            if not room["players"]:
                rooms.pop(code, None)
        try:
            await ws.close()
        except Exception:
            pass

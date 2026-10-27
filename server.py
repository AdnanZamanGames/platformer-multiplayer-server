
import asyncio
import json
import secrets
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

app = FastAPI(title="2D Platformer Multiplayer")

rooms = {}
MAX_PLAYERS = 8
MAX_MESSAGE_SIZE = 4096
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
    """Send a message to all connected players except the sender."""
    disconnected = []

    for player_id, ws in list(room["players"].items()):
        if player_id == exclude:
            continue

        try:
            await send(ws, data)
        except Exception:
            disconnected.append(player_id)

    # Remove broken connections.
    for player_id in disconnected:
        room["players"].pop(player_id, None)
        room["names"].pop(player_id, None)
        room["states"].pop(player_id, None)


def player_list(room):
    """Return player details, including each player's latest known state."""
    return [
        {
            "id": player_id,
            "name": name,
            "state": room["states"].get(player_id)
        }
        for player_id, name in room["names"].items()
    ]


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
    return {
        "ok": True,
        "rooms": len(rooms),
        "max_players_per_room": MAX_PLAYERS
    }


@app.get("/rooms/{code}")
async def room_status(code: str):
    room = rooms.get(code.upper().strip())

    if not room:
        return JSONResponse(
            {"exists": False},
            status_code=404
        )

    return {
        "exists": True,
        "room_code": code.upper().strip(),
        "players": len(room["players"]),
        "max_players": MAX_PLAYERS
    }


@app.websocket("/ws")
async def multiplayer(ws: WebSocket):
    await ws.accept()

    code = None
    player_id = None
    room = None

    try:
        # The first message must create or join a room.
        raw = await asyncio.wait_for(
            ws.receive_text(),
            timeout=20
        )

        if len(raw) > MAX_MESSAGE_SIZE:
            await ws.close(code=1009)
            return

        try:
            request = json.loads(raw)
        except json.JSONDecodeError:
            await send(ws, {
                "type": "error",
                "message": "Invalid JSON"
            })
            await ws.close(code=1008)
            return

        if not isinstance(request, dict):
            await send(ws, {
                "type": "error",
                "message": "Invalid request"
            })
            await ws.close(code=1008)
            return

        action = request.get("action")

        name = str(request.get("player_name", "Player"))
        name = name.strip()[:MAX_PLAYER_NAME] or "Player"

        if action == "create_room":
            code = create_room_code()

            rooms[code] = {
                "players": {},
                "names": {},
                "states": {}
            }

        elif action == "join_room":
            code = str(
                request.get("room_code", "")
            ).upper().strip()

            if code not in rooms:
                await send(ws, {
                    "type": "error",
                    "message": "Room not found"
                })
                await ws.close(code=1008)
                return

        else:
            await send(ws, {
                "type": "error",
                "message": "Unknown room action"
            })
            await ws.close(code=1008)
            return

        room = rooms[code]

        # Backwards compatibility with existing rooms.
        room.setdefault("states", {})
        room.setdefault("names", {})
        room.setdefault("players", {})

        if len(room["players"]) >= MAX_PLAYERS:
            await send(ws, {
                "type": "error",
                "message": "Room is full"
            })
            await ws.close(code=1008)
            return

        player_id = secrets.token_hex(8)

        # Save the existing players before adding this player.
        existing_players = player_list(room)

        room["players"][player_id] = ws
        room["names"][player_id] = name
        room["states"][player_id] = None

        # Send the newcomer the room details and existing players.
        await send(ws, {
            "type": "room_joined",
            "room_code": code,
            "player_id": player_id,
            "players": existing_players + [{
                "id": player_id,
                "name": name,
                "state": None
            }]
        })

        # Tell existing players that someone joined.
        await broadcast(room, {
            "type": "player_joined",
            "player_id": player_id,
            "player_name": name
        }, exclude=player_id)

        # Receive movement and other multiplayer messages.
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

            if not isinstance(message, dict):
                continue

            kind = message.get("type")

            if kind == "ping":
                await send(ws, {
                    "type": "pong",
                    "time": time.time()
                })

            elif kind == "state":
                # Expected data might contain x, y, facing, animation, etc.
                state = message.get("data", {})

                if not isinstance(state, dict):
                    continue

                # Keep the latest state for players joining later.
                room["states"][player_id] = state

                # Forward movement/state to all other players.
                await broadcast(room, {
                    "type": "state",
                    "player_id": player_id,
                    "data": state
                }, exclude=player_id)

            elif kind == "game_event":
                # Forward gameplay events without changing their format.
                await broadcast(room, {
                    "type": "game_event",
                    "player_id": player_id,
                    "data": message.get("data", {})
                }, exclude=player_id)

            elif kind == "list_players":
                await send(ws, {
                    "type": "players",
                    "players": player_list(room)
                })

            elif kind == "leave":
                break

            else:
                await send(ws, {
                    "type": "error",
                    "message": "Unknown message type"
                })

    except asyncio.TimeoutError:
        try:
            await send(ws, {
                "type": "error",
                "message": "Room join timed out"
            })
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

            await broadcast(room, {
                "type": "player_left",
                "player_id": player_id
            })

            # Delete the room when its last player leaves.
            if not room["players"]:
                rooms.pop(code, None)

        try:
            await ws.close()
        except Exception:
            pass

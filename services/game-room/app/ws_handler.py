import asyncio
import json
import logging
import uuid
from fastapi import WebSocket, WebSocketDisconnect
from prometheus_client import Counter

logger = logging.getLogger("game-room")

player_connections = Counter("gameroom_player_connections_total", "Total player connections")


async def validate_token(token, auth_service_url):
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{auth_service_url}/auth/validate",
                                 headers={"Authorization": f"Bearer {token}"}, timeout=5)
            if r.status_code == 200:
                return r.json()["player_id"]
    except Exception:
        pass
    return None


async def websocket_endpoint(websocket: WebSocket, election, game_loop, connected_players, connected_spectators, auth_service_url):
    await websocket.accept()
    try:
        data = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        token = data.get("token")
        mode = data.get("mode", "player")
        if not token:
            await websocket.close(code=4001, reason="Missing token")
            return
        player_id = await validate_token(token, auth_service_url)
        if not player_id and mode != "spectator":
            await websocket.close(code=4001, reason="Invalid token")
            return
    except asyncio.TimeoutError:
        await websocket.close(code=4001, reason="Auth timeout")
        return
    except Exception:
        await websocket.close(code=4001, reason="Auth failed")
        return
    if not election or not election.is_leader:
        await websocket.close(code=4000, reason="Not the leader")
        return
    if mode == "spectator":
        sid = f"spectator-{uuid.uuid4()}"
        connected_spectators[sid] = websocket
        logger.info(f"Spectator {sid} connected")
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            connected_spectators.pop(sid, None)
        return
    connected_players[player_id] = websocket
    player_connections.inc()
    if game_loop:
        await game_loop.add_player(player_id)
    logger.info(f"Player {player_id} connected")
    try:
        while True:
            raw = await websocket.receive_text()
            input_data = json.loads(raw)
            if game_loop:
                await game_loop.enqueue_input(player_id, input_data)
    except WebSocketDisconnect:
        connected_players.pop(player_id, None)
        if game_loop:
            await game_loop.remove_player(player_id)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        connected_players.pop(player_id, None)

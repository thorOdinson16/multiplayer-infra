"""WebSocket helper functions for integration tests."""
import asyncio
import json
import time
import sys
try:
    import websockets
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets


async def play_move(ws_url, token, dx=0, dy=0, duration=3, tick_interval=0.1):
    """Connect as player, send movement inputs for `duration` seconds, return last received state."""
    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({"token": token, "mode": "player"}))
        state = None
        deadline = time.time() + duration
        while time.time() < deadline:
            await ws.send(json.dumps({"dx": dx, "dy": dy, "speed": 5}))
            try:
                state = await asyncio.wait_for(ws.recv(), timeout=1.0)
                state = json.loads(state)
            except (asyncio.TimeoutError, json.JSONDecodeError):
                pass
            await asyncio.sleep(tick_interval)
        return state


def _player_id(token):
    """Read the `sub` claim from a JWT without verifying it (the server does that)."""
    import base64
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))["sub"]


async def _state_with_player(ws, player_id, accept, timeout=5.0):
    """Read broadcasts until one contains `player_id` and `accept(player_state)` holds.

    Broadcasts sent before the server registers the player (or before an input is
    applied) do not contain the player yet, so the first message cannot be trusted.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.time())))
        except (asyncio.TimeoutError, json.JSONDecodeError):
            continue
        player = (msg.get("players") or {}).get(player_id)
        if player and accept(player):
            return msg
    return None


async def connect_and_disconnect(ws_url, token, hold_seconds=2):
    """Connect, move, disconnect, wait, reconnect without sending input.

    Returns the last state seen on each connection that contains this player, so a
    caller can check the position survived the gap (the hold slot).
    """
    pid = _player_id(token)
    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({"token": token, "mode": "player"}))
        await ws.send(json.dumps({"dx": 10, "dy": 4, "speed": 5}))
        first_state = await _state_with_player(ws, pid, lambda p: p["connected"] and p["y"] > 0)
        # Let any in-flight input settle, then take the final position.
        await asyncio.sleep(0.3)
        latest = await _state_with_player(ws, pid, lambda p: p["connected"], timeout=1.0)
        first_state = latest or first_state
    # Wait for hold seconds (within the 30s hold window)
    await asyncio.sleep(hold_seconds)
    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({"token": token, "mode": "player"}))
        second_state = await _state_with_player(ws, pid, lambda p: p["connected"])
    return first_state, second_state


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "play_move":
        result = asyncio.run(play_move(sys.argv[2], sys.argv[3]))
        print(json.dumps(result))
    elif cmd == "connect_and_disconnect":
        result = asyncio.run(connect_and_disconnect(sys.argv[2], sys.argv[3]))
        print(json.dumps(result))
    else:
        print("Usage: test_ws_helpers.py <play_move|connect_and_disconnect> <ws_url> <token>")

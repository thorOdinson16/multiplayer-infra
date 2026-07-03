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


async def connect_and_disconnect(ws_url, token, hold_seconds=2):
    """Connect, send one input, disconnect, wait, reconnect, return both states."""
    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({"token": token, "mode": "player"}))
        await ws.send(json.dumps({"dx": 10, "dy": 0, "speed": 5}))
        first_state = None
        try:
            first_state = await asyncio.wait_for(ws.recv(), timeout=2.0)
            first_state = json.loads(first_state)
        except (asyncio.TimeoutError, json.JSONDecodeError):
            pass
    # Wait for hold seconds (within the 30s hold window)
    await asyncio.sleep(hold_seconds)
    # Reconnect
    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({"token": token, "mode": "player"}))
        await ws.send(json.dumps({"dx": 0, "dy": 5, "speed": 5}))
        second_state = None
        try:
            second_state = await asyncio.wait_for(ws.recv(), timeout=2.0)
            second_state = json.loads(second_state)
        except (asyncio.TimeoutError, json.JSONDecodeError):
            pass
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

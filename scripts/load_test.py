#!/usr/bin/env python3
"""WebSocket load test for the game room.

Registers N players through the gateway, connects them all to one match, has
each send movement inputs, and reports what the server delivers:
  - state broadcast interval per client (target: 1/tick_rate = 50 ms at 20 Hz)
  - input -> state round-trip time (first broadcast reflecting a changed position)
  - connection failures

usage: scripts/load_test.py [players] [seconds]     (defaults 20, 15)
"""
import asyncio
import json
import statistics
import sys
import time
import urllib.error
import urllib.request

import websockets

BASE = "http://localhost:8080"
WS = "ws://localhost:8080/ws"


def register(username):
    req = urllib.request.Request(
        f"{BASE}/auth/register",
        data=json.dumps({"username": username, "password": "pass123"}).encode(),
        headers={"Content-Type": "application/json"},
    )
    return json.load(urllib.request.urlopen(req, timeout=10))["access_token"]


def player_id(token):
    import base64
    p = token.split(".")[1]
    p += "=" * (-len(p) % 4)
    return json.loads(base64.urlsafe_b64decode(p))["sub"]


async def client(token, duration, intervals, rtts, errors):
    pid = player_id(token)
    try:
        async with websockets.connect(WS, max_size=None) as ws:
            await ws.send(json.dumps({"token": token, "mode": "player"}))
            last_arrival = None
            pending = {}            # expected x -> send time
            x_sent = 0.0
            end = time.time() + duration

            async def sender():
                nonlocal x_sent
                step = 1
                while time.time() < end:
                    x_sent += step * 5
                    pending[x_sent] = time.perf_counter()
                    await ws.send(json.dumps({"dx": step, "dy": 0, "speed": 5}))
                    step = -step if abs(x_sent) > 400 else step
                    await asyncio.sleep(0.25)

            send_task = asyncio.create_task(sender())
            while time.time() < end:
                try:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                except asyncio.TimeoutError:
                    continue
                arrival = time.perf_counter()
                if msg.get("type") != "state":
                    continue
                if last_arrival is not None:
                    intervals.append((arrival - last_arrival) * 1000)
                last_arrival = arrival
                me = (msg.get("players") or {}).get(pid)
                if me and me["x"] in pending:
                    rtts.append((arrival - pending.pop(me["x"])) * 1000)
            send_task.cancel()
    except Exception as e:
        errors.append(f"{type(e).__name__}: {e}")


def pct(values, p):
    v = sorted(values)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


async def main(n, seconds):
    stamp = int(time.time())
    print(f"registering {n} players...")
    tokens = []
    for i in range(n):
        tokens.append(register(f"load{stamp}_{i}"))
        await asyncio.sleep(0.12)   # stay under the gateway's auth rate limit
    print(f"running {n} clients for {seconds}s...")
    intervals, rtts, errors = [], [], []
    tasks = []
    for t in tokens:
        tasks.append(asyncio.create_task(client(t, seconds, intervals, rtts, errors)))
        await asyncio.sleep(0.05)   # stagger joins; a burst trips the gateway's /ws rate limit (503)
    await asyncio.gather(*tasks)

    print()
    print(f"players={n} duration={seconds}s errors={len(errors)}")
    for e in sorted(set(errors))[:3]:
        print("  ", e)
    if intervals:
        print(f"broadcast interval ms  p50={statistics.median(intervals):.1f}  p95={pct(intervals, 95):.1f}  "
              f"p99={pct(intervals, 99):.1f}  max={max(intervals):.1f}  (target 50)  n={len(intervals)}")
    if rtts:
        print(f"input->state RTT ms    p50={statistics.median(rtts):.1f}  p95={pct(rtts, 95):.1f}  "
              f"p99={pct(rtts, 99):.1f}  max={max(rtts):.1f}  n={len(rtts)}")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 20,
                     int(sys.argv[2]) if len(sys.argv) > 2 else 15))

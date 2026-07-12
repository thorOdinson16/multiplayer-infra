import asyncio
import json
import logging
import time
from confluent_kafka import Consumer, KafkaException
from prometheus_client import Gauge, Counter, Histogram

from .config import settings
from .models import GameState, PlayerState
from .spectator_buffer import SpectatorRingBuffer

logger = logging.getLogger("game-room")

active_matches = Gauge("gameroom_active_matches", "Number of active matches", ["match_id"])
tick_latency = Histogram("gameroom_tick_latency_seconds", "Tick processing latency", buckets=[0.01, 0.025, 0.05, 0.1, 0.25])
state_broadcasts = Counter("gameroom_state_broadcasts_total", "Total state broadcasts")


class GameLoop:
    def __init__(self, match_id, r, kafka_producer, connected_players, connected_spectators):
        self.match_id = match_id
        self.state = GameState(match_id=match_id)
        self.redis = r
        self.kafka_producer = kafka_producer
        self.tick_rate = settings.tick_rate
        self.running = False
        self.input_queue = asyncio.Queue()
        self.last_committed_kafka_offset = -1
        self.spectator_buffer = SpectatorRingBuffer(max_size=self.tick_rate * 30, delay_ticks=self.tick_rate * 10)
        self.match_duration_ticks = self.tick_rate * 300
        self.match_ended = False
        self._connected_players = connected_players
        self._connected_spectators = connected_spectators

    async def load_state(self):
        redis_state_key = f"match:{self.match_id}:state"
        redis_offset_key = f"match:{self.match_id}:last_offset"
        state_data = await self.redis.get(redis_state_key)
        offset_data = await self.redis.get(redis_offset_key)
        if state_data and offset_data:
            self.state = GameState.from_dict(json.loads(state_data))
            self.last_committed_kafka_offset = int(offset_data)
            logger.info(f"Loaded state at tick {self.state.tick}, offset {self.last_committed_kafka_offset}")
        else:
            logger.info("No Redis state -- cold-start replay from Kafka")
            await self._cold_start_replay()

    async def _try_load_checkpoint(self):
        try:
            from couchbase.cluster import Cluster
            from couchbase.auth import PasswordAuthenticator
            from couchbase.options import ClusterOptions
            auth = PasswordAuthenticator(settings.couchbase_username, settings.couchbase_password)
            cluster = Cluster(f"couchbase://{settings.couchbase_host}", ClusterOptions(auth))
            bucket = cluster.bucket(settings.couchbase_matches_bucket)
            coll = bucket.default_collection()
            query = (
                f"SELECT tick, events FROM {settings.couchbase_matches_bucket} "
                f"WHERE type = 'replay_checkpoint' AND matchId = $match_id "
                f"ORDER BY tick DESC LIMIT 1"
            )
            result = cluster.query(query, match_id=self.match_id)
            for row in result:
                for event in row["events"]:
                    self._apply_event(event)
                tick = row["tick"]
                cluster.close()
                logger.info(f"Restored state from Couchbase checkpoint at tick {tick}")
                return tick
            cluster.close()
        except Exception as e:
            logger.warning(f"Checkpoint load failed (will replay from Kafka): {e}")
        return None

    async def _cold_start_replay(self):
        checkpoint_tick = await self._try_load_checkpoint()
        consumer_conf = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'group.id': f'game-room-coldstart-{self.match_id}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([settings.kafka_topic_events])
        try:
            last_offset = -1
            target_offset = -1
            topic_metadata = consumer.list_topics(settings.kafka_topic_events, timeout=5)
            if topic_metadata.topics.get(settings.kafka_topic_events):
                partitions = topic_metadata.topics[settings.kafka_topic_events].partitions
                if partitions:
                    partition = list(partitions.values())[0]
                    low, high = consumer.get_watermark_offsets(partition)
                    target_offset = high - 1 if high > 0 else -1
                    logger.info(f"Replay target: low={low}, high={high}, target_offset={target_offset}")
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    break
                if msg.error():
                    raise KafkaException(msg.error())
                event = json.loads(msg.value().decode())
                event_tick = event.get("tick", 0)
                if checkpoint_tick is None or event_tick > checkpoint_tick:
                    self._apply_event(event)
                last_offset = msg.offset()
                if target_offset >= 0 and last_offset >= target_offset:
                    logger.info(f"Reached target offset {target_offset}")
                    break
            if last_offset >= 0:
                self.last_committed_kafka_offset = last_offset
                logger.info(f"Cold-start replay complete at offset {last_offset}, tick {self.state.tick}")
        finally:
            consumer.close()

    def _apply_event(self, event):
        if "players" in event:
            for pid, pdata in event["players"].items():
                if pid not in self.state.players:
                    self.state.players[pid] = PlayerState(player_id=pid)
                self.state.players[pid].x = pdata["x"]
                self.state.players[pid].y = pdata["y"]
                self.state.players[pid].health = pdata.get("health", 100)
                self.state.players[pid].score = pdata.get("score", 0)
                self.state.players[pid].connected = pdata.get("connected", True)
            self.state.tick = max(self.state.tick, event.get("tick", 0))

    async def enqueue_input(self, player_id, input_data):
        await self.input_queue.put((player_id, input_data))

    async def add_player(self, player_id):
        hold_key = f"hold:{self.match_id}:{player_id}"
        hold_data = await self.redis.get(hold_key)
        if hold_data:
            try:
                saved = json.loads(hold_data)
                self.state.players[player_id] = PlayerState(
                    player_id=player_id, x=saved.get("x", 0), y=saved.get("y", 0),
                    health=saved.get("health", 100), score=saved.get("score", 0),
                    connected=True,
                )
                await self.redis.delete(hold_key)
                logger.info(f"Player {player_id} restored from hold slot")
                return
            except Exception:
                pass
        if player_id not in self.state.players:
            self.state.players[player_id] = PlayerState(player_id=player_id)
        self.state.players[player_id].connected = True

    async def remove_player(self, player_id):
        if player_id in self.state.players:
            self.state.players[player_id].connected = False
            hold_key = f"hold:{self.match_id}:{player_id}"
            hold_value = json.dumps({
                "x": self.state.players[player_id].x,
                "y": self.state.players[player_id].y,
                "health": self.state.players[player_id].health,
                "score": self.state.players[player_id].score,
            })
            await self.redis.setex(hold_key, settings.player_slot_hold_seconds, hold_value)
            logger.info(f"Player {player_id} hold slot reserved for {settings.player_slot_hold_seconds}s")

    async def broadcast_to_players(self, state_snapshot):
        dead = []
        for pid, ws in self._connected_players.items():
            try:
                await ws.send_json(state_snapshot)
            except Exception:
                dead.append(pid)
        for pid in dead:
            self._connected_players.pop(pid, None)

    async def broadcast_to_spectators(self, state_snapshot):
        delayed = self.spectator_buffer.get_delayed_state(self.state.tick) or state_snapshot
        dead = []
        for sid, ws in self._connected_spectators.items():
            try:
                await ws.send_json(delayed)
            except Exception:
                dead.append(sid)
        for sid in dead:
            self._connected_spectators.pop(sid, None)

    async def run(self):
        self.running = True
        tick_delay = 1.0 / self.tick_rate
        active_matches.labels(match_id=self.match_id).set(1)
        while self.running and not self.match_ended:
            start = asyncio.get_event_loop().time()
            await self._process_tick()
            elapsed = asyncio.get_event_loop().time() - start
            tick_latency.observe(elapsed)
            await asyncio.sleep(max(0, tick_delay - elapsed))

    async def _process_tick(self):
        next_players = {}
        for pid, player in self.state.players.items():
            next_players[pid] = PlayerState(
                player_id=pid, x=player.x, y=player.y,
                health=player.health, score=player.score,
                connected=player.connected,
            )

        while not self.input_queue.empty():
            player_id, input_data = await self.input_queue.get()
            self._apply_input_to(player_id, input_data, next_players)

        for pid, player in next_players.items():
            if player.connected and player.health <= 0:
                player.health = 100
                player.x = 0
                player.y = 0

        next_tick = self.state.tick + 1

        state_snapshot = {
            "match_id": self.match_id,
            "tick": next_tick,
            "type": "state",
            "players": {
                pid: {"x": p.x, "y": p.y, "health": p.health, "score": p.score, "connected": p.connected}
                for pid, p in next_players.items()
            },
        }
        event = {"match_id": self.match_id, "tick": next_tick, "players": state_snapshot["players"]}

        kafka_ok = await self._publish_to_kafka(event)
        if not kafka_ok:
            logger.error("Kafka publish failed, state NOT advanced")
            return

        self.state.tick = next_tick
        for pid, p in next_players.items():
            if pid in self.state.players:
                self.state.players[pid].x = p.x
                self.state.players[pid].y = p.y
                self.state.players[pid].health = p.health
                self.state.players[pid].score = p.score
                self.state.players[pid].connected = p.connected
            else:
                self.state.players[pid] = p

        self._publish_telemetry(state_snapshot)
        self.spectator_buffer.append(next_tick, state_snapshot)
        await self._update_redis()
        asyncio.create_task(self.broadcast_to_players(state_snapshot))
        asyncio.create_task(self.broadcast_to_spectators(state_snapshot))
        state_broadcasts.inc()
        if next_tick >= self.match_duration_ticks:
            await self.end_match()

    def _apply_input_to(self, player_id, input_data, players_dict):
        if player_id not in players_dict:
            players_dict[player_id] = PlayerState(player_id=player_id)
        player = players_dict[player_id]
        dx = input_data.get("dx", 0)
        dy = input_data.get("dy", 0)
        speed = input_data.get("speed", 5)
        player.x += dx * speed
        player.y += dy * speed
        player.x = max(-500, min(500, player.x))
        player.y = max(-500, min(500, player.y))
        if input_data.get("shoot"):
            nearest = self._find_nearest_enemy_in(player_id, players_dict)
            if nearest:
                nearest.health -= 10
                player.score += 10

    def _find_nearest_enemy_in(self, player_id, players_dict):
        player = players_dict.get(player_id)
        if not player:
            return None
        nearest = None
        nearest_dist = float("inf")
        for pid, p in players_dict.items():
            if pid == player_id or not p.connected:
                continue
            dist = ((p.x - player.x) ** 2 + (p.y - player.y) ** 2) ** 0.5
            if dist < nearest_dist:
                nearest_dist = dist
                nearest = p
        return nearest if nearest_dist < 200 else None

    def _apply_input(self, player_id, input_data):
        if player_id not in self.state.players:
            self.state.players[player_id] = PlayerState(player_id=player_id)
        player = self.state.players[player_id]
        dx = input_data.get("dx", 0)
        dy = input_data.get("dy", 0)
        speed = input_data.get("speed", 5)
        player.x += dx * speed
        player.y += dy * speed
        player.x = max(-500, min(500, player.x))
        player.y = max(-500, min(500, player.y))
        if input_data.get("shoot"):
            nearest = self._find_nearest_enemy(player_id)
            if nearest:
                nearest.health -= 10
                player.score += 10

    def _find_nearest_enemy(self, player_id):
        player = self.state.players.get(player_id)
        if not player:
            return None
        nearest = None
        nearest_dist = float("inf")
        for pid, p in self.state.players.items():
            if pid == player_id or not p.connected:
                continue
            dist = ((p.x - player.x) ** 2 + (p.y - player.y) ** 2) ** 0.5
            if dist < nearest_dist:
                nearest_dist = dist
                nearest = p
        return nearest if nearest_dist < 200 else None

    async def _publish_to_kafka(self, event):
        delivery_result = {"error": None}

        def callback(err, msg):
            if err:
                delivery_result["error"] = str(err)
                logger.error(f"Kafka delivery error: {err}")
            else:
                self.last_committed_kafka_offset = msg.offset()

        try:
            self.kafka_producer.produce(
                settings.kafka_topic_events,
                key=self.match_id.encode(),
                value=json.dumps(event).encode(),
                callback=callback,
            )
            self.kafka_producer.flush(timeout=5.0)
            if delivery_result["error"]:
                return False
            return True
        except Exception as e:
            logger.error(f"Kafka publish failed: {e}")
            return False

    def _publish_telemetry(self, state_snapshot):
        try:
            for pid, pdata in state_snapshot["players"].items():
                telem = {
                    "match_id": self.match_id,
                    "tick": state_snapshot["tick"],
                    "type": "move",
                    "player_id": pid,
                    "x": pdata["x"],
                    "y": pdata["y"],
                    "health": pdata["health"],
                    "score": pdata["score"],
                    "connected": pdata["connected"],
                    "timestamp": time.time(),
                }
                self.kafka_producer.produce(
                    settings.kafka_topic_telemetry,
                    key=self.match_id.encode(),
                    value=json.dumps(telem).encode(),
                )
            self.kafka_producer.flush(timeout=5.0)
        except Exception as e:
            logger.error(f"Telemetry publish failed: {e}")

    def _kafka_delivery_callback(self, err, msg):
        if err:
            logger.error(f"Kafka delivery error: {err}")
        else:
            self.last_committed_kafka_offset = msg.offset()

    async def _update_redis(self):
        state_key = f"match:{self.match_id}:state"
        offset_key = f"match:{self.match_id}:last_offset"
        players_key = f"match:{self.match_id}:players"
        await self.redis.set(state_key, json.dumps(self.state.to_dict()))
        await self.redis.set(offset_key, str(self.last_committed_kafka_offset))
        await self.redis.sadd(players_key, *list(self.state.players.keys()))
        ttl = int(self.match_duration_ticks / self.tick_rate) + 60
        for key in [state_key, offset_key, players_key]:
            await self.redis.expire(key, ttl)

    async def end_match(self):
        if self.match_ended:
            return
        self.match_ended = True
        self.running = False
        logger.info(f"Match {self.match_id} ended at tick {self.state.tick}")
        for pid, player in self.state.players.items():
            player.connected = False
        for ws in list(self._connected_players.values()):
            try:
                await ws.send_json({"type": "match_end", "scores": {pid: p.score for pid, p in self.state.players.items()}})
                await ws.close()
            except Exception:
                pass
        for ws in list(self._connected_spectators.values()):
            try:
                await ws.send_json({"type": "match_end"})
                await ws.close()
            except Exception:
                pass
        self._connected_players.clear()
        self._connected_spectators.clear()
        outcome = {"winner": max(self.state.players, key=lambda p: self.state.players[p].score) if self.state.players else None,
                   "scores": {pid: p.score for pid, p in self.state.players.items()}}
        lifecycle_event = {
            "match_id": self.match_id, "type": "match.end", "tick": self.state.tick,
            "outcome": outcome, "players": list(self.state.players.keys()),
            "started_at": self.state.started_at, "duration_seconds": time.time() - self.state.started_at,
        }
        try:
            self.kafka_producer.produce(settings.kafka_topic_lifecycle, key=self.match_id.encode(), value=json.dumps(lifecycle_event).encode())
            self.kafka_producer.flush()
        except Exception as e:
            logger.error(f"Lifecycle event failed: {e}")
        try:
            from couchbase.cluster import Cluster
            from couchbase.auth import PasswordAuthenticator
            from couchbase.options import ClusterOptions
            auth = PasswordAuthenticator(settings.couchbase_username, settings.couchbase_password)
            cluster = Cluster(f"couchbase://{settings.couchbase_host}", ClusterOptions(auth))
            bucket = cluster.bucket(settings.couchbase_matches_bucket)
            coll = bucket.default_collection()
            match_doc = {
                "type": "match", "matchId": self.match_id,
                "players": list(self.state.players.keys()),
                "startedAt": self.state.started_at, "endedAt": time.time(),
                "durationSeconds": time.time() - self.state.started_at, "outcome": outcome,
            }
            coll.upsert(self.match_id, match_doc)
            cluster.close()
        except Exception as e:
            logger.error(f"Couchbase match write failed: {e}")

        try:
            from .room_pool import register_room
            register_room(self.match_id)
            logger.info(f"Room {self.match_id} returned to pool")
        except Exception as e:
            logger.error(f"Room pool return failed: {e}")

        active_matches.labels(match_id=self.match_id).set(0)

    async def stop(self):
        self.running = False
        self.match_ended = True
        self.kafka_producer.flush()

import asyncio
import logging
import etcd3
from typing import Callable, Optional
from .config import settings
from .room_pool import register_room, mark_in_use, remove_room

logger = logging.getLogger("leader-election")


class LeaderElection:
    def __init__(self, match_id: str, instance_id: str, ttl: int = 5,
                 on_leadership_lost: Optional[Callable] = None):
        self.etcd = etcd3.client(host=settings.etcd_host, port=settings.etcd_port)
        self.match_id = match_id
        self.key = f"/match/{match_id}/leader"
        self.instance_id = instance_id
        self.ttl = ttl
        self.lease = None
        self.is_leader = False
        self._on_leadership_lost = on_leadership_lost

    async def campaign(self):
        while True:
            if self.lease:
                try:
                    self.lease.revoke()
                except Exception:
                    pass
            try:
                self.lease = self.etcd.lease(self.ttl)
                success, _ = self.etcd.transaction(
                    compare=[self.etcd.transactions.version(self.key) == 0],
                    success=[self.etcd.transactions.put(self.key, self.instance_id, lease=self.lease)],
                    failure=[],
                )
                if success:
                    self.is_leader = True
                    leader_addr_key = f"/match/{self.match_id}/leader-address"
                    address = f"game-room:{8000}"
                    self.etcd.put(leader_addr_key, address)
                    mark_in_use(self.match_id)
                    asyncio.create_task(self._keepalive())
                    asyncio.create_task(self.watch_for_leadership_loss())
                    logger.info(f"Elected leader for match {self.match_id} on {self.instance_id}")
                    return True
                else:
                    logger.info(f"Leader key already held, watching for leadership...")
                    self.is_leader = False
                    await self._poll_until_key_deleted()
            except Exception as e:
                logger.error(f"Leader election error: {e}")
                self.is_leader = False
                await asyncio.sleep(1)

    async def _poll_until_key_deleted(self):
        while True:
            try:
                value, meta = self.etcd.get(self.key)
                if value is None:
                    logger.info("Leader key deleted, attempting to campaign...")
                    return
            except Exception as e:
                logger.error(f"Poll leader key error: {e}")
            await asyncio.sleep(self.ttl / 3.0)

    async def _keepalive(self):
        while self.is_leader and self.lease:
            try:
                self.lease.refresh()
            except Exception as e:
                logger.error(f"Lease keepalive failed: {e}")
                self.is_leader = False
                break
            await asyncio.sleep(self.ttl / 3.0)

    async def step_down(self):
        self.is_leader = False
        if self.lease:
            try:
                self.lease.revoke()
            except Exception:
                pass
        try:
            self.etcd.delete(self.key)
            self.etcd.delete(f"/match/{self.match_id}/leader-address")
            remove_room(self.match_id)
        except Exception:
            pass
        if self._on_leadership_lost:
            await self._on_leadership_lost()
        logger.info(f"Stepped down as leader for match {self.match_id}")

    async def watch_for_leadership_loss(self):
        while True:
            try:
                value, meta = self.etcd.get(self.key)
                if value is None:
                    if self.is_leader:
                        self.is_leader = False
                        logger.warning("Leader key deleted (lease expired) - lost leadership")
                        register_room(self.match_id)
                        if self._on_leadership_lost:
                            await self._on_leadership_lost()
                    else:
                        logger.info("Leader key is gone, re-campaigning...")
                        asyncio.create_task(self.campaign())
                    break
                elif value.decode() != self.instance_id:
                    if self.is_leader:
                        self.is_leader = False
                        logger.warning("Lost leadership to another instance")
                        register_room(self.match_id)
                        if self._on_leadership_lost:
                            await self._on_leadership_lost()
                    break
            except Exception as e:
                logger.error(f"Leadership watch error: {e}")
            await asyncio.sleep(self.ttl / 3.0)

    async def start_follower_watch(self):
        while True:
            try:
                value, meta = self.etcd.get(self.key)
                if value is None:
                    logger.info("No leader, attempting to campaign...")
                    await self.campaign()
                    return
                else:
                    current_leader = value.decode()
                    logger.info(f"Current leader: {current_leader}, polling for deletion...")
                    await self._poll_until_key_deleted()
                    logger.info("Leader key deleted, re-campaigning...")
                    await self.campaign()
                    return
            except Exception as e:
                logger.error(f"Follower watch error: {e}")
                await asyncio.sleep(2)

import asyncio
import logging
import threading
from typing import Awaitable, Callable, Optional

import etcd3

from .config import settings
from .room_pool import remove_room

logger = logging.getLogger("uvicorn.error")


class LeaderElection:
    """etcd lease-based leader election.

    All etcd interaction is confined to a single worker thread so the etcd
    client and its lease are never used across threads / event loops. The
    public ``is_leader`` flag is read from the asyncio side, and lifecycle
    callbacks are scheduled back onto the main event loop.
    """

    def __init__(
        self,
        match_id: str,
        instance_id: str,
        ttl: int = 5,
        on_elected: Optional[Callable[[], Awaitable[None]]] = None,
        on_lost: Optional[Callable[[], Awaitable[None]]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.etcd = etcd3.client(host=settings.etcd_host, port=settings.etcd_port, timeout=5)
        self.match_id = match_id
        self.key = f"/match/{match_id}/leader"
        self.address_key = f"/match/{match_id}/leader-address"
        self.instance_id = instance_id
        self.leader_address = settings.room_address or f"{instance_id}:8000"
        self.ttl = ttl
        self.lease = None
        self.is_leader = False
        self.etcd_ok = False
        self._on_elected = on_elected
        self._on_lost = on_lost
        self._loop = loop
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self.run, name="leader-election", daemon=True)
        self._thread.start()

    def run(self):
        try:
            while not self._stop.is_set():
                try:
                    if self._campaign():
                        self._serve_as_leader()
                    else:
                        self._wait_for_key_release()
                except Exception as e:
                    logger.error(f"Leader election error: {e}", exc_info=True)
                    self.etcd_ok = False
                    self._stop.wait(1)
        finally:
            self._cleanup()

    def step_down(self):
        """Stop campaigning, release the lease and clean up keys (blocking)."""
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.ttl + 2)

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    # ------------------------------------------------------------------ #
    # election
    # ------------------------------------------------------------------ #
    def _campaign(self) -> bool:
        self._revoke_lease()
        try:
            self.lease = self.etcd.lease(self.ttl)
            success, _ = self.etcd.transaction(
                compare=[self.etcd.transactions.version(self.key) == 0],
                success=[self.etcd.transactions.put(self.key, self.instance_id, lease=self.lease)],
                failure=[],
            )
            self.etcd_ok = True
        except Exception as e:
            logger.error(f"Campaign failed: {e}")
            self.etcd_ok = False
            self.lease = None
            self._stop.wait(1)
            return False

        if not success:
            logger.info("Leader key already held, waiting for release")
            return False

        self.is_leader = True
        try:
            self.etcd.put(self.address_key, self.leader_address, lease=self.lease)
        except Exception as e:
            logger.error(f"Failed to publish leader address: {e}")
        logger.info(f"Elected leader for match {self.match_id} on {self.instance_id} ({self.leader_address})")
        self._dispatch(self._on_elected)
        return True

    def _serve_as_leader(self):
        while not self._stop.is_set() and self.is_leader:
            try:
                value, _ = self.etcd.get(self.key)
                if value is None or value.decode() != self.instance_id:
                    logger.warning("Leader key no longer held; stepping down")
                    self._lose_leadership()
                    return
                self.lease.refresh()
                self.etcd_ok = True
            except Exception as e:
                logger.error(f"Lease keepalive failed: {e}")
                self.etcd_ok = False
                self._lose_leadership()
                return
            self._stop.wait(self.ttl / 3.0)

    def _wait_for_key_release(self):
        while not self._stop.is_set():
            try:
                value, _ = self.etcd.get(self.key)
                self.etcd_ok = True
                if value is None:
                    return
            except Exception as e:
                logger.error(f"Poll leader key error: {e}")
                self.etcd_ok = False
            self._stop.wait(self.ttl / 3.0)

    # ------------------------------------------------------------------ #
    # cleanup helpers
    # ------------------------------------------------------------------ #
    def _lose_leadership(self):
        if not self.is_leader:
            return
        self.is_leader = False
        self._revoke_lease()
        self._delete_if_owned(self.address_key, self.leader_address)
        logger.warning(f"Lost leadership for match {self.match_id} on {self.instance_id}")
        self._dispatch(self._on_lost)

    def _cleanup(self):
        was_leader = self.is_leader
        self.is_leader = False
        self._revoke_lease()
        self._delete_if_owned(self.key, self.instance_id)
        self._delete_if_owned(self.address_key, self.leader_address)
        if was_leader:
            try:
                remove_room(self.match_id)
            except Exception:
                pass
        logger.info(f"Leader election stopped for match {self.match_id} on {self.instance_id}")

    def _revoke_lease(self):
        if self.lease is not None:
            try:
                self.lease.revoke()
            except Exception:
                pass
            self.lease = None

    def _delete_if_owned(self, key, expected_value):
        """Best-effort delete of ``key`` only if it still holds our value."""
        try:
            self.etcd.transaction(
                compare=[self.etcd.transactions.value(key) == expected_value],
                success=[self.etcd.transactions.delete(key)],
                failure=[],
            )
        except Exception:
            pass

    def _dispatch(self, callback):
        if callback is None:
            return
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._schedule_callback, callback)
        else:
            logger.error("Main event loop unavailable; callback not dispatched")

    @staticmethod
    def _schedule_callback(callback):
        task = asyncio.create_task(callback())

        def report_failure(completed):
            if completed.cancelled():
                return
            error = completed.exception()
            if error is not None:
                logger.error("Leader lifecycle callback failed", exc_info=error)

        task.add_done_callback(report_failure)

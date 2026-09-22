import logging

import etcd3

from .config import settings

logger = logging.getLogger("room-manager")


def get_etcd_client():
    return etcd3.client(host=settings.etcd_host, port=settings.etcd_port)


def get_available_room() -> str | None:
    """Return an idle room ID or None. Read-only; prefer claim_available_room."""
    client = get_etcd_client()
    for value, metadata in client.get_prefix('/rooms/available/'):
        room_id = metadata.key.decode().split('/')[-1]
        raw_value = value.decode() if value else ""
        if raw_value == "available":
            return room_id
    return None


def claim_available_room() -> str | None:
    """Atomically claim an idle room by flipping it to 'in-use'.

    Uses an etcd compare-and-swap so two matchmaking workers can never be
    handed the same room. Returns the claimed room ID, or None.
    """
    client = get_etcd_client()
    for value, metadata in client.get_prefix('/rooms/available/'):
        key = metadata.key.decode()
        room_id = key.split('/')[-1]
        raw_value = value.decode() if value else ""
        if raw_value != "available":
            continue
        try:
            success, _ = client.transaction(
                compare=[client.transactions.value(key) == "available"],
                success=[client.transactions.put(key, "in-use")],
                failure=[],
            )
        except Exception as e:
            logger.error(f"Room claim transaction failed for {room_id}: {e}")
            continue
        if success:
            logger.info(f"Claimed room {room_id}")
            return room_id
    return None


def register_room(room_id: str):
    client = get_etcd_client()
    client.put(f'/rooms/available/{room_id}', 'available')


def remove_room(room_id: str):
    client = get_etcd_client()
    client.delete(f'/rooms/available/{room_id}')

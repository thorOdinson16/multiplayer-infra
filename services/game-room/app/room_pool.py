import etcd3
import logging
from .config import settings

logger = logging.getLogger("room-pool")


def get_etcd_client():
    return etcd3.client(host=settings.etcd_host, port=settings.etcd_port)


def register_room(room_id: str):
    client = get_etcd_client()
    client.put(f'/rooms/available/{room_id}', 'available')
    logger.info(f"Room {room_id} registered as available in pool")


def mark_in_use(room_id: str):
    client = get_etcd_client()
    client.put(f'/rooms/available/{room_id}', 'in-use')


def remove_room(room_id: str):
    client = get_etcd_client()
    client.delete(f'/rooms/available/{room_id}')


def get_available_room() -> str | None:
    client = get_etcd_client()
    for value, metadata in client.get_prefix('/rooms/available/'):
        room_id = metadata.key.decode().split('/')[-1]
        raw_value = value.decode() if value else ""
        if raw_value == "available":
            return room_id
    return None

import etcd3
from .config import settings

def get_etcd_client():
    return etcd3.client(host=settings.etcd_host, port=settings.etcd_port)

def get_available_room() -> str | None:
    """Returns a room ID from /rooms/available/ or None."""
    client = get_etcd_client()
    for value, metadata in client.get_prefix('/rooms/available/'):
        room_id = metadata.key.decode().split('/')[-1]
        return room_id
    return None

def register_room(room_id: str):
    client = get_etcd_client()
    client.put(f'/rooms/available/{room_id}', 'available', lease=None)

def remove_room(room_id: str):
    client = get_etcd_client()
    client.delete(f'/rooms/available/{room_id}')
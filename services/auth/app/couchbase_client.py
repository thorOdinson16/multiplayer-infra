from datetime import timedelta
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, UpsertOptions
from couchbase.exceptions import DocumentNotFoundException
from .config import settings

_cluster = None

def get_cluster():
    global _cluster
    if _cluster is None:
        auth = PasswordAuthenticator(settings.couchbase_username, settings.couchbase_password)
        _cluster = Cluster(f"couchbase://{settings.couchbase_host}", ClusterOptions(auth))
        _cluster.wait_until_ready(timedelta(seconds=30))
    return _cluster

def get_bucket(bucket_name):
    return get_cluster().bucket(bucket_name)

def get_collection(bucket_name):
    return get_bucket(bucket_name).default_collection()

# ---------- sessions bucket ----------
def store_session(session_id: str, session_doc: dict, ttl_seconds: int):
    coll = get_collection(settings.couchbase_sessions_bucket)
    opts = UpsertOptions(expiry=timedelta(seconds=ttl_seconds))
    coll.upsert(session_id, session_doc, opts)

def get_session(session_id: str) -> dict | None:
    coll = get_collection(settings.couchbase_sessions_bucket)
    try:
        res = coll.get(session_id)
        return res.content_as[dict]
    except DocumentNotFoundException:
        return None

def delete_session(session_id: str):
    coll = get_collection(settings.couchbase_sessions_bucket)
    try:
        coll.remove(session_id)
    except DocumentNotFoundException:
        pass

# ---------- players bucket ----------
def store_player(player_id: str, player_doc: dict):
    coll = get_collection(settings.couchbase_players_bucket)
    coll.upsert(player_id, player_doc)

def get_player(player_id: str) -> dict | None:
    coll = get_collection(settings.couchbase_players_bucket)
    try:
        res = coll.get(player_id)
        return res.content_as[dict]
    except DocumentNotFoundException:
        return None

def get_player_by_username(username: str) -> dict | None:
    cluster = get_cluster()
    query = f"SELECT * FROM `{settings.couchbase_players_bucket}` WHERE username = $1"
    result = cluster.query(query, username)
    rows = list(result.rows())
    if rows:
        return rows[0][settings.couchbase_players_bucket]
    return None

def close_connections():
    global _cluster
    if _cluster is not None:
        try:
            _cluster.close()
        except Exception:
            pass
        _cluster = None
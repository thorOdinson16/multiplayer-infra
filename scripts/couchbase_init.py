#!/usr/bin/env python3
"""Couchbase cluster and bucket initialiser for local development."""
import time
import requests
import json

COUCHBASE_HOST = "couchbase"
ADMIN = "Administrator"
PASSWORD = "password"
BUCKETS = ["sessions", "players", "matches", "replays"]
RAM_QUOTA_MB = 100

base_url = f"http://{COUCHBASE_HOST}:8091"
auth = (ADMIN, PASSWORD)


def wait_for_couchbase():
    print("Waiting for Couchbase...")
    for _ in range(90):
        try:
            r = requests.get(f"{base_url}/pools", auth=auth)
            if r.status_code == 200:
                print("Couchbase ready")
                return True
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError("Couchbase did not become ready")


def init_cluster():
    print("Initialising cluster...")
    try:
        r = requests.get(f"{base_url}/pools/default", auth=auth)
        if r.status_code == 200:
            print("Cluster already initialised.")
            return
    except Exception:
        pass
    r = requests.post(
        f"{base_url}/clusterInit",
        data={
            "services": "kv,n1ql,index",
            "username": ADMIN,
            "password": PASSWORD,
            "port": "SAME",
        },
    )
    if r.status_code in (200, 201):
        print("Cluster initialised.")
        time.sleep(10)
    else:
        print(f"Cluster init status {r.status_code}: {r.text}")


def create_buckets():
    print("Creating buckets...")
    for bucket in BUCKETS:
        r = requests.post(
            f"{base_url}/pools/default/buckets",
            auth=auth,
            data={
                "name": bucket,
                "ramQuotaMB": RAM_QUOTA_MB,
                "bucketType": "couchbase",
            },
        )
        if r.status_code in (200, 202):
            print(f"  Bucket '{bucket}' created.")
        elif r.status_code == 400 and "already exists" in r.text.lower():
            print(f"  Bucket '{bucket}' already exists.")
        else:
            print(f"  Bucket '{bucket}' status {r.status_code}: {r.text}")
    print("All buckets created.")


def create_indexes():
    print("Creating N1QL indexes...")
    n1ql_endpoint = f"{base_url}/query/service"
    indexes = [
        "CREATE PRIMARY INDEX ON `players` USING GSI",
        "CREATE PRIMARY INDEX ON `sessions` USING GSI",
        "CREATE PRIMARY INDEX ON `matches` USING GSI",
        "CREATE PRIMARY INDEX ON `replays` USING GSI",
        "CREATE INDEX idx_players_username ON `players`(username) USING GSI",
        "CREATE INDEX idx_players_elo ON `players`(eloRating) USING GSI",
        "CREATE INDEX idx_players_lastseen ON `players`(lastSeen) USING GSI",
        "CREATE INDEX idx_players_type ON `players`(type) USING GSI",
    ]
    for idx in indexes:
        r = requests.post(n1ql_endpoint, auth=auth, data={"statement": idx})
        if r.status_code == 200:
            result = r.json()
            if result.get("status") == "success" or "already exists" in result.get("errors", [{}])[0].get("msg", ""):
                print(f"  Index: {idx[:60]}... OK")
            else:
                print(f"  Index: {idx[:60]}... {result.get('status', 'unknown')}")
        elif "already exists" in r.text.lower():
            print(f"  Index: {idx[:60]}... already exists")
        else:
            print(f"  Index: {idx[:60]}... {r.status_code}")
    print("Indexes created.")


if __name__ == "__main__":
    wait_for_couchbase()
    init_cluster()
    create_buckets()
    create_indexes()
    print("Couchbase initialised successfully.")

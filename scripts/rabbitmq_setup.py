import requests
import os

HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq") + ":15672"
AUTH = ("guest", "guest")

def put(url, json):
    r = requests.put(f"http://{HOST}/api/{url}", auth=AUTH, json=json)
    r.raise_for_status()
    return r

# Exchange
put("exchanges/%2F/notifications.exchange", {"type": "topic", "durable": True})

# matchmaking.requests queue with TTL and dead-letter
put("queues/%2F/matchmaking.requests", {
    "durable": True,
    "arguments": {
        "x-dead-letter-exchange": "notifications.exchange",
        "x-dead-letter-routing-key": "match.expired",
        "x-message-ttl": 60000  # 60s (2x default 30s window)
    }
})

# matchmaking.expired queue
put("queues/%2F/matchmaking.expired", {"durable": True})

# Bind expired queue to exchange with routing key match.expired
requests.post(f"http://{HOST}/api/bindings/%2F/e/notifications.exchange/q/matchmaking.expired",
              auth=AUTH, json={"routing_key": "match.expired"})
print("RabbitMQ setup complete.")
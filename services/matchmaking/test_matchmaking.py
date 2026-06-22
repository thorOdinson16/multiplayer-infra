import httpx, json, os

TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlNGZlN2IyMy04Yzg4LTRlNmUtOTM4NC0xNTZjMTk3ODRmZGQiLCJpYXQiOjE3ODIwNjUyMTUsImV4cCI6MTc4MjE1MTYxNX0.RTOSb2UhI3Job7kwySv5vi6xNqPXNOoSoQRP0C0-1Pv8zrjR97BWRxpZ6QtIrJUXBFE-3mAf5rNbETT1JCE2CCCgOqF7D78-fVVcSwhuqQTQIA1Cod_58BASjXzJHEt_rykyCVy8gWC0dw_mpOxg8rq9abcuUP_7kmX-Zeg72M0foulYnWPJ0DMChPHtIierOeULKrDzKT5WZgOt_xsYkdB-foZd1bGlGFqoBE2fdjbzD7tPBlkV_W39Q5OF6Z9ezckgh6WgnXXI8JoxPrIf9xi1veFXVjPlKkDqs9I6FRnUYOmOu_VIvzBU7Fb1LmgnmbgLXDMd8eQYNSmUPRc5dg"

payload = {
    "vhost": "/",
    "name": "amq.default",
    "properties": {},
    "routing_key": "matchmaking.requests",
    "payload_encoding": "string",
    "payload": json.dumps({"token": TOKEN, "elo": 1250})
}

r = httpx.post("http://localhost:15672/api/exchanges/%2F/amq.default/publish",
              auth=("guest", "guest"), json=payload)
print(r.status_code, r.text)
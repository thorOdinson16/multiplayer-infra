"""replay service entrypoint."""
import os
import asyncio
from fastapi import FastAPI

app = FastAPI(title="replay-service")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    # TODO: add real readiness checks (DB, broker, etc.)
    return {"status": "ready"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

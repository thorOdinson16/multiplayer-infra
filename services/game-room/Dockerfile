FROM python:3.11-slim
WORKDIR /app
COPY services/game-room/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/game-room/app/ ./app/
COPY lib/ ./lib/
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

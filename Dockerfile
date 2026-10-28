FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY sql ./sql

ENV POLL_INTERVAL_SECONDS=2
CMD ["python", "-m", "app.worker_runner"]

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg libchromaprint-tools sox git curl \
    aubio-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir tiddl

COPY . .
RUN chmod +x entrypoint.sh
RUN mkdir -p /app/data

ENTRYPOINT ["./entrypoint.sh"]

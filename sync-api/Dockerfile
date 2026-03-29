FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libchromaprint-tools \
    sox \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY sync-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sync-api/ .
COPY VERSION /app/VERSION

ARG GIT_SHA=unknown
ENV GIT_SHA=$GIT_SHA

RUN chmod +x entrypoint.sh

EXPOSE 8402

ENTRYPOINT ["./entrypoint.sh"]

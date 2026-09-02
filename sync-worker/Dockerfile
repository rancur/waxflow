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

# /app/VERSION is what tasks/auto_update.py compares against the latest GitHub
# release. This context is ./sync-worker, so the repo-root VERSION file cannot be
# COPYed; the release workflow (and docker-compose) pass it as a build arg.
ARG VERSION=unknown
RUN echo "$VERSION" > /app/VERSION

ENTRYPOINT ["./entrypoint.sh"]

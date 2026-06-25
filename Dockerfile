FROM python:3.12-slim

# Logs stream straight to Docker; no .pyc clutter.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# gosu is required so the entrypoint can drop from root to app while still
# forwarding signals correctly (plain `su` breaks SIGTERM delivery).
# Dependencies first for layer caching.
COPY requirements.txt ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends gosu \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Non-root user that will own the output files once the entrypoint chowns /data.
RUN groupadd --gid 1000 app \
    && useradd --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin app

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "main"]

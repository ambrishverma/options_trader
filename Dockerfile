# Options Trader — application container
#
# Python 3.13 matches the host scheduler (3.13.9), so the fixture-replay parity
# test in Phase 5 compares like with like.  A different interpreter minor
# version would make that test prove much less than it appears to.
FROM python:3.13-slim

# Fail the build on any error inside a pipeline, so a curl failure surfaces
# directly instead of as a confusing NO_PUBKEY error two steps later.
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# tini reaps zombies from the gsutil subprocess calls added in Phase 2; without
# a real init as PID 1 a long-running container accumulates defunct processes.
# google-cloud-cli provides gsutil for strategy fetch and GCS sync.
#
# GCLOUD_VERSION is pinned for the same reason requirements.lock exists: an
# unpinned apt package floats on every rebuild, which is the same
# reproducibility hole in a different package manager.  Bump deliberately.
ARG GCLOUD_VERSION=580.0.0-0
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg tini \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends "google-cloud-cli=${GCLOUD_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies before source: editing code then does not invalidate the
# (slow) dependency layer on rebuild.
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock

COPY . .

# Without this Python buffers stdout and Cloud Logging receives logs in delayed
# chunks — painful precisely when you are mid-incident.
ENV PYTHONUNBUFFERED=1

# Arms the safety gate in the IMAGE, not just in docker-compose.yaml.
# _force_dry_run keys off this variable, so an image run outside the
# shipped compose file — a docker run smoke test, or a Phase 2/4 unit that
# omits the environment block — would otherwise be a fully ungated live
# instance beside the laptop scheduler.  Compose sets the same value, so
# the supported path is unchanged.
ENV TRADER_DATA_DIR=/data

# /data is the persistent disk mount: snapshots, logs, cache, recommendations, tokens
EXPOSE 8080
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "main.py", "--serve"]

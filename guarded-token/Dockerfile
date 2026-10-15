# Image for the token tools, monitor and watchdog.
# Build:  docker compose build

ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Run as an ordinary user, not root.
RUN useradd --create-home app
WORKDIR /app

# Install dependencies first so code changes don't redo this slow step.
COPY requirements.txt .
RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install -r requirements.txt
ENV PATH=/opt/venv/bin:$PATH

COPY . .
RUN mkdir -p /app/keys /data && chown -R app:app /app /data
USER app

# Compile the contract now, so containers start fast.
RUN ape compile

# Train the Phase 3 detection models (about a minute). Training inside the
# image guarantees the model matches the installed library versions.
RUN python -m ml.train

ENTRYPOINT ["python", "docker/entrypoint.py"]
CMD ["ape", "test"]

# syntax=docker/dockerfile:1.7

ARG UV_IMAGE="ghcr.io/astral-sh/uv:0.11.14@sha256:1025398289b62de8269e70c45b91ffa37c373f38118d7da036fb8bb8efc85d97"
ARG PYTHON_IMAGE="python:3.12.10-slim-bookworm@sha256:fd95fa221297a88e1cf49c55ec1828edd7c5a428187e67b5d1805692d11588db"

FROM ${UV_IMAGE} AS uv
FROM ${PYTHON_IMAGE} AS runtime

ARG SOURCE_REVISION="unknown"
ARG SOURCE_CREATED="1970-01-01T00:00:00Z"

LABEL org.opencontainers.image.title="Lifecycle Authority" \
      org.opencontainers.image.description="Standalone deterministic lifecycle authority for 33GOD" \
      org.opencontainers.image.source="https://github.com/delorenj/lifecycle" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.created="${SOURCE_CREATED}" \
      org.opencontainers.image.version="1.0.0"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PATH=/app/.venv/bin:${PATH}

WORKDIR /app

COPY --from=uv /uv /uvx /usr/local/bin/
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY migrations ./migrations
COPY contracts ./contracts
COPY docs ./docs

USER 65532:65532
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-m", "main", "healthcheck", "--url", "http://127.0.0.1:8080/livez"]

ENTRYPOINT ["python", "-m", "main"]
CMD ["serve"]

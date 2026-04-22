FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

RUN apt-get update && apt-get install -y \
    libpq-dev gcc ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
RUN uv sync --locked

COPY data_provider_meteostat.py .
COPY seeds/ seeds/

ENTRYPOINT ["uv", "run", "python", "data_provider_meteostat.py"]
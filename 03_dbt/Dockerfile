FROM python:3.12-slim

RUN apt-get update && apt-get install -y jq && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies via uv
RUN uv sync --locked

# DBT project
COPY dbt_project/ ./dbt_project/
COPY dbt_project.yml ./dbt_project.yml
COPY models/ ./models/
COPY report.py ./

# Application code
COPY entrypoint.sh ./entrypoint.sh

RUN chmod +x entrypoint.sh

# DBT profiles
COPY profiles.yml /root/.dbt/profiles.yml

# Entrypoint
ENTRYPOINT ["bash", "./entrypoint.sh"]
FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL postgres;');"

COPY src/ ./src/

ENV PARTICIPANTE=etevaldo15 \
    PG_TABLE=etevaldo15_empresas \
    PG_HOST=postgres_db \
    PG_PORT=5432 \
    PG_USER=homelab_postgres \
    PG_PASSWORD=postgres \
    PG_DB=db_empresas

CMD ["python", "src/main.py"]
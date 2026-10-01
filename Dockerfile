FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -e ".[dev]"

# Instala a extensão do DuckLake no build para o pipeline não depender de rede em runtime.
RUN python -c "import duckdb; duckdb.connect().install_extension('ducklake')"

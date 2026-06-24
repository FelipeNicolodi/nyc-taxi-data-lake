# Base presa numa versão fixa, para o build ser sempre igual.
FROM python:3.11-slim-bookworm

# JDK headless: PySpark roda sobre a JVM. JDK 17 é compatível com Spark 3.5.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1
# DATA_ROOT aponta para o volume montado (ver docker-compose.yml).
ENV DATA_ROOT=/app/data

WORKDIR /app

# Instala dependências.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Baixa o JAR do Delta já no build, para a primeira execução não ter que baixar.
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; \
    configure_spark_with_delta_pip(SparkSession.builder).getOrCreate().stop()"

COPY src/ ./src/
COPY analysis/ ./analysis/
COPY main.py ./main.py

# Usuário não-root.
RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app
USER appuser

# Por padrão roda a ingestão (Fase 1). Dá pra trocar no compose/CLI para outras etapas.
CMD ["python", "-m", "src.ingestion"]

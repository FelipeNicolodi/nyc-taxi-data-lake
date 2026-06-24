"""Entrypoint único do pipeline: ingestion -> transform -> gold.

Encadeia as três etapas em uma única SparkSession, com os DQ gates entre as camadas.
Cada etapa é idempotente e também pode ser executada isolada via `python -m src.<etapa>`.

Uso:
    docker compose run --rm pipeline python main.py
"""

from __future__ import annotations

import logging

from src import dq, gold, transform
from src.ingestion import download_to_landing, ingest_to_bronze
from src.spark import get_spark

logger = logging.getLogger("pipeline")


def main() -> None:
    # 1) Download para a landing (Python puro, fora do Spark).
    download_to_landing()

    spark = get_spark("nyc-taxi-pipeline")

    # 2) Landing -> bronze, com gate de saída.
    bronze = ingest_to_bronze(spark)
    dq.gate_bronze(bronze)

    # 3) Bronze -> silver (gate silver roda dentro do transform, antes de persistir).
    transform.bronze_to_silver(spark)

    # 4) Silver -> gold (fato de consumo + marts das duas perguntas).
    gold.silver_to_gold(spark)

    logger.info("Pipeline completo: landing -> bronze -> silver -> gold.")
    spark.stop()


if __name__ == "__main__":
    main()

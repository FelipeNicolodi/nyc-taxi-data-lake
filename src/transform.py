"""Conformação bronze -> silver: limpeza, tipagem, colunas obrigatórias e dedup.

Decisões de limpeza (todas saíram da análise exploratória da bronze, ver README):
  - Remove total_amount negativo (estornos), que puxam a média da Q1 para baixo.
  - Remove corridas com pickup >= dropoff (intervalo de tempo inválido).
  - Filtra jan a mai/2023 pela data real do pickup, não pelo nome do arquivo
    (a bronze tem datas de 2001 a 2023-09).
  - Remove duplicatas pela chave de negócio.
  - Mantém passenger_count nulo/zero: são corridas válidas, com total_amount válido,
    que a Q1 usa. Tirar o passageiro não registrado é regra de negócio da Q2, não
    limpeza estrutural.

Fica só com as 5 colunas obrigatórias mais a linhagem. As outras colunas da TLC
ficam de fora (o enunciado permite), deixando a silver enxuta.

No fim, loga quantos registros cada regra removeu.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config, dq
from src.spark import get_spark

logger = logging.getLogger("transform")


def _read_bronze(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load(config.BRONZE_PATH)


def _diagnostics(df: DataFrame) -> dict[str, int]:
    """Conta, em uma única passada, quantos registros cada regra vai descartar."""
    row = df.select(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias("neg_total"),
        F.sum(
            F.when(
                F.col("tpep_pickup_datetime") >= F.col("tpep_dropoff_datetime"), 1
            ).otherwise(0)
        ).alias("bad_interval"),
        F.sum(
            F.when(
                (F.col("tpep_pickup_datetime") < F.lit(dq.DATE_LOWER).cast("timestamp"))
                | (F.col("tpep_pickup_datetime") >= F.lit(dq.DATE_UPPER).cast("timestamp")),
                1,
            ).otherwise(0)
        ).alias("out_of_range"),
    ).first()
    return row.asDict()


def bronze_to_silver(spark: SparkSession) -> DataFrame:
    """Lê a bronze, conforma e grava a silver em Delta. Retorna o DataFrame silver."""
    config.ensure_dirs()
    # Não dá cache na bronze inteira: re-ler o Delta a cada ação é barato e evita
    # segurar 16M linhas no heap do driver (o que estourava a memória no modo local).
    bronze = _read_bronze(spark)

    diag = _diagnostics(bronze)
    logger.info("Bronze: %d linhas.", diag["total"])

    # 1) Limpeza estrutural (linha-a-linha).
    cleaned = bronze.filter(
        (F.col("total_amount") >= 0)
        & (F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
        & (F.col("tpep_pickup_datetime") >= F.lit(dq.DATE_LOWER).cast("timestamp"))
        & (F.col("tpep_pickup_datetime") < F.lit(dq.DATE_UPPER).cast("timestamp"))
    )

    # 2) Tipa as colunas obrigatórias de forma explícita.
    conformed = cleaned.select(
        F.col("VendorID").cast("int").alias("VendorID"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
        F.col("source_file"),
        F.col("ingestion_timestamp"),
    )

    # 3) Remove duplicatas pela chave de negócio.
    before_dedup = conformed.count()
    silver = conformed.dropDuplicates(dq.BUSINESS_KEY)
    after_dedup = silver.count()

    # Loga quantos registros cada regra removeu.
    kept = after_dedup
    logger.info("Descartes na conformação bronze -> silver:")
    logger.info("  total_amount negativo : %d", diag["neg_total"])
    logger.info("  pickup >= dropoff     : %d", diag["bad_interval"])
    logger.info("  data fora jan-mai/2023: %d", diag["out_of_range"])
    logger.info("  duplicatas (chave neg): %d", before_dedup - after_dedup)
    logger.info("  -> silver mantém %d de %d linhas (%.2f%%).",
                kept, diag["total"], 100.0 * kept / diag["total"])

    # 4) Roda o gate antes de gravar: dado ruim não vira silver.
    dq.gate_silver(silver)

    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(config.SILVER_PATH)
    )
    logger.info("Silver gravada em %s", config.SILVER_PATH)

    return spark.read.format("delta").load(config.SILVER_PATH)


def run() -> None:
    """Entrypoint da Fase 2: bronze -> silver com gates e relatório de descartes."""
    spark = get_spark("nyc-taxi-transform")
    # Gate de entrada: valida a bronze antes de transformar.
    dq.gate_bronze(_read_bronze(spark))
    silver = bronze_to_silver(spark)
    logger.info("Silver final: %d linhas.", silver.count())
    spark.stop()


if __name__ == "__main__":
    run()

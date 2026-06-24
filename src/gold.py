"""Camada de consumo silver -> gold.

Materializa:
  1. `fact_trips`: a fato de consumo, com as 5 colunas obrigatórias mais colunas de
     tempo derivadas (pickup_month, pickup_hour) que servem o SQL ad-hoc e os marts.
     É a tabela que os usuários finais consultam à vontade.
  2. `agg_avg_total_amount_by_month` (mart da Q1): média de total_amount por mês.
  3. `agg_avg_passengers_by_hour_may` (mart da Q2): média de passageiros por hora do
     dia em maio/2023.

Os marts são as duas perguntas já pré-agregadas. As respostas legíveis ficam em
`analysis/q1_*.py` e `analysis/q2_*.py`, que consultam a fato.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.spark import get_spark

logger = logging.getLogger("gold")


def build_fact(spark: SparkSession) -> DataFrame:
    """Cria a fato de consumo a partir da silver, com colunas de tempo derivadas."""
    silver = spark.read.format("delta").load(config.SILVER_PATH)

    fact = silver.select(
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        # Colunas de tempo para facilitar as análises (Q1 por mês, Q2 por hora).
        F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month"),
        F.hour("tpep_pickup_datetime").alias("pickup_hour"),
    )

    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(config.GOLD_FACT_PATH)
    )
    logger.info("Gold fact_trips gravada em %s", config.GOLD_FACT_PATH)
    return spark.read.format("delta").load(config.GOLD_FACT_PATH)


def build_mart_q1(fact: DataFrame) -> DataFrame:
    """Mart Q1: média de total_amount por mês (todas as corridas yellow).

    A média é por corrida, agrupada por mês (dá 5 médias). O total_amount negativo
    já saiu na silver.
    """
    mart = (
        fact.groupBy("pickup_month")
        .agg(
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.count(F.lit(1)).alias("trips"),
        )
        .orderBy("pickup_month")
    )
    mart.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(config.GOLD_MART_Q1_PATH)
    logger.info("Gold mart Q1 gravada em %s", config.GOLD_MART_Q1_PATH)
    return mart


def build_mart_q2(fact: DataFrame) -> DataFrame:
    """Mart Q2: média de passageiros por hora do dia em maio/2023.

    "Média de passageiros" só faz sentido com passageiro registrado, então deixamos
    de fora passenger_count nulo e zero (que ficam na silver). O escopo é yellow,
    como na ingestão (incluir green/FHV exigiria outras fontes).
    """
    mart = (
        fact.filter(
            (F.col("pickup_month") == "2023-05")
            & (F.col("passenger_count").isNotNull())
            & (F.col("passenger_count") > 0)
        )
        .groupBy("pickup_hour")
        .agg(
            F.round(F.avg("passenger_count"), 3).alias("avg_passenger_count"),
            F.count(F.lit(1)).alias("trips"),
        )
        .orderBy("pickup_hour")
    )
    mart.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(config.GOLD_MART_Q2_PATH)
    logger.info("Gold mart Q2 gravada em %s", config.GOLD_MART_Q2_PATH)
    return mart


def silver_to_gold(spark: SparkSession) -> None:
    """Constrói a fato de consumo e os dois marts de análise."""
    config.ensure_dirs()
    fact = build_fact(spark)
    build_mart_q1(fact)
    build_mart_q2(fact)


def run() -> None:
    """Entrypoint da Fase 3: silver -> gold (fato + marts)."""
    spark = get_spark("nyc-taxi-gold")
    silver_to_gold(spark)
    logger.info("Gold construída (fact_trips + marts Q1 e Q2).")
    spark.stop()


if __name__ == "__main__":
    run()

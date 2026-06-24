"""Cria a SparkSession com Delta Lake, ajustada ao ambiente.

A ideia é o mesmo código rodar em dois lugares sem mudança:

- Local / Docker: cria a sessão com configure_spark_with_delta_pip, que baixa o JAR
  do Delta na versão certa para o PySpark instalado (evita descasamento de versão).
- Databricks (Free Edition): o runtime já vem com Spark e Delta, então só
  reaproveitamos a sessão que já existe. Lá o configure_spark_with_delta_pip nem
  funciona e nem é preciso.

Para saber onde está rodando, olhamos a variável DATABRICKS_RUNTIME_VERSION, que o
Databricks sempre define.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

APP_NAME = "nyc-taxi-pipeline"


def on_databricks() -> bool:
    """True se executando dentro de um runtime Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = APP_NAME) -> SparkSession:
    """Retorna uma SparkSession com Delta Lake habilitado, adequada ao ambiente.

    Idempotente: reaproveita a sessão ativa se já existir.
    """
    if on_databricks():
        # No Databricks o Delta já vem pronto; reaproveita a sessão do runtime.
        spark = SparkSession.builder.getOrCreate()
        # Fixa o fuso em UTC para a hora/data sair igual em qualquer máquina (importa na Q2).
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        return spark

    # Local/Docker: importa o delta-spark só aqui, porque no Databricks ele não existe.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Fuso fixo em UTC para a extração de hora/data sair igual em qualquer máquina
        # (importa na Q2, que agrupa por hora do dia).
        .config("spark.sql.session.timeZone", "UTC")
        # Poucas partições de shuffle: o volume cabe numa máquina e evita
        # gerar muitos arquivos pequenos no Delta local.
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

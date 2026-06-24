"""Ingestão: download dos Parquet originais (TLC) → landing zone → bronze (Delta).

Duas etapas, ambas idempotentes:

1. `download_to_landing`: baixa os 5 Parquet da CloudFront da TLC para a landing.
   Pula arquivos que já estão lá, já que a landing não muda (1:1 com a fonte).
2. `ingest_to_bronze`: lê os Parquet da landing e grava em Delta na bronze,
   acrescentando os campos de linhagem (`source_file`, `ingestion_timestamp`).
   Fica 1:1 com a origem, sem regra de negócio. Grava em modo overwrite para que
   rodar de novo gere o mesmo resultado (é batch histórico).

PySpark entra na etapa landing -> bronze (requisito do case).
"""

from __future__ import annotations

import functools
import logging
from pathlib import Path

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.spark import get_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingestion")

# Timeout generoso: arquivos têm dezenas de MB e o servidor pode ser lento.
_DOWNLOAD_TIMEOUT = 120
_CHUNK_SIZE = 1 << 20  # 1 MiB


def download_to_landing(months: list[str] | None = None) -> list[Path]:
    """Baixa os Parquet originais para a landing zone. Idempotente (pula existentes).

    Retorna a lista de paths locais dos arquivos disponíveis na landing.
    """
    months = months or config.MONTHS
    config.ensure_dirs()
    paths: list[Path] = []

    for month in months:
        url = config.source_url(month)
        dest = config.LANDING_DIR / config.source_filename(month)

        if dest.exists() and dest.stat().st_size > 0:
            logger.info("Landing já contém %s (%.1f MB), pulando download.",
                        dest.name, dest.stat().st_size / 1e6)
            paths.append(dest)
            continue

        logger.info("Baixando %s -> %s", url, dest.name)
        _download_file(url, dest)
        logger.info("Concluído %s (%.1f MB).", dest.name, dest.stat().st_size / 1e6)
        paths.append(dest)

    return paths


def _download_file(url: str, dest: Path) -> None:
    """Baixa em streaming para arquivo temporário e renomeia ao final.

    O temporário evita deixar um arquivo parcial na landing se o download falhar.
    """
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=_DOWNLOAD_TIMEOUT) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
                if chunk:
                    fh.write(chunk)
    tmp.replace(dest)


def ingest_to_bronze(spark: SparkSession, months: list[str] | None = None) -> DataFrame:
    """Lê os Parquet da landing e grava em Delta na bronze com linhagem.

    Retorna o DataFrame da bronze, para inspeção ou checagem depois.
    """
    months = months or config.MONTHS
    config.ensure_dirs()

    logger.info("Lendo %d arquivos da landing para a bronze.", len(months))

    # Os Parquet da TLC mudam de schema entre meses (ex.: VendorID ora INT32, ora
    # INT64). Ler todos de uma vez com um schema só quebra na conversão. Então lemos
    # arquivo por arquivo, marcamos a linhagem (source_file vem do nome do arquivo) e
    # juntamos com unionByName. Na união, a regra WidenSetOperationTypes do Spark
    # promove o tipo menor para o maior (INT32 -> INT64) sozinha.
    per_file: list[DataFrame] = []
    for month in months:
        filename = config.source_filename(month)
        path = str(config.LANDING_DIR / filename)
        df_file = (
            spark.read.parquet(path)
            .withColumn("source_file", F.lit(filename))
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )
        per_file.append(df_file)

    df = functools.reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True), per_file
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(config.BRONZE_PATH)
    )
    logger.info("Bronze gravada em %s", config.BRONZE_PATH)

    return spark.read.format("delta").load(config.BRONZE_PATH)


def run() -> None:
    """Entrypoint da Fase 1: download + ingestão na bronze, com resumo no log."""
    download_to_landing()
    spark = get_spark()
    bronze = ingest_to_bronze(spark)

    total = bronze.count()
    logger.info("Bronze: %d linhas ingeridas.", total)
    logger.info("Linhas por arquivo de origem:")
    (
        bronze.groupBy("source_file")
        .count()
        .orderBy("source_file")
        .show(truncate=False)
    )
    spark.stop()


if __name__ == "__main__":
    run()

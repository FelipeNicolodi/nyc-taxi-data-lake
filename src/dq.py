"""Data quality gates entre as camadas do medallion.

Cada gate recebe um DataFrame e checa um conjunto de regras. Se alguma regra é
violada, lança `DataQualityError` e interrompe o pipeline, evitando que um dado ruim passe
para a camada seguinte. Os gates também devolvem as contagens checadas, que
ficam registradas no log.

O gate só valida, não corrige. Quem limpa os dados é o `transform.py`.
Ou seja, se um gate falha, o problema está na transformação
anterior, não em um filtro que faltou aqui.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config

logger = logging.getLogger("dq")

# Limites que vieram da análise exploratória da bronze.
DATE_LOWER = "2023-01-01"   # inclusivo
DATE_UPPER = "2023-06-01"   # exclusivo (jan a mai/2023)
PASSENGER_MAX = 9           # maior valor que ainda faz sentido (vans); acima disso é erro


class DataQualityError(Exception):
    """Levantada quando uma regra de qualidade é violada."""


def _fail(rule: str, count: int) -> None:
    msg = f"DQ gate falhou: {rule} ({count} registros violam a regra)."
    logger.error(msg)
    raise DataQualityError(msg)


def gate_bronze(df: DataFrame) -> dict[str, int]:
    """Valida a saída da ingestão (bronze).

    Regras:
      - colunas obrigatórias + linhagem presentes no schema;
      - contagem de linhas > 0;
      - linhagem (source_file, ingestion_timestamp) não nula.
    """
    logger.info("Executando gate bronze...")
    expected_cols = set(config.REQUIRED_COLUMNS) | {"source_file", "ingestion_timestamp"}
    missing = expected_cols - set(df.columns)
    if missing:
        _fail(f"colunas ausentes no schema: {sorted(missing)}", len(missing))

    total = df.count()
    if total == 0:
        _fail("bronze vazia", 0)

    null_lineage = df.filter(
        F.col("source_file").isNull() | F.col("ingestion_timestamp").isNull()
    ).count()
    if null_lineage > 0:
        _fail("linhagem nula (source_file/ingestion_timestamp)", null_lineage)

    logger.info("Gate bronze OK: %d linhas, linhagem íntegra.", total)
    return {"rows": total}


def gate_silver(df: DataFrame) -> dict[str, int]:
    """Valida a saída da conformação (silver).

    Espera que a limpeza do transform.py já tenha sido aplicada. Regras:
      - 5 colunas obrigatórias presentes e não totalmente nulas;
      - sem total_amount negativo;
      - pickup < dropoff (sem intervalos inválidos);
      - datas dentro de jan–mai/2023;
      - passenger_count, quando preenchido, dentro do range plausível [0, PASSENGER_MAX];
      - sem duplicatas na chave de negócio.
    """
    logger.info("Executando gate silver...")

    missing = set(config.REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        _fail(f"colunas obrigatórias ausentes: {sorted(missing)}", len(missing))

    total = df.count()
    if total == 0:
        _fail("silver vazia", 0)

    # Colunas obrigatórias não podem estar 100% nulas.
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in config.REQUIRED_COLUMNS]
    ).first()
    for col in config.REQUIRED_COLUMNS:
        if null_counts[col] == total:
            _fail(f"coluna obrigatória totalmente nula: {col}", total)

    # Checagens de limpeza (têm que dar zero depois do transform).
    checks = {
        "total_amount negativo": F.col("total_amount") < 0,
        "pickup >= dropoff": F.col("tpep_pickup_datetime") >= F.col("tpep_dropoff_datetime"),
        "data fora de jan-mai/2023": (
            (F.col("tpep_pickup_datetime") < F.lit(DATE_LOWER).cast("timestamp"))
            | (F.col("tpep_pickup_datetime") >= F.lit(DATE_UPPER).cast("timestamp"))
        ),
        "passenger_count fora de [0, %d]" % PASSENGER_MAX: (
            F.col("passenger_count").isNotNull()
            & ((F.col("passenger_count") < 0) | (F.col("passenger_count") > PASSENGER_MAX))
        ),
    }
    violations = df.select(
        [F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in checks.items()]
    ).first()
    for name in checks:
        if violations[name] and violations[name] > 0:
            _fail(name, violations[name])

    # Unicidade da chave de negócio.
    distinct = df.select(*BUSINESS_KEY).distinct().count()
    dup = total - distinct
    if dup > 0:
        _fail(f"duplicatas na chave de negócio {BUSINESS_KEY}", dup)

    logger.info("Gate silver OK: %d linhas, todas as regras passaram.", total)
    return {"rows": total}


# Chave de negócio para dedup: identifica uma corrida única.
BUSINESS_KEY = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "total_amount",
]

"""Configuração central do pipeline: paths das camadas, meses e URLs da fonte.

Junta as constantes em um lugar só para que ingestion, transform e gold sejam
idempotentes e fáceis de parametrizar. Os paths saem de DATA_ROOT, que pode ser
trocado por variável de ambiente. Assim o mesmo código roda local, em Docker
(volume montado) ou no Databricks (Volumes) sem mudança.
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Fonte: NYC TLC yellow taxi, janeiro a maio de 2023 (Parquet via CloudFront).
# ---------------------------------------------------------------------------
SOURCE_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
)

# Meses que o enunciado pede: jan a mai/2023.
MONTHS: list[str] = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]


def source_url(month: str) -> str:
    """URL oficial do arquivo Parquet de um mês (ex.: '2023-01')."""
    return SOURCE_URL_TEMPLATE.format(month=month)


def source_filename(month: str) -> str:
    """Nome do arquivo original como salvo na landing zone."""
    return f"yellow_tripdata_{month}.parquet"


# ---------------------------------------------------------------------------
# Data lake: raiz e camadas.
# Por padrão usa a pasta ./data; dá pra trocar pela variável de ambiente DATA_ROOT.
# ---------------------------------------------------------------------------
DATA_ROOT = Path(os.environ.get("DATA_ROOT", Path(__file__).resolve().parent.parent / "data"))

LANDING_DIR = DATA_ROOT / "landing"   # Parquet originais (1:1 com a fonte)
BRONZE_DIR = DATA_ROOT / "bronze"     # Delta, ingestão 1:1 + metadados de linhagem
SILVER_DIR = DATA_ROOT / "silver"     # Delta, limpo e conformado (DQ gates)
GOLD_DIR = DATA_ROOT / "gold"         # Delta, camada de consumo (fato + marts)

# Nomes lógicos das tabelas Delta por camada.
BRONZE_TABLE = "yellow_tripdata"
SILVER_TABLE = "yellow_tripdata"
GOLD_FACT_TABLE = "fact_trips"
GOLD_MART_Q1_TABLE = "agg_avg_total_amount_by_month"      # responde a Q1
GOLD_MART_Q2_TABLE = "agg_avg_passengers_by_hour_may"     # responde a Q2

# Path físico das tabelas Delta (cada camada tem sua tabela em subpasta).
BRONZE_PATH = str(BRONZE_DIR / BRONZE_TABLE)
SILVER_PATH = str(SILVER_DIR / SILVER_TABLE)
GOLD_FACT_PATH = str(GOLD_DIR / GOLD_FACT_TABLE)
GOLD_MART_Q1_PATH = str(GOLD_DIR / GOLD_MART_Q1_TABLE)
GOLD_MART_Q2_PATH = str(GOLD_DIR / GOLD_MART_Q2_TABLE)

# Colunas obrigatórias na camada de consumo (do enunciado).
REQUIRED_COLUMNS: list[str] = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def ensure_dirs() -> None:
    """Cria a estrutura de pastas do data lake se ainda não existir."""
    for path in (LANDING_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        path.mkdir(parents=True, exist_ok=True)

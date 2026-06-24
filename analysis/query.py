"""Atalho para explorar as camadas Delta em SQL.

Registra como views temporárias as tabelas que já existem no data lake
(bronze, silver, gold) e roda SQL em cima delas. É pensado para quem prefere
SQL a PySpark. A view só aparece se a tabela Delta já tiver sido criada por uma
etapa anterior do pipeline.

Uso (dentro do Docker):

    # Query única passada na linha de comando:
    docker compose run --rm pipeline python -m analysis.query "SELECT COUNT(*) FROM bronze"

    # Modo interativo (digite SQL, ENTER para rodar, 'exit' para sair):
    docker compose run --rm pipeline python -m analysis.query
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src import config
from src.spark import get_spark

# Mapeia nome da view -> path da tabela Delta de cada camada.
_VIEWS = {
    "bronze": config.BRONZE_PATH,
    "silver": config.SILVER_PATH,
    "gold_fact": config.GOLD_FACT_PATH,
}


def register_views(spark: SparkSession) -> list[str]:
    """Registra como temp view cada camada Delta já existente. Retorna as criadas."""
    registered: list[str] = []
    for name, path in _VIEWS.items():
        if Path(path, "_delta_log").exists():
            spark.read.format("delta").load(path).createOrReplaceTempView(name)
            registered.append(name)
    return registered


def run_query(spark: SparkSession, sql: str) -> None:
    """Executa um SELECT e imprime o resultado (até 50 linhas, sem truncar)."""
    spark.sql(sql).show(50, truncate=False)


def _interactive(spark: SparkSession) -> None:
    print("Modo interativo. Digite SQL e ENTER. 'exit' ou Ctrl-D para sair.")
    while True:
        try:
            sql = input("sql> ").strip()
        except EOFError:
            break
        if not sql:
            continue
        if sql.lower() in {"exit", "quit"}:
            break
        try:
            run_query(spark, sql)
        except Exception as exc:  # noqa: BLE001 - exploração: mostrar erro e seguir
            print(f"[erro] {exc}")


def main() -> None:
    spark = get_spark("nyc-taxi-query")
    views = register_views(spark)
    if not views:
        print("Nenhuma camada Delta encontrada. Rode a ingestão primeiro "
              "(docker compose run --rm pipeline).")
        spark.stop()
        return

    print(f"Views disponíveis: {', '.join(views)}")

    if len(sys.argv) > 1:
        run_query(spark, " ".join(sys.argv[1:]))
    else:
        _interactive(spark)

    spark.stop()


if __name__ == "__main__":
    main()

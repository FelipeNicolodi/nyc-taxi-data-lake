"""Q1: média de valor total (total_amount) recebido em um mês, considerando todos
os yellow táxis da frota.

A interpretação que adotamos: média de `total_amount` por corrida, agrupada por
mês, ou seja, uma média para cada mês de jan a mai/2023. A outra leitura possível
(receita total no mês) vai junto, só como contexto.

O que já foi tratado na silver: o `total_amount` negativo (estornos) saiu, e o
escopo é yellow taxi, como na ingestão.

Roda em cima da fato gold (`fact_trips`) com Spark SQL.
"""

from __future__ import annotations

from src.spark import get_spark
from src import config

QUERY = """
    SELECT
        pickup_month,
        ROUND(AVG(total_amount), 2) AS avg_total_amount_per_trip,
        COUNT(*)                    AS trips,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM fact_trips
    GROUP BY pickup_month
    ORDER BY pickup_month
"""


def main() -> None:
    spark = get_spark("nyc-taxi-q1")
    spark.read.format("delta").load(config.GOLD_FACT_PATH).createOrReplaceTempView(
        "fact_trips"
    )

    print("\n=== Q1: média de total_amount por mês (por corrida), yellow taxi ===")
    print(QUERY)
    spark.sql(QUERY).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

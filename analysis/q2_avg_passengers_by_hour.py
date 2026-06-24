"""Q2: média de passageiros (passenger_count) por cada hora do dia que pegaram
táxi no mês de maio, considerando todos os táxis da frota.

Sobre o escopo: lemos "todos os táxis da frota" como yellow taxi, que é o que a
ingestão do case cobre (as colunas obrigatórias são de yellow: `tpep_*`). Incluir
green (`lpep_*`) ou FHV exigiria outras fontes, fora do escopo.

"Média de passageiros" só faz sentido com passageiro registrado, então deixamos de
fora `passenger_count` nulo e zero (que ficam na silver para a Q1).

A hora sai de `tpep_pickup_datetime` em UTC (o fuso é fixo na SparkSession, para o
resultado bater em qualquer máquina). Roda em cima da fato gold com Spark SQL.
"""

from __future__ import annotations

from src.spark import get_spark
from src import config

QUERY = """
    SELECT
        pickup_hour,
        ROUND(AVG(passenger_count), 3) AS avg_passenger_count,
        COUNT(*)                       AS trips
    FROM fact_trips
    WHERE pickup_month = '2023-05'
      AND passenger_count IS NOT NULL
      AND passenger_count > 0
    GROUP BY pickup_hour
    ORDER BY pickup_hour
"""


def main() -> None:
    spark = get_spark("nyc-taxi-q2")
    spark.read.format("delta").load(config.GOLD_FACT_PATH).createOrReplaceTempView(
        "fact_trips"
    )

    print("\n=== Q2: média de passageiros por hora do dia, maio/2023, yellow taxi ===")
    print(QUERY)
    spark.sql(QUERY).show(24, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

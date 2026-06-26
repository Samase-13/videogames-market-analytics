"""
02_pipeline_batch.py
Pipeline Batch — Cálculo de 8 KPIs estratégicos con Apache Spark.
Lee CSV desde HDFS, limpia datos y guarda resultados en Parquet particionado.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import sys

HDFS_MASTER = "hdfs://10.242.175.212:9000"
INPUT_PATH  = f"{HDFS_MASTER}/data/raw/videogames_synthetic.csv"
OUTPUT_BASE = f"{HDFS_MASTER}/data/curated/videogames"

def create_spark_session():
    return (SparkSession.builder
            .appName("VideoGames_Batch_KPIs")
            .master("yarn")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "2")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())

def load_and_clean(spark):
    print("Cargando datos desde HDFS...")
    df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
    df = (df
          .filter(F.col("estimated_revenue_usd") > 0)
          .withColumn("obs_date", F.to_date("obs_date", "yyyy-MM-dd"))
          .withColumn("year_month", F.date_format("obs_date", "yyyy-MM"))
          .withColumn("current_price_usd",      F.col("current_price_usd").cast(DoubleType()))
          .withColumn("discount_pc",             F.col("discount_pc").cast(DoubleType()))
          .withColumn("concurrent_players",      F.col("concurrent_players").cast(IntegerType()))
          .withColumn("hype_score",              F.col("hype_score").cast(DoubleType()))
          .withColumn("estimated_revenue_usd",   F.col("estimated_revenue_usd").cast(DoubleType()))
          .dropna(subset=["platform", "genre", "estimated_revenue_usd"]))
    print(f"Registros limpios: {df.count():,}")
    return df

def calculate_kpis(df):
    kpis = {}

    # KPI 1 — Ingresos totales por plataforma
    kpis["kpi1_revenue_platform"] = (
        df.groupBy("platform")
          .agg(F.round(F.sum("estimated_revenue_usd"), 2).alias("total_revenue_usd"))
          .orderBy(F.desc("total_revenue_usd"))
    )

    # KPI 2 — Ingresos totales por género
    kpis["kpi2_revenue_genre"] = (
        df.groupBy("genre")
          .agg(F.round(F.sum("estimated_revenue_usd"), 2).alias("total_revenue_usd"))
          .orderBy(F.desc("total_revenue_usd"))
    )

    # KPI 3 — Top 10 géneros por jugadores concurrentes
    kpis["kpi3_top_genres_ccu"] = (
        df.groupBy("genre")
          .agg(F.round(F.avg("concurrent_players"), 0).alias("avg_concurrent_players"))
          .orderBy(F.desc("avg_concurrent_players"))
          .limit(10)
    )

    # KPI 4 — Promedio de jugadores concurrentes por plataforma
    kpis["kpi4_avg_ccu_platform"] = (
        df.groupBy("platform")
          .agg(F.round(F.avg("concurrent_players"), 0).alias("avg_concurrent_players"))
          .orderBy(F.desc("avg_concurrent_players"))
    )

    # KPI 5 — Precio promedio por plataforma
    kpis["kpi5_avg_price_platform"] = (
        df.groupBy("platform")
          .agg(F.round(F.avg("current_price_usd"), 2).alias("avg_price_usd"))
          .orderBy(F.desc("avg_price_usd"))
    )

    # KPI 6 — Tasa de descuento efectiva (solo juegos en oferta)
    kpis["kpi6_discount_rate"] = (
        df.filter(F.col("is_on_sale") == 1)
          .groupBy("platform")
          .agg(F.round(F.avg("discount_pc"), 2).alias("avg_discount_pct"))
          .orderBy(F.desc("avg_discount_pct"))
    )

    # KPI 7 — Diferencia de revenue entre Early Access y Full Release
    kpis["kpi7_early_vs_full"] = (
        df.groupBy("is_early_access")
          .agg(
              F.round(F.avg("estimated_revenue_usd"), 2).alias("avg_revenue_usd"),
              F.round(F.sum("estimated_revenue_usd"), 2).alias("total_revenue_usd"),
              F.count("*").alias("num_records")
          )
    )

    # KPI 8 — Tendencia de ingresos mensual
    kpis["kpi8_monthly_trend"] = (
        df.groupBy("year_month")
          .agg(F.round(F.sum("estimated_revenue_usd"), 2).alias("monthly_revenue_usd"))
          .orderBy("year_month")
    )

    return kpis

def save_kpis(df_raw, kpis):
    print("Guardando datos curados particionados por plataforma...")
    (df_raw.write
           .mode("overwrite")
           .partitionBy("platform")
           .parquet(f"{OUTPUT_BASE}/raw_partitioned"))

    for name, kpi_df in kpis.items():
        path = f"{OUTPUT_BASE}/{name}"
        print(f"  Guardando {name} -> {path}")
        (kpi_df.coalesce(1)
               .write
               .mode("overwrite")
               .parquet(path))

def main():
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        df = load_and_clean(spark)
        kpis = calculate_kpis(df)

        for name, kpi_df in kpis.items():
            print(f"\n--- {name} ---")
            kpi_df.show(5, truncate=False)

        save_kpis(df, kpis)
        print("\nPipeline batch completado exitosamente.")

    except Exception as e:
        print(f"Error en pipeline batch: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()

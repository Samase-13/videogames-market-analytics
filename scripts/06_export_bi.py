"""
06_export_bi.py
Exportación de KPIs unificados a CSV para Tableau / Apache Superset.
Lee todos los Parquets de KPIs, los unifica y guarda como un único CSV.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

HDFS_MASTER = "hdfs://10.242.175.212:9000"
KPI_BASE    = f"{HDFS_MASTER}/data/curated/videogames"
EXPORT_PATH = f"{HDFS_MASTER}/data/export/videogames_dashboard"

KPI_PATHS = {
    "kpi1_revenue_platform": f"{KPI_BASE}/kpi1_revenue_platform",
    "kpi2_revenue_genre":    f"{KPI_BASE}/kpi2_revenue_genre",
    "kpi3_top_genres_ccu":   f"{KPI_BASE}/kpi3_top_genres_ccu",
    "kpi4_avg_ccu_platform": f"{KPI_BASE}/kpi4_avg_ccu_platform",
    "kpi5_avg_price_platform":f"{KPI_BASE}/kpi5_avg_price_platform",
    "kpi6_discount_rate":    f"{KPI_BASE}/kpi6_discount_rate",
    "kpi7_early_vs_full":    f"{KPI_BASE}/kpi7_early_vs_full",
    "kpi8_monthly_trend":    f"{KPI_BASE}/kpi8_monthly_trend",
}

def create_spark_session():
    return (SparkSession.builder
            .appName("VideoGames_Export_BI")
            .master("yarn")
            .config("spark.executor.memory", "1g")
            .getOrCreate())

def load_kpi(spark, kpi_name, path):
    try:
        df = spark.read.parquet(path)
        df = df.withColumn("kpi_name", F.lit(kpi_name))
        # Normalizar todas las columnas numéricas a string para unión
        for col_name, dtype in df.dtypes:
            if dtype in ("double", "float", "int", "bigint", "long") and col_name != "kpi_name":
                df = df.withColumn(col_name, F.col(col_name).cast("string"))
        return df
    except Exception as e:
        print(f"  [WARN] No se pudo leer {kpi_name}: {e}")
        return None

def main():
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        print("Cargando KPIs desde HDFS...")
        all_dfs = []

        for kpi_name, path in KPI_PATHS.items():
            print(f"  Leyendo {kpi_name}...")
            df = load_kpi(spark, kpi_name, path)
            if df is not None:
                all_dfs.append(df)

        if not all_dfs:
            print("ERROR: No se encontraron KPIs para exportar.")
            sys.exit(1)

        # Unificar con unionByName (fill missing con null)
        combined = all_dfs[0]
        for df in all_dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)

        total = combined.count()
        print(f"\nTotal de filas unificadas: {total:,}")
        combined.printSchema()
        combined.show(20, truncate=False)

        # Exportar como un único CSV
        print(f"\nExportando CSV a: {EXPORT_PATH}")
        (combined.coalesce(1)
                 .write
                 .mode("overwrite")
                 .option("header", "true")
                 .csv(EXPORT_PATH))

        print("Exportación completada. Archivo listo para Tableau/Superset.")

    except Exception as e:
        print(f"Error en exportación BI: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()

"""
04_pipeline_streaming.py
Structured Streaming — Consume eventos Kafka y genera alertas de hype spikes.
Guarda resultados en HDFS en modo Append con checkpoint.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys

KAFKA_BROKER  = "10.242.175.212:9092"
TOPIC         = "eventos_juego"
HDFS_MASTER   = "hdfs://10.242.175.212:9000"
OUTPUT_PATH   = f"{HDFS_MASTER}/data/streaming/alertas_hype"
CHECKPOINT    = f"{HDFS_MASTER}/data/streaming/checkpoint"

EVENT_SCHEMA = StructType([
    StructField("timestamp",          StringType(),  True),
    StructField("platform",           StringType(),  True),
    StructField("genre",              StringType(),  True),
    StructField("concurrent_players", IntegerType(), True),
    StructField("hype_score",         DoubleType(),  True),
    StructField("current_price_usd",  DoubleType(),  True),
    StructField("is_on_sale",         IntegerType(), True),
    StructField("hype_spike",         IntegerType(), True),
])

def create_spark_session():
    return (SparkSession.builder
            .appName("VideoGames_Streaming_HypeAlerts")
            .master("yarn")
            .config("spark.executor.memory", "1g")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .getOrCreate())

def main():
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        # Leer stream desde Kafka
        raw_stream = (spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", KAFKA_BROKER)
                      .option("subscribe", TOPIC)
                      .option("startingOffsets", "latest")
                      .option("failOnDataLoss", "false")
                      .load())

        # Parsear JSON
        parsed = (raw_stream
                  .select(F.from_json(
                      F.col("value").cast("string"), EVENT_SCHEMA
                  ).alias("data"))
                  .select("data.*")
                  .withColumn("event_time", F.to_timestamp("timestamp")))

        # Filtrar solo hype spikes
        alertas = parsed.filter(F.col("hype_spike") == 1)

        # Consola (debug)
        console_query = (alertas.writeStream
                         .format("console")
                         .outputMode("append")
                         .option("truncate", False)
                         .trigger(processingTime="10 seconds")
                         .start())

        # HDFS en modo Append
        hdfs_query = (alertas.writeStream
                      .format("parquet")
                      .outputMode("append")
                      .option("path", OUTPUT_PATH)
                      .option("checkpointLocation", CHECKPOINT)
                      .trigger(processingTime="30 seconds")
                      .start())

        print(f"Streaming iniciado. Escuchando tópico '{TOPIC}' en {KAFKA_BROKER}")
        print(f"Alertas guardadas en: {OUTPUT_PATH}")
        print("Presiona Ctrl+C para detener.\n")

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print("\nStreaming detenido por el usuario.")
    except Exception as e:
        print(f"Error en pipeline streaming: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()

# Diagrama de Arquitectura

Coloca aquí el archivo `diagrama_arquitectura.png` con el diagrama de la arquitectura Lambda.

## Componentes

```
[Kafka Producer] → [Kafka Broker :9092] → [Spark Streaming] → [HDFS /streaming/alertas]
                                                                        ↓
[CSV Raw Data]   → [HDFS /data/raw]    → [Spark Batch]     → [HDFS /data/curated Parquet]
                                                                        ↓
                                                              [Spark MLlib Model]
                                                                        ↓
                                                              [CSV Export → Tableau/Superset]
```

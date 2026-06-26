# Video Game Market Analytics — Lambda Architecture

Arquitectura Lambda completa para análisis del mercado de videojuegos desplegada sobre un clúster Hadoop 3.3.6 con ZeroTier.

## Arquitectura

```
                    ┌─────────────────────────────────────────────┐
                    │           CAPA DE VELOCIDAD (Speed Layer)    │
                    │  Kafka Producer → Kafka :9092 → Spark        │
                    │  Streaming → HDFS /streaming/alertas_hype    │
                    └─────────────────────────────────────────────┘
                                         │
┌──────────────┐    ┌────────────────────▼────────────────────────┐
│  Dataset CSV │───▶│          CAPA BATCH (Batch Layer)            │
│  5.6M rows   │    │  HDFS /data/raw → Spark Batch → Parquet      │
└──────────────┘    │  Particionado por platform                   │
                    └─────────────────────────────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │         CAPA DE SERVICIO (Serving Layer)     │
                    │  8 KPIs Parquet + ML Model + CSV Export      │
                    │  → Tableau / Apache Superset                 │
                    └─────────────────────────────────────────────┘
```

## Infraestructura ZeroTier

| Nodo | Rol | IP ZeroTier |
|------|-----|-------------|
| master | NameNode + ResourceManager | 10.242.175.212 |
| esclavo1 | DataNode + NodeManager | 10.242.135.39 |
| esclavo2 | DataNode + NodeManager | 10.242.214.123 |

**Stack:** Hadoop 3.3.6 · Spark 3.5.0 · Kafka 3.7.0 · Python 3.11 · Parquet

## Dataset

- **Fuente:** Video Game Market Price and Revenue (Kaggle)
- **Archivo principal:** `vg_market_analytics.csv` (~1.1 GB, 5.6M registros)
- **Columnas clave:** `obs_date`, `platform`, `genre`, `current_price_usd`, `discount_pc`, `is_on_sale`, `concurrent_players`, `hype_score`, `estimated_revenue_usd`, `is_early_access`, `holiday_sale`

## Estructura del Repositorio

```
videogames-market-analytics/
├── README.md
├── docs/
│   └── diagrama_arquitectura.png
├── scripts/
│   ├── 01_generate_dataset.py      # Generador sintético 100k registros
│   ├── 02_pipeline_batch.py        # 8 KPIs con Spark (Batch Layer)
│   ├── 03_productor_kafka.py       # Simulador eventos CCU tiempo real
│   ├── 04_pipeline_streaming.py    # Alertas hype spikes (Speed Layer)
│   ├── 05_train_ml_model.py        # Regresión revenue con MLlib
│   └── 06_export_bi.py             # Exportación CSV para Tableau/Superset
├── config/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── yarn-site.xml
│   ├── workers
│   └── server.properties           # Kafka
└── sql/
    └── kpi_definitions.sql         # Definición lógica de los 8 KPIs
```

## Despliegue

### 1. Subir dataset a HDFS
```bash
# Desde el nodo master
hdfs dfs -mkdir -p /data/raw
hdfs dfs -put vg_market_analytics.csv /data/raw/videogames_synthetic.csv
hdfs dfs -ls /data/raw/
```

### 2. Generar dataset sintético (alternativa)
```bash
python3 scripts/01_generate_dataset.py
hdfs dfs -put data/videogames_synthetic.csv /data/raw/
```

### 3. Pipeline Batch — Calcular 8 KPIs
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2 \
  scripts/02_pipeline_batch.py
```

### 4. Productor Kafka (ventana separada)
```bash
# Instalar dependencia
pip3 install kafka-python

# Iniciar productor
python3 scripts/03_productor_kafka.py
```

### 5. Pipeline Streaming
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  scripts/04_pipeline_streaming.py
```

### 6. Entrenamiento del Modelo ML
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2 \
  scripts/05_train_ml_model.py
```

### 7. Exportar KPIs para BI
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 1g \
  scripts/06_export_bi.py

# Descargar CSV desde HDFS
hdfs dfs -getmerge /data/export/videogames_dashboard videogames_dashboard.csv
```

## 8 KPIs Estratégicos

| # | KPI | Propósito de Negocio |
|---|-----|----------------------|
| 1 | Ingresos totales por plataforma | Identificar ecosistemas más rentables |
| 2 | Ingresos totales por género | Detectar géneros con mayor ROI |
| 3 | Top 10 géneros por CCU | Medir engagement activo del mercado |
| 4 | Promedio CCU por plataforma | Evaluar tamaño de base de usuarios |
| 5 | Precio promedio por plataforma | Análisis de estrategia de precios |
| 6 | Tasa de descuento efectiva | Medir agresividad de ofertas |
| 7 | Revenue Early Access vs Full Release | Validar viabilidad del modelo EA |
| 8 | Tendencia de ingresos mensual | Detectar estacionalidad del mercado |

## Interfaces Web del Clúster

| Servicio | URL |
|----------|-----|
| HDFS NameNode | http://10.242.175.212:9870 |
| YARN ResourceManager | http://10.242.175.212:8088 |
| Kafka | 10.242.175.212:9092 |

## Monitoreo HDFS

```bash
# Ver estructura de datos en HDFS
hdfs dfs -ls -R /data/

# Ver tamaño de directorios
hdfs dfs -du -h /data/curated/videogames/

# Salud del clúster
hdfs dfsadmin -report
yarn node -list
```

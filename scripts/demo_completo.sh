#!/bin/bash
# =============================================================================
# demo_completo.sh — Demostración Lambda Architecture completa
# Ejecutar como: su - hadoop  (y luego bash /home/vboxuser/videogames-market-analytics/scripts/demo_completo.sh)
# =============================================================================

set -e
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC} $1"; }
ok()    { echo -e "${GREEN}[OK]${NC}   $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ─── PASO 0: Verificar que corremos como hadoop ────────────────────────────
if [ "$(whoami)" != "hadoop" ]; then
  error "Este script debe correr como usuario 'hadoop'. Ejecuta: su - hadoop"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "   🎮 VIDEO GAME MARKET ANALYTICS — Lambda Architecture"
echo "       Hadoop 3.3.6 · Spark 3.5.0 · Kafka 3.7.0"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ─── PASO 1: VERIFICACIÓN DEL CLÚSTER ─────────────────────────────────────
echo -e "${CYAN}╔══ CAPA 0: VERIFICACIÓN DEL CLÚSTER ══╗${NC}"

info "Verificando HDFS..."
hdfs dfsadmin -report 2>/dev/null | grep -E "(Live datanodes|Dead datanodes|Configured Capacity)" || warn "HDFS no responde"

info "Verificando YARN..."
yarn node -list 2>/dev/null | grep -E "(Total Nodes|RUNNING)" || warn "YARN no responde"

info "Iniciando clúster si no está corriendo..."
NAMENODE_PID=$(jps 2>/dev/null | grep NameNode | awk '{print $1}')
if [ -z "$NAMENODE_PID" ]; then
  warn "Clúster detenido. Iniciando..."
  /opt/hadoop/sbin/start-dfs.sh
  /opt/hadoop/sbin/start-yarn.sh
  sleep 5
  ok "Clúster iniciado"
else
  ok "Clúster ya está corriendo (NameNode PID: $NAMENODE_PID)"
fi

echo ""
info "Nodos activos:"
hdfs dfsadmin -report 2>/dev/null | grep -A2 "Live datanodes"
echo ""

# ─── PASO 2: INGESTA DE DATOS EN HDFS ─────────────────────────────────────
echo -e "${CYAN}╔══ CAPA BATCH: INGESTA DE DATOS EN HDFS ══╗${NC}"

CSV_LOCAL="/home/vboxuser/videogames-market-analytics/data/videogames_synthetic.csv"
CSV_KAGGLE="/home/vboxuser/Downloads/vg_market_analytics.csv"
HDFS_RAW="hdfs://10.242.175.212:9000/data/raw"

# Crear directorios HDFS
info "Creando estructura de directorios en HDFS..."
hdfs dfs -mkdir -p /data/raw /data/curated /data/streaming /data/export /data/models 2>/dev/null || true
ok "Directorios HDFS listos"

# Verificar si ya existe el dataset
if hdfs dfs -test -e /data/raw/videogames_synthetic.csv 2>/dev/null; then
  ok "Dataset ya en HDFS — omitiendo ingesta"
else
  # Prioridad: dataset Kaggle real → sintético
  if [ -f "$CSV_KAGGLE" ]; then
    info "Subiendo dataset real de Kaggle ($CSV_KAGGLE) a HDFS..."
    hdfs dfs -put "$CSV_KAGGLE" /data/raw/videogames_synthetic.csv
    ok "Dataset Kaggle subido a HDFS"
  elif [ -f "$CSV_LOCAL" ]; then
    info "Subiendo dataset sintético a HDFS..."
    hdfs dfs -put "$CSV_LOCAL" /data/raw/videogames_synthetic.csv
    ok "Dataset sintético subido a HDFS"
  else
    info "Generando dataset sintético (100k registros)..."
    python3 /home/vboxuser/videogames-market-analytics/scripts/01_generate_dataset.py
    hdfs dfs -put "$CSV_LOCAL" /data/raw/videogames_synthetic.csv
    ok "Dataset generado y subido a HDFS"
  fi
fi

info "Verificando dataset en HDFS:"
hdfs dfs -ls /data/raw/
echo ""

# ─── PASO 3: PIPELINE BATCH — 8 KPIs ──────────────────────────────────────
echo -e "${CYAN}╔══ CAPA BATCH: SPARK SQL — 8 KPIs ══╗${NC}"

if hdfs dfs -test -d /data/curated/videogames 2>/dev/null; then
  warn "KPIs ya calculados. Eliminando para recalcular..."
  hdfs dfs -rm -r /data/curated/videogames 2>/dev/null || true
fi

info "Ejecutando pipeline batch (Spark + YARN)..."
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  /home/vboxuser/videogames-market-analytics/scripts/02_pipeline_batch.py
ok "Pipeline batch completado — 8 KPIs en HDFS /data/curated/videogames/"

echo ""
info "Estructura de KPIs en HDFS:"
hdfs dfs -ls /data/curated/videogames/ 2>/dev/null
echo ""

# ─── PASO 4: EXPORTAR KPIs A CSV ──────────────────────────────────────────
echo -e "${CYAN}╔══ CAPA SERVING: EXPORTAR KPIs PARA DASHBOARD ══╗${NC}"

info "Exportando KPIs a CSV para Streamlit..."
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 1g \
  /home/vboxuser/videogames-market-analytics/scripts/06_export_bi.py
ok "Exportación completada en HDFS /data/export/videogames_dashboard"

# Descargar CSV al nodo master para que Streamlit pueda leerlos localmente
info "Descargando KPIs al nodo master..."
mkdir -p /tmp/vg_dashboard_cache
for kpi in kpi1_revenue_platform kpi2_revenue_genre kpi3_top_genres_ccu kpi4_avg_ccu_platform kpi5_avg_price_platform kpi6_discount_rate kpi7_early_vs_full kpi8_monthly_trend; do
  hdfs dfs -getmerge /data/curated/videogames/${kpi} /tmp/vg_dashboard_cache/${kpi}.csv 2>/dev/null && ok "  → ${kpi}.csv descargado" || warn "  → ${kpi} no encontrado"
done

echo ""

# ─── PASO 5: MODELO ML ────────────────────────────────────────────────────
echo -e "${CYAN}╔══ CAPA BATCH: SPARK MLlib — REGRESIÓN ══╗${NC}"

read -p "¿Entrenar modelo ML (puede tardar 2-3 min)? [s/N]: " train_ml
if [[ "$train_ml" =~ ^[sS]$ ]]; then
  info "Entrenando modelo de regresión con CrossValidator..."
  spark-submit \
    --master yarn \
    --deploy-mode client \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    /home/vboxuser/videogames-market-analytics/scripts/05_train_ml_model.py
  ok "Modelo ML guardado en HDFS /data/models/regresion_videogames"
else
  warn "Saltando entrenamiento ML"
fi

echo ""

# ─── PASO 6: KAFKA + STREAMING (OPCIONAL) ─────────────────────────────────
echo -e "${CYAN}╔══ CAPA VELOCIDAD: KAFKA + SPARK STREAMING ══╗${NC}"

read -p "¿Iniciar Kafka y Streaming (en background)? [s/N]: " start_kafka
if [[ "$start_kafka" =~ ^[sS]$ ]]; then
  info "Iniciando Zookeeper..."
  /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
  sleep 3
  info "Iniciando Kafka broker..."
  /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
  sleep 5
  ok "Kafka listo en 10.242.175.212:9092"

  info "Creando topic eventos_juego..."
  /opt/kafka/bin/kafka-topics.sh --create --topic eventos_juego \
    --bootstrap-server 10.242.175.212:9092 \
    --partitions 3 --replication-factor 1 2>/dev/null || true

  info "Iniciando Productor Kafka en background..."
  nohup python3 /home/vboxuser/videogames-market-analytics/scripts/03_productor_kafka.py \
    > /tmp/kafka_producer.log 2>&1 &
  ok "Productor corriendo (PID: $!). Log: /tmp/kafka_producer.log"

  info "Iniciando Spark Streaming en background..."
  nohup spark-submit \
    --master yarn --deploy-mode client \
    --executor-memory 1g \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /home/vboxuser/videogames-market-analytics/scripts/04_pipeline_streaming.py \
    > /tmp/spark_streaming.log 2>&1 &
  ok "Streaming corriendo (PID: $!). Log: /tmp/spark_streaming.log"
else
  warn "Saltando Kafka/Streaming"
fi

echo ""

# ─── PASO 7: DASHBOARD STREAMLIT ──────────────────────────────────────────
echo -e "${CYAN}╔══ CAPA SERVING: STREAMLIT DASHBOARD ══╗${NC}"

export PATH="$HOME/.local/bin:/home/vboxuser/.local/bin:$PATH"
STREAMLIT_BIN=$(which streamlit 2>/dev/null || echo "/home/vboxuser/.local/bin/streamlit")

if [ -x "$STREAMLIT_BIN" ]; then
  info "Lanzando dashboard Streamlit en http://10.242.175.212:8501 ..."
  cd /home/vboxuser/videogames-market-analytics
  nohup "$STREAMLIT_BIN" run scripts/07_dashboard_streamlit.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true \
    > /tmp/streamlit.log 2>&1 &
  STREAMLIT_PID=$!
  sleep 3
  if kill -0 $STREAMLIT_PID 2>/dev/null; then
    ok "Dashboard disponible en: http://10.242.175.212:8501"
  else
    warn "Streamlit falló. Ver /tmp/streamlit.log"
  fi
else
  warn "Streamlit no encontrado. Instalar con: pip install streamlit plotly pandas"
fi

# ─── RESUMEN FINAL ────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════"
echo -e "${GREEN}   DEMOSTRACIÓN COMPLETADA${NC}"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  📊 HDFS NameNode:       http://10.242.175.212:9870"
echo "  📦 YARN ResourceManager: http://10.242.175.212:8088"
echo "  🎮 Streamlit Dashboard:  http://10.242.175.212:8501"
echo ""
echo "  Logs:"
echo "    Kafka Producer:  /tmp/kafka_producer.log"
echo "    Spark Streaming: /tmp/spark_streaming.log"
echo "    Streamlit:       /tmp/streamlit.log"
echo ""
echo "  Para detener todo:"
echo "    pkill -f streamlit"
echo "    pkill -f '04_pipeline_streaming'"
echo "    pkill -f '03_productor_kafka'"
echo "    /opt/kafka/bin/kafka-server-stop.sh"
echo "    /opt/kafka/bin/zookeeper-server-stop.sh"
echo ""

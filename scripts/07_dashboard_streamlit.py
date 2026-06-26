"""
07_dashboard_streamlit.py
Dashboard interactivo en Streamlit para los 8 KPIs de Video Game Market Analytics.
Lee los CSV exportados desde HDFS o los genera localmente para demo.

Instalar: pip install streamlit pandas plotly
Ejecutar:  streamlit run scripts/07_dashboard_streamlit.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import subprocess
import os

# ─── Configuración de página ───────────────────────────────────────────────
st.set_page_config(
    page_title="Video Game Market Analytics",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded",
)

HDFS_MASTER = "hdfs://10.242.175.212:9000"
LOCAL_CACHE = "/tmp/vg_dashboard_cache"
os.makedirs(LOCAL_CACHE, exist_ok=True)

# ─── Paleta de colores ─────────────────────────────────────────────────────
COLORS = px.colors.qualitative.Vivid

# ─── Helpers ───────────────────────────────────────────────────────────────
def hdfs_to_local(hdfs_path: str, local_name: str) -> str:
    local_path = os.path.join(LOCAL_CACHE, local_name)
    if not os.path.exists(local_path):
        try:
            subprocess.run(
                ["hdfs", "dfs", "-getmerge", hdfs_path, local_path],
                check=True, capture_output=True, timeout=60
            )
        except Exception:
            return None
    return local_path if os.path.exists(local_path) else None

def load_csv(local_path: str) -> pd.DataFrame | None:
    try:
        return pd.read_csv(local_path)
    except Exception:
        return None

def demo_data():
    """Genera datos de demostración si HDFS no está disponible."""
    platforms = ["PC", "PlayStation 5", "Xbox Series X", "Nintendo Switch", "PlayStation 4", "Mobile"]
    genres    = ["Action", "RPG", "Strategy", "Sports", "Shooter", "Adventure", "Simulation", "Horror"]
    import random, numpy as np
    random.seed(42)
    np.random.seed(42)

    kpi1 = pd.DataFrame({"platform": platforms,
                          "total_revenue_usd": np.random.uniform(5e6, 50e6, len(platforms)).round(2)})
    kpi2 = pd.DataFrame({"genre": genres,
                          "total_revenue_usd": np.random.uniform(3e6, 40e6, len(genres)).round(2)})
    kpi3 = pd.DataFrame({"genre": genres,
                          "avg_concurrent_players": np.random.randint(5000, 150000, len(genres))})
    kpi4 = pd.DataFrame({"platform": platforms,
                          "avg_concurrent_players": np.random.randint(2000, 80000, len(platforms))})
    kpi5 = pd.DataFrame({"platform": platforms,
                          "avg_price_usd": np.random.uniform(9.99, 59.99, len(platforms)).round(2)})
    kpi6 = pd.DataFrame({"platform": platforms,
                          "avg_discount_pct": np.random.uniform(10, 60, len(platforms)).round(2)})
    kpi7 = pd.DataFrame({"release_type": ["Early Access", "Full Release"],
                          "avg_revenue_usd": [120000, 280000],
                          "total_revenue_usd": [15e6, 85e6]})
    months = pd.date_range("2022-01", periods=36, freq="MS").strftime("%Y-%m").tolist()
    kpi8 = pd.DataFrame({"year_month": months,
                          "monthly_revenue_usd": np.random.uniform(2e6, 12e6, len(months)).round(2)})
    return kpi1, kpi2, kpi3, kpi4, kpi5, kpi6, kpi7, kpi8

# ─── Verificar clúster ─────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def check_cluster():
    results = {}
    try:
        r = subprocess.run(["hdfs", "dfsadmin", "-report"],
                           capture_output=True, text=True, timeout=10)
        results["hdfs"] = "Live datanodes" in r.stdout
        lines = [l for l in r.stdout.split("\n") if "Live datanodes" in l]
        results["datanodes"] = lines[0] if lines else "Desconocido"
    except Exception as e:
        results["hdfs"] = False
        results["datanodes"] = str(e)
    try:
        r = subprocess.run(["yarn", "node", "-list"],
                           capture_output=True, text=True, timeout=10)
        results["yarn"] = "Total Nodes" in r.stdout
        lines = [l for l in r.stdout.split("\n") if "Total Nodes" in l]
        results["yarn_nodes"] = lines[0] if lines else "Desconocido"
    except Exception as e:
        results["yarn"] = False
        results["yarn_nodes"] = str(e)
    return results

# ─── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/0/0e/Hadoop_logo.svg/320px-Hadoop_logo.svg.png",
             width=140)
    st.title("🎮 VG Analytics")
    st.markdown("**Lambda Architecture**")
    st.divider()

    st.subheader("⚙️ Estado del Clúster")
    cluster = check_cluster()
    st.metric("HDFS", "✅ Online" if cluster["hdfs"] else "❌ Offline")
    st.metric("YARN", "✅ Online" if cluster["yarn"] else "❌ Offline")
    if cluster["hdfs"]:
        st.info(cluster.get("datanodes", ""))
    if cluster["yarn"]:
        st.info(cluster.get("yarn_nodes", ""))

    st.divider()
    use_demo = not cluster["hdfs"]
    if use_demo:
        st.warning("⚠️ Modo DEMO\nHDFS no disponible.\nUsando datos sintéticos.")
    else:
        st.success("✅ Leyendo desde HDFS")

    st.divider()
    st.caption("Hadoop 3.3.6 · Spark 3.5.0\nKafka 3.7.0 · ZeroTier")
    st.caption(f"Master: 10.242.175.212")

# ─── Cargar datos ──────────────────────────────────────────────────────────
if use_demo:
    kpi1, kpi2, kpi3, kpi4, kpi5, kpi6, kpi7, kpi8 = demo_data()
else:
    base = f"{HDFS_MASTER}/data/curated/videogames"
    paths = {
        "kpi1": f"{base}/kpi1_revenue_platform",
        "kpi2": f"{base}/kpi2_revenue_genre",
        "kpi3": f"{base}/kpi3_top_genres_ccu",
        "kpi4": f"{base}/kpi4_avg_ccu_platform",
        "kpi5": f"{base}/kpi5_avg_price_platform",
        "kpi6": f"{base}/kpi6_discount_rate",
        "kpi7": f"{base}/kpi7_early_vs_full",
        "kpi8": f"{base}/kpi8_monthly_trend",
    }
    dfs = {}
    for k, p in paths.items():
        lp = hdfs_to_local(p, f"{k}.csv")
        dfs[k] = load_csv(lp)

    kpi1 = dfs["kpi1"] or demo_data()[0]
    kpi2 = dfs["kpi2"] or demo_data()[1]
    kpi3 = dfs["kpi3"] or demo_data()[2]
    kpi4 = dfs["kpi4"] or demo_data()[3]
    kpi5 = dfs["kpi5"] or demo_data()[4]
    kpi6 = dfs["kpi6"] or demo_data()[5]
    kpi7 = dfs["kpi7"] or demo_data()[6]
    kpi8 = dfs["kpi8"] or demo_data()[7]

# ─── Header ────────────────────────────────────────────────────────────────
st.title("🎮 Video Game Market Analytics")
st.markdown("**Arquitectura Lambda** · Hadoop 3.3.6 + Spark 3.5.0 + Kafka 3.7.0 · ZeroTier Cluster")
st.divider()

# ─── Métricas resumen ──────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
total_rev = kpi1["total_revenue_usd"].sum() if "total_revenue_usd" in kpi1.columns else 0
top_plat  = kpi1.sort_values("total_revenue_usd", ascending=False).iloc[0]["platform"] if len(kpi1) else "N/A"
top_genre = kpi2.sort_values("total_revenue_usd", ascending=False).iloc[0]["genre"] if len(kpi2) else "N/A"
avg_disc  = kpi6["avg_discount_pct"].mean() if "avg_discount_pct" in kpi6.columns else 0

col1.metric("💰 Revenue Total", f"${total_rev/1e6:.1f}M")
col2.metric("🏆 Top Plataforma", top_plat)
col3.metric("🎯 Top Género", top_genre)
col4.metric("🏷️ Descuento Promedio", f"{avg_disc:.1f}%")

st.divider()

# ─── Fila 1: KPI 1 y KPI 2 ────────────────────────────────────────────────
st.subheader("📊 KPI 1 & 2 — Ingresos por Plataforma y Género")
c1, c2 = st.columns(2)

with c1:
    fig = px.bar(kpi1.sort_values("total_revenue_usd"),
                 x="total_revenue_usd", y="platform", orientation="h",
                 title="KPI 1: Revenue Total por Plataforma",
                 labels={"total_revenue_usd": "Revenue (USD)", "platform": "Plataforma"},
                 color="total_revenue_usd", color_continuous_scale="Viridis")
    fig.update_layout(showlegend=False, height=350)
    st.plotly_chart(fig, use_container_width=True)

with c2:
    fig = px.pie(kpi2, values="total_revenue_usd", names="genre",
                 title="KPI 2: Revenue Total por Género",
                 color_discrete_sequence=COLORS)
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

# ─── Fila 2: KPI 3 y KPI 4 ────────────────────────────────────────────────
st.subheader("👥 KPI 3 & 4 — Jugadores Concurrentes")
c1, c2 = st.columns(2)

with c1:
    fig = px.bar(kpi3.sort_values("avg_concurrent_players", ascending=False),
                 x="genre", y="avg_concurrent_players",
                 title="KPI 3: Top Géneros por CCU Promedio",
                 labels={"avg_concurrent_players": "CCU Promedio", "genre": "Género"},
                 color="avg_concurrent_players", color_continuous_scale="Plasma")
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

with c2:
    fig = px.bar(kpi4.sort_values("avg_concurrent_players", ascending=False),
                 x="platform", y="avg_concurrent_players",
                 title="KPI 4: CCU Promedio por Plataforma",
                 labels={"avg_concurrent_players": "CCU Promedio", "platform": "Plataforma"},
                 color="platform", color_discrete_sequence=COLORS)
    fig.update_layout(showlegend=False, height=350)
    st.plotly_chart(fig, use_container_width=True)

# ─── Fila 3: KPI 5 y KPI 6 ────────────────────────────────────────────────
st.subheader("💲 KPI 5 & 6 — Precios y Descuentos")
c1, c2 = st.columns(2)

with c1:
    fig = px.bar(kpi5.sort_values("avg_price_usd", ascending=False),
                 x="platform", y="avg_price_usd",
                 title="KPI 5: Precio Promedio por Plataforma",
                 labels={"avg_price_usd": "Precio Promedio (USD)", "platform": "Plataforma"},
                 color="avg_price_usd", color_continuous_scale="Teal")
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

with c2:
    fig = px.bar(kpi6.sort_values("avg_discount_pct", ascending=False),
                 x="platform", y="avg_discount_pct",
                 title="KPI 6: Tasa de Descuento Efectiva por Plataforma",
                 labels={"avg_discount_pct": "Descuento Promedio (%)", "platform": "Plataforma"},
                 color="avg_discount_pct", color_continuous_scale="Reds")
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

# ─── Fila 4: KPI 7 y KPI 8 ────────────────────────────────────────────────
st.subheader("🚀 KPI 7 & 8 — Early Access vs Full Release y Tendencia Mensual")
c1, c2 = st.columns(2)

with c1:
    fig = go.Figure()
    for i, row in kpi7.iterrows():
        label = row.get("release_type",
                        "Early Access" if row.get("is_early_access", 0) == 1 else "Full Release")
        fig.add_trace(go.Bar(
            name=label,
            x=[label],
            y=[row["total_revenue_usd"]],
            text=[f"${row['total_revenue_usd']/1e6:.1f}M"],
            textposition="auto",
        ))
    fig.update_layout(title="KPI 7: Revenue Early Access vs Full Release",
                      yaxis_title="Revenue Total (USD)", height=350, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with c2:
    fig = px.line(kpi8, x="year_month", y="monthly_revenue_usd",
                  title="KPI 8: Tendencia de Ingresos Mensual",
                  labels={"monthly_revenue_usd": "Revenue (USD)", "year_month": "Mes"},
                  markers=True, line_shape="spline",
                  color_discrete_sequence=["#00CC96"])
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

# ─── Tablas de datos ───────────────────────────────────────────────────────
st.divider()
with st.expander("📋 Ver Datos Completos de los KPIs"):
    tab1, tab2, tab3, tab4 = st.tabs(["Revenue Plataforma", "Revenue Género", "CCU", "Mensual"])
    with tab1:
        st.dataframe(kpi1.style.format({"total_revenue_usd": "${:,.2f}"}), use_container_width=True)
    with tab2:
        st.dataframe(kpi2.style.format({"total_revenue_usd": "${:,.2f}"}), use_container_width=True)
    with tab3:
        st.dataframe(kpi3, use_container_width=True)
    with tab4:
        st.dataframe(kpi8.style.format({"monthly_revenue_usd": "${:,.2f}"}), use_container_width=True)

st.caption("🔄 Dashboard actualizado automáticamente · Clúster Hadoop 3.3.6 + Spark 3.5.0 + Kafka 3.7.0")

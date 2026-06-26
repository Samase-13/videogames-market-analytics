-- =============================================================================
-- kpi_definitions.sql
-- Definición lógica de los 8 KPIs estratégicos — Video Game Market Analytics
-- Ejecutar sobre la tabla: videogames_curated (datos limpios en Hive/SparkSQL)
-- =============================================================================

-- Crear vista sobre los datos curados
CREATE OR REPLACE VIEW videogames_curated AS
SELECT *
FROM parquet.`hdfs://10.242.175.212:9000/data/curated/videogames/raw_partitioned`
WHERE estimated_revenue_usd > 0;

-- -----------------------------------------------------------------------------
-- KPI 1: Ingresos totales por plataforma
-- Propósito: Identificar qué plataformas generan mayor retorno económico.
-- -----------------------------------------------------------------------------
SELECT
    platform,
    ROUND(SUM(estimated_revenue_usd), 2) AS total_revenue_usd,
    COUNT(*)                              AS num_registros
FROM videogames_curated
GROUP BY platform
ORDER BY total_revenue_usd DESC;

-- -----------------------------------------------------------------------------
-- KPI 2: Ingresos totales por género
-- Propósito: Detectar géneros más rentables para decisiones de inversión.
-- -----------------------------------------------------------------------------
SELECT
    genre,
    ROUND(SUM(estimated_revenue_usd), 2) AS total_revenue_usd,
    COUNT(*)                              AS num_registros
FROM videogames_curated
GROUP BY genre
ORDER BY total_revenue_usd DESC;

-- -----------------------------------------------------------------------------
-- KPI 3: Top 10 géneros por jugadores concurrentes promedio
-- Propósito: Medir el engagement activo por género.
-- -----------------------------------------------------------------------------
SELECT
    genre,
    ROUND(AVG(concurrent_players), 0) AS avg_concurrent_players
FROM videogames_curated
GROUP BY genre
ORDER BY avg_concurrent_players DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- KPI 4: Promedio de jugadores concurrentes por plataforma
-- Propósito: Evaluar la actividad de la base de usuarios por ecosistema.
-- -----------------------------------------------------------------------------
SELECT
    platform,
    ROUND(AVG(concurrent_players), 0) AS avg_concurrent_players,
    MAX(concurrent_players)            AS peak_concurrent_players
FROM videogames_curated
GROUP BY platform
ORDER BY avg_concurrent_players DESC;

-- -----------------------------------------------------------------------------
-- KPI 5: Precio promedio por plataforma
-- Propósito: Análisis de estrategia de precios por ecosistema.
-- -----------------------------------------------------------------------------
SELECT
    platform,
    ROUND(AVG(current_price_usd), 2) AS avg_price_usd,
    ROUND(MIN(current_price_usd), 2) AS min_price_usd,
    ROUND(MAX(current_price_usd), 2) AS max_price_usd
FROM videogames_curated
GROUP BY platform
ORDER BY avg_price_usd DESC;

-- -----------------------------------------------------------------------------
-- KPI 6: Tasa de descuento efectiva (solo juegos en oferta)
-- Propósito: Medir agresividad de descuentos y su impacto en el mercado.
-- -----------------------------------------------------------------------------
SELECT
    platform,
    ROUND(AVG(discount_pc), 2)         AS avg_discount_pct,
    COUNT(*)                            AS juegos_en_oferta,
    ROUND(SUM(estimated_revenue_usd), 2) AS revenue_en_oferta
FROM videogames_curated
WHERE is_on_sale = 1
GROUP BY platform
ORDER BY avg_discount_pct DESC;

-- -----------------------------------------------------------------------------
-- KPI 7: Diferencia de revenue entre Early Access y Full Release
-- Propósito: Evaluar si el modelo Early Access es financieramente viable.
-- -----------------------------------------------------------------------------
SELECT
    CASE WHEN is_early_access = 1 THEN 'Early Access' ELSE 'Full Release' END AS release_type,
    ROUND(AVG(estimated_revenue_usd), 2) AS avg_revenue_usd,
    ROUND(SUM(estimated_revenue_usd), 2) AS total_revenue_usd,
    COUNT(*)                              AS num_registros
FROM videogames_curated
GROUP BY is_early_access
ORDER BY total_revenue_usd DESC;

-- -----------------------------------------------------------------------------
-- KPI 8: Tendencia de ingresos mensual
-- Propósito: Identificar estacionalidad y ciclos de crecimiento del mercado.
-- -----------------------------------------------------------------------------
SELECT
    DATE_FORMAT(obs_date, 'yyyy-MM')     AS year_month,
    ROUND(SUM(estimated_revenue_usd), 2) AS monthly_revenue_usd,
    COUNT(*)                             AS num_registros,
    ROUND(AVG(concurrent_players), 0)    AS avg_ccu
FROM videogames_curated
GROUP BY DATE_FORMAT(obs_date, 'yyyy-MM')
ORDER BY year_month ASC;

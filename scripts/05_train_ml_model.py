"""
05_train_ml_model.py
Entrenamiento de modelo ML con Spark MLlib.
Regresión lineal para predecir estimated_revenue_usd.
Pipeline: VectorAssembler → StandardScaler → LinearRegression con CrossValidator (3 folds).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
import sys

HDFS_MASTER  = "hdfs://10.242.175.212:9000"
INPUT_PATH   = f"{HDFS_MASTER}/data/curated/videogames/raw_partitioned"
MODEL_PATH   = f"{HDFS_MASTER}/data/models/regresion_videogames"

FEATURES = ["concurrent_players", "hype_score", "discount_pc"]
LABEL    = "estimated_revenue_usd"

def create_spark_session():
    return (SparkSession.builder
            .appName("VideoGames_ML_RevenuePredictor")
            .master("yarn")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "2")
            .getOrCreate())

def load_data(spark):
    print("Cargando datos curados desde HDFS...")
    df = (spark.read.parquet(INPUT_PATH)
              .select(FEATURES + [LABEL])
              .filter(F.col(LABEL) > 0)
              .dropna())
    print(f"Registros para entrenamiento: {df.count():,}")
    return df

def build_pipeline():
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features_raw")
    scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                               withMean=True, withStd=True)
    lr        = LinearRegression(featuresCol="features", labelCol=LABEL,
                                 predictionCol="predicted_revenue")
    return Pipeline(stages=[assembler, scaler, lr])

def build_cross_validator(pipeline):
    param_grid = (ParamGridBuilder()
                  .addGrid(pipeline.getStages()[-1].regParam,    [0.01, 0.1])
                  .addGrid(pipeline.getStages()[-1].elasticNetParam, [0.0, 0.5])
                  .build())
    evaluator = RegressionEvaluator(
        labelCol=LABEL, predictionCol="predicted_revenue", metricName="rmse"
    )
    return CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        seed=42,
    )

def main():
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        df = load_data(spark)
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        print(f"Train: {train_df.count():,} | Test: {test_df.count():,}")

        pipeline = build_pipeline()
        cv       = build_cross_validator(pipeline)

        print("Entrenando modelo con CrossValidator (3 folds)...")
        cv_model = cv.fit(train_df)
        best_model = cv_model.bestModel

        # Evaluación
        evaluator_rmse = RegressionEvaluator(labelCol=LABEL, predictionCol="predicted_revenue", metricName="rmse")
        evaluator_r2   = RegressionEvaluator(labelCol=LABEL, predictionCol="predicted_revenue", metricName="r2")

        predictions = best_model.transform(test_df)
        rmse = evaluator_rmse.evaluate(predictions)
        r2   = evaluator_r2.evaluate(predictions)

        print(f"\n=== Resultados del Modelo ===")
        print(f"  RMSE: {rmse:,.2f}")
        print(f"  R²:   {r2:.4f}")

        lr_model = best_model.stages[-1]
        print(f"  Coeficientes: {lr_model.coefficients}")
        print(f"  Intercepto:   {lr_model.intercept:.4f}")

        # Guardar modelo
        print(f"\nGuardando modelo en: {MODEL_PATH}")
        best_model.write().overwrite().save(MODEL_PATH)
        print("Modelo guardado exitosamente.")

        predictions.select(LABEL, "predicted_revenue").show(10, truncate=False)

    except Exception as e:
        print(f"Error en entrenamiento ML: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()

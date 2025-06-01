from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, month, year, round, max, min, stddev, when, isnan, isnull
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline
from datetime import datetime
import os

# --- Configuración de Credenciales ---
def setup_credentials():
    """
    Configura las credenciales de Google Cloud de manera segura
    """
    # Opción 1: Usando variable de entorno
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if not credentials_path:
        # Opción 2: Ruta relativa al archivo de credenciales
        credentials_path = "../tet-p3-2025-credentials.json"
        if os.path.exists(credentials_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            print(f"Credenciales configuradas desde: {credentials_path}")
        else:
            print("⚠️  Advertencia: No se encontraron credenciales de GCP")
            print("   Por favor, configura GOOGLE_APPLICATION_CREDENTIALS o coloca el archivo JSON")
            return False
    else:
        print(f"Credenciales encontradas en: {credentials_path}")
    
    return True


def create_spark_session():
    """
    Crea una sesión de Spark optimizada para trabajar con GCS
    """
    return SparkSession.builder \
        .appName("WeatherAnalyticsAdvanced") \
        .config("spark.jars.packages", 
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11") \
        .config("spark.jars.excludes", 
                "com.google.guava:guava") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


# --- Configuración de rutas ---
TRUSTED_DATA_BUCKET = "gs://tet-trusted-data"
REFINED_DATA_BUCKET = "gs://tet-refined-data"

TRUSTED_INPUT_PATH = f"{TRUSTED_DATA_BUCKET}/processed_weather_locations_data/*.parquet"
DESCRIPTIVE_OUTPUT_PATH = f"{REFINED_DATA_BUCKET}/descriptive_analytics_results/"
ML_OUTPUT_PATH = f"{REFINED_DATA_BUCKET}/ml_analytics_results/"


def data_quality_check(df):
    """
    Realiza verificaciones de calidad de datos
    """
    print("\n=== VERIFICACIÓN DE CALIDAD DE DATOS ===")
    
    total_rows = df.count()
    print(f"Total de registros: {total_rows:,}")
    
    # Verificar valores nulos por columna
    print("\nValores nulos por columna:")
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        null_percentage = (null_count / total_rows) * 100
        print(f"  {column}: {null_count:,} ({null_percentage:.2f}%)")
    
    # Estadísticas básicas de temperaturas
    temp_stats = df.select(
        min("max_temp_c").alias("min_temp"),
        max("max_temp_c").alias("max_temp"),
        avg("max_temp_c").alias("avg_temp"),
        stddev("max_temp_c").alias("stddev_temp")
    ).collect()[0]
    
    print(f"\nEstadísticas de temperatura:")
    print(f"  Mínima: {temp_stats['min_temp']:.2f}°C")
    print(f"  Máxima: {temp_stats['max_temp']:.2f}°C")
    print(f"  Promedio: {temp_stats['avg_temp']:.2f}°C")
    print(f"  Desviación estándar: {temp_stats['stddev_temp']:.2f}°C")


def descriptive_analytics(spark, df):
    """
    Análisis descriptivo avanzado usando SparkSQL
    """
    print("\n=== ANÁLISIS DESCRIPTIVO CON SPARKSQL ===")
    
    # Registrar la tabla temporal
    df.createOrReplaceTempView("weather_data")
    
    # 1. Resumen mensual por ciudad
    print("\n1. Análisis mensual por ciudad...")
    monthly_analysis = spark.sql("""
        SELECT 
            city_name,
            country,
            YEAR(weather_date) as year,
            MONTH(weather_date) as month,
            ROUND(AVG(max_temp_c), 2) as avg_max_temp,
            ROUND(AVG(min_temp_c), 2) as avg_min_temp,
            ROUND(SUM(precipitation_mm), 2) as total_precipitation,
            ROUND(AVG(humidity_percent), 2) as avg_humidity,
            COUNT(*) as days_with_data,
            ROUND(STDDEV(max_temp_c), 2) as temp_variability
        FROM weather_data 
        WHERE max_temp_c IS NOT NULL 
        GROUP BY city_name, country, YEAR(weather_date), MONTH(weather_date)
        ORDER BY city_name, year, month
    """)
    
    monthly_analysis.show(10)
    monthly_analysis.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}monthly_analysis/")
    
    # 2. Ciudades más extremas (más calientes y más frías)
    print("\n2. Ciudades con temperaturas extremas...")
    extreme_cities = spark.sql("""
        WITH city_temps AS (
            SELECT 
                city_name,
                country,
                ROUND(AVG(max_temp_c), 2) as avg_temp,
                ROUND(MAX(max_temp_c), 2) as max_temp_recorded,
                ROUND(MIN(max_temp_c), 2) as min_temp_recorded,
                COUNT(*) as total_records
            FROM weather_data 
            WHERE max_temp_c IS NOT NULL
            GROUP BY city_name, country
            HAVING COUNT(*) >= 30  -- Solo ciudades con suficientes datos
        )
        SELECT 
            city_name,
            country,
            avg_temp,
            max_temp_recorded,
            min_temp_recorded,
            total_records,
            CASE 
                WHEN avg_temp >= 30 THEN 'Muy Caliente'
                WHEN avg_temp >= 20 THEN 'Caliente'
                WHEN avg_temp >= 10 THEN 'Templado'
                WHEN avg_temp >= 0 THEN 'Frío'
                ELSE 'Muy Frío'
            END as climate_category
        FROM city_temps
        ORDER BY avg_temp DESC
    """)
    
    extreme_cities.show(15)
    extreme_cities.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}extreme_cities/")
    
    # 3. Análisis estacional
    print("\n3. Análisis estacional...")
    seasonal_analysis = spark.sql("""
        SELECT 
            city_name,
            country,
            CASE 
                WHEN MONTH(weather_date) IN (12, 1, 2) THEN 'Invierno'
                WHEN MONTH(weather_date) IN (3, 4, 5) THEN 'Primavera'
                WHEN MONTH(weather_date) IN (6, 7, 8) THEN 'Verano'
                ELSE 'Otoño'
            END as season,
            ROUND(AVG(max_temp_c), 2) as avg_temp,
            ROUND(SUM(precipitation_mm), 2) as total_precipitation,
            COUNT(*) as days_count
        FROM weather_data 
        WHERE max_temp_c IS NOT NULL
        GROUP BY city_name, country, 
            CASE 
                WHEN MONTH(weather_date) IN (12, 1, 2) THEN 'Invierno'
                WHEN MONTH(weather_date) IN (3, 4, 5) THEN 'Primavera'
                WHEN MONTH(weather_date) IN (6, 7, 8) THEN 'Verano'
                ELSE 'Otoño'
            END
        ORDER BY city_name, season
    """)
    
    seasonal_analysis.show(20)
    seasonal_analysis.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}seasonal_analysis/")
    
    # 4. Correlaciones y patrones
    print("\n4. Análisis de correlaciones...")
    correlation_analysis = spark.sql("""
        SELECT 
            city_name,
            country,
            ROUND(CORR(max_temp_c, humidity_percent), 3) as temp_humidity_corr,
            ROUND(CORR(max_temp_c, precipitation_mm), 3) as temp_precipitation_corr,
            ROUND(AVG(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END), 3) as rainy_days_ratio
        FROM weather_data 
        WHERE max_temp_c IS NOT NULL AND humidity_percent IS NOT NULL
        GROUP BY city_name, country
        HAVING COUNT(*) >= 50
        ORDER BY temp_humidity_corr DESC
    """)
    
    correlation_analysis.show(10)
    correlation_analysis.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}correlations/")


def advanced_ml_analytics(spark, df):
    """
    Análisis predictivo avanzado con SparkML
    """
    print("\n=== ANÁLISIS PREDICTIVO CON SPARKML ===")
    
    # Preparar datos para ML
    print("\n1. Preparando datos para Machine Learning...")
    
    # Crear features agregadas por ciudad
    city_features = df.groupBy("city_name", "country", "location_latitude", "location_longitude") \
        .agg(
            round(avg("max_temp_c"), 2).alias("avg_max_temp"),
            round(avg("min_temp_c"), 2).alias("avg_min_temp"),
            round(sum("precipitation_mm"), 2).alias("total_precipitation"),
            round(avg("humidity_percent"), 2).alias("avg_humidity"),
            count("*").alias("data_points"),
            round(stddev("max_temp_c"), 2).alias("temp_variability")
        ).filter(col("data_points") >= 30)  # Solo ciudades con suficientes datos
    
    print("Features por ciudad preparadas:")
    city_features.show(5)
    
    # === CLUSTERING AVANZADO ===
    print("\n2. Clustering de ciudades por patrones climáticos...")
    
    # Preparar features para clustering
    clustering_cols = ["avg_max_temp", "total_precipitation", "avg_humidity", "temp_variability"]
    
    # Limpiar datos nulos
    clean_data = city_features.na.drop(subset=clustering_cols)
    
    # Vector assembler
    assembler = VectorAssembler(inputCols=clustering_cols, outputCol="features_raw")
    
    # Escalador para normalizar features
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    
    # Pipeline de preparación
    prep_pipeline = Pipeline(stages=[assembler, scaler])
    prep_model = prep_pipeline.fit(clean_data)
    scaled_data = prep_model.transform(clean_data)
    
    # Encontrar número óptimo de clusters usando el método del codo
    print("Evaluando número óptimo de clusters...")
    silhouette_scores = []
    k_values = range(2, 8)
    
    for k in k_values:
        kmeans = KMeans(k=k, seed=42)
        model = kmeans.fit(scaled_data)
        predictions = model.transform(scaled_data)
        
        evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction")
        score = evaluator.evaluate(predictions)
        silhouette_scores.append((k, score))
        print(f"  K={k}: Silhouette Score = {score:.4f}")
    
    # Usar el mejor K
    best_k = max(silhouette_scores, key=lambda x: x[1])[0]
    print(f"\nMejor número de clusters: K={best_k}")
    
    # Entrenar modelo final de clustering
    final_kmeans = KMeans(k=best_k, seed=42)
    final_cluster_model = final_kmeans.fit(scaled_data)
    cluster_predictions = final_cluster_model.transform(scaled_data)
    
    print(f"\nResultados de clustering (K={best_k}):")
    cluster_summary = cluster_predictions.groupBy("prediction") \
        .agg(
            count("*").alias("cities_count"),
            round(avg("avg_max_temp"), 2).alias("cluster_avg_temp"),
            round(avg("total_precipitation"), 2).alias("cluster_avg_precip"),
            round(avg("avg_humidity"), 2).alias("cluster_avg_humidity")
        ).orderBy("prediction")
    
    cluster_summary.show()
    
    # Mostrar ciudades por cluster
    print("\nCiudades por cluster:")
    cluster_predictions.select("city_name", "country", "prediction", "avg_max_temp", "total_precipitation") \
        .orderBy("prediction", "city_name").show(20)
    
    # Guardar resultados de clustering
    cluster_predictions.write.mode("overwrite").parquet(f"{ML_OUTPUT_PATH}clustering_results/")
    
    # === MODELO PREDICTIVO DE TEMPERATURA ===
    print("\n3. Modelo predictivo de temperatura...")
    
    # Preparar datos diarios para predicción
    daily_features = df.filter(
        col("max_temp_c").isNotNull() & 
        col("humidity_percent").isNotNull() & 
        col("precipitation_mm").isNotNull()
    ).withColumn("month_num", month(col("weather_date"))) \
     .withColumn("day_of_year", col("weather_date").cast("timestamp").cast("long") / 86400 % 365)
    
    # Features para predicción
    prediction_features = ["location_latitude", "location_longitude", "humidity_percent", 
                          "precipitation_mm", "month_num", "day_of_year"]
    
    # Preparar pipeline de ML para predicción
    pred_assembler = VectorAssembler(inputCols=prediction_features, outputCol="pred_features")
    pred_scaler = StandardScaler(inputCol="pred_features", outputCol="scaled_features", 
                                withStd=True, withMean=True)
    lr = LinearRegression(featuresCol="scaled_features", labelCol="max_temp_c", 
                         regParam=0.1, elasticNetParam=0.8)
    
    # Pipeline completo
    pred_pipeline = Pipeline(stages=[pred_assembler, pred_scaler, lr])
    
    # División train/test
    train_data, test_data = daily_features.randomSplit([0.8, 0.2], seed=42)
    
    print(f"Datos de entrenamiento: {train_data.count():,} registros")
    print(f"Datos de prueba: {test_data.count():,} registros")
    
    # Entrenar modelo
    pred_model = pred_pipeline.fit(train_data)
    
    # Predicciones
    predictions = pred_model.transform(test_data)
    
    # Evaluación
    evaluator = RegressionEvaluator(labelCol="max_temp_c", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
    
    print(f"\nMétricas del modelo predictivo:")
    print(f"  RMSE: {rmse:.4f}°C")
    print(f"  MAE: {mae:.4f}°C")
    print(f"  R²: {r2:.4f}")
    
    # Mostrar algunas predicciones
    print("\nEjemplos de predicciones vs valores reales:")
    predictions.select("city_name", "weather_date", "max_temp_c", "prediction") \
        .withColumn("error", round(col("max_temp_c") - col("prediction"), 2)) \
        .orderBy("weather_date").show(10)
    
    # Guardar modelo y predicciones
    predictions.write.mode("overwrite").parquet(f"{ML_OUTPUT_PATH}temperature_predictions/")
    
    return cluster_predictions, predictions


def run_analytics():
    """
    Función principal que ejecuta todo el análisis
    """
    print("=== INICIANDO ANÁLISIS AVANZADO DE DATOS METEOROLÓGICOS ===")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configurar credenciales
    if not setup_credentials():
        print("❌ Error: No se pudieron configurar las credenciales. Abortando.")
        return False
    
    # Crear sesión Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reducir verbosidad de logs
    
    try:
        # Leer datos
        print(f"\n📖 Leyendo datos desde: {TRUSTED_INPUT_PATH}")
        # En lugar de:
        #df = spark.read.parquet(TRUSTED_INPUT_PATH)

        # Prueba con:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("google.cloud.auth.service.account.json.keyfile", os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        df = spark.read.parquet(TRUSTED_INPUT_PATH)
        df.cache()
        
        print(f"✅ Datos cargados exitosamente")
        print(f"   Registros: {df.count():,}")
        print(f"   Columnas: {len(df.columns)}")
        
        # Mostrar esquema
        print("\n📋 Esquema de datos:")
        df.printSchema()
        
        # Verificación de calidad
        data_quality_check(df)
        
        # Análisis descriptivo
        descriptive_analytics(spark, df)
        
        # Análisis predictivo
        cluster_results, prediction_results = advanced_ml_analytics(spark, df)
        
        print("\n✅ ANÁLISIS COMPLETADO EXITOSAMENTE")
        print(f"📁 Resultados guardados en: {REFINED_DATA_BUCKET}")
        print("   - Análisis descriptivo: /descriptive_analytics_results/")
        print("   - Análisis ML: /ml_analytics_results/")
        
        return True
        
    except Exception as e:
        print(f"❌ Error durante el análisis: {str(e)}")
        return False
        
    finally:
        # Limpiar recursos
        if 'df' in locals():
            df.unpersist()
        spark.stop()
        print("\n🔒 Recursos liberados")


if __name__ == "__main__":
    success = run_analytics()
    exit(0 if success else 1)
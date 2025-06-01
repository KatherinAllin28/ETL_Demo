#!/usr/bin/env python
# coding: utf-8

# In[21]:


# configuración inicial
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum, count, month, year, round, max, min, stddev, when, isnan, isnull,
    date_format, abs, lit
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline
from datetime import datetime
import os

# Configurar Spark para BigQuery
spark = SparkSession.builder \
    .appName("WeatherAnalyticsFromBigQuery") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark configurado para BigQuery")


# In[ ]:

# Configuración de rutas
RAW_JSON_PATH = "gs://tet-raw-data/weather_data/2022/open_meteo_data_20250528_162227.json"
TRUSTED_DATA_BUCKET = "gs://tet-trusted-data"
REFINED_DATA_BUCKET = "gs://tet-refined-data"

TRUSTED_INPUT_PATH = f"{TRUSTED_DATA_BUCKET}/processed_weather_locations_data/*.parquet"
DESCRIPTIVE_OUTPUT_PATH = f"{REFINED_DATA_BUCKET}/descriptive_analytics_results/"
ML_OUTPUT_PATH = f"{REFINED_DATA_BUCKET}/ml_analytics_results/"

# El cluster ya tiene Spark configurado para GCS
spark = SparkSession.builder.appName("WeatherAnalyticsNotebook").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("✅ Configuración completada")
print(f"📍 Datos de entrada: {TRUSTED_INPUT_PATH}")
print(f"📍 Resultados: {REFINED_DATA_BUCKET}")


# # Correr análisis

# ## Análisis descriptivo

# In[24]:


# Celda para cargar datos desde BigQuery
# Configuración de BigQuery
PROJECT_ID = "tet-p3-2025"
DATASET_ID = "tet_analytics"
TABLE_ID = "weather_locations_data"
BIGQUERY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Rutas de salida (mantener las mismas)
REFINED_DATA_BUCKET = "gs://tet-refined-data"
DESCRIPTIVE_OUTPUT_PATH = f"{REFINED_DATA_BUCKET}/descriptive_analytics_results/"
ML_OUTPUT_PATH = f"{REFINED_DATA_BUCKET}/ml_analytics_results/"

print("📖 Cargando datos desde BigQuery...")
try:
    # Leer tabla completa desde BigQuery
    df = spark.read \
        .format("bigquery") \
        .option("table", BIGQUERY_TABLE) \
        .load()
    
    df.cache()
    
    print(f"✅ Datos cargados exitosamente desde BigQuery")
    print(f"   📊 Registros: {df.count():,}")
    print(f"   📋 Columnas: {len(df.columns)}")
    
    # Mostrar esquema
    print("\n📋 Esquema de datos:")
    df.printSchema()
    
    # Mostrar muestra de datos
    print("\n🔍 Muestra de datos:")
    df.show(5, truncate=False)
    
except Exception as e:
    print(f"❌ Error cargando desde BigQuery: {e}")
    print("Verificar que la tabla existe y tiene datos")


# In[ ]:

def data_quality_check(df):
    """Verificaciones de calidad de datos - Versión corregida"""
    print("=== VERIFICACIÓN DE CALIDAD DE DATOS ===")
    
    total_rows = df.count()
    print(f"Total de registros: {total_rows:,}")
    
    # Verificar valores nulos por columna
    print("\nValores nulos por columna:")
    for column in df.columns:
        column_type = dict(df.dtypes)[column]
        
        if column_type in ['double', 'float', 'int', 'bigint', 'long']:
            try:
                null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            except:
                null_count = df.filter(col(column).isNull()).count()
        else:
            null_count = df.filter(col(column).isNull()).count()
            
        null_percentage = (null_count / total_rows) * 100
        print(f"  {column} ({column_type}): {null_count:,} ({null_percentage:.2f}%)")
    
    # Estadísticas básicas de temperaturas
    print("\nEstadísticas de temperatura:")
    if "max_temp_c" in df.columns:
        temp_stats = df.select(
            min("max_temp_c").alias("min_temp"),
            max("max_temp_c").alias("max_temp"),
            avg("max_temp_c").alias("avg_temp"),
            stddev("max_temp_c").alias("stddev_temp")
        ).collect()[0]

        def safe_print(label, value):
            if value is not None:
                print(f"  {label}: {value:.2f}°C")
            else:
                print(f"  {label}: No disponible")

        safe_print("Mínima", temp_stats['min_temp'])
        safe_print("Máxima", temp_stats['max_temp'])
        safe_print("Promedio", temp_stats['avg_temp'])
        safe_print("Desviación estándar", temp_stats['stddev_temp'])
    else:
        print("  ⚠️ Columna max_temp_c no encontrada")
    
    # Estadísticas adicionales
    print("\nEstadísticas adicionales:")
    try:
        date_range = df.select(min("weather_date"), max("weather_date")).collect()[0]
        print(f"  Rango de fechas: {date_range}")
    except Exception as e:
        print(f"  ⚠️ Error obteniendo rango de fechas: {e}")

    try:
        print(f"  Ciudades únicas: {df.select('city_name').distinct().count()}")
    except Exception as e:
        print(f"  ⚠️ Error contando ciudades únicas: {e}")
    
    # Distribución por ciudad
    print("\nDistribución de registros por ciudad:")
    try:
        df.groupBy("city_name", "country").count().orderBy("count", ascending=False).show()
    except Exception as e:
        print(f"  ⚠️ Error mostrando distribución por ciudad: {e}")

data_quality_check(df)

# In[ ]:


# Registrar tabla temporal
df.createOrReplaceTempView("weather_data")

print("=== ANÁLISIS DESCRIPTIVO CON SPARKSQL ===")

# 1. Análisis mensual por ciudad (solo con columnas que existen)
print("\n1. 📅 Análisis mensual por ciudad...")
monthly_analysis = spark.sql("""
    SELECT 
        city_name,
        country,
        YEAR(weather_date) as year,
        MONTH(weather_date) as month,
        ROUND(AVG(max_temp_c), 2) as avg_max_temp,
        ROUND(SUM(precipitation_mm), 2) as total_precipitation,
        ROUND(AVG(precipitation_mm), 2) as avg_precipitation,
        COUNT(*) as days_with_data
    FROM weather_data 
    WHERE max_temp_c IS NOT NULL 
    GROUP BY city_name, country, YEAR(weather_date), MONTH(weather_date)
    ORDER BY city_name, year, month
""")

monthly_analysis.show(10)
monthly_analysis.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}monthly_analysis/")
print("✅ Análisis mensual guardado")


# In[ ]:


# 2. Ciudades con temperaturas extremas
print("\n2. 🌡️ Ciudades con temperaturas extremas...")
extreme_cities = spark.sql("""
    WITH city_temps AS (
        SELECT 
            city_name,
            country,
            ROUND(AVG(max_temp_c), 2) as avg_temp,
            ROUND(MAX(max_temp_c), 2) as max_temp_recorded,
            ROUND(MIN(max_temp_c), 2) as min_temp_recorded,
            ROUND(AVG(precipitation_mm), 2) as avg_precipitation,
            COUNT(*) as total_records
        FROM weather_data 
        WHERE max_temp_c IS NOT NULL
        GROUP BY city_name, country
        HAVING COUNT(*) >= 30
    )
    SELECT 
        city_name,
        country,
        avg_temp,
        max_temp_recorded,
        min_temp_recorded,
        avg_precipitation,
        total_records,
        CASE 
            WHEN avg_temp >= 25 THEN 'Caliente'
            WHEN avg_temp >= 20 THEN 'Templado Cálido'
            WHEN avg_temp >= 15 THEN 'Templado'
            WHEN avg_temp >= 10 THEN 'Fresco'
            ELSE 'Frío'
        END as climate_category
    FROM city_temps
    ORDER BY avg_temp DESC
""")

extreme_cities.show(15)
extreme_cities.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}extreme_cities/")
print("✅ Análisis de ciudades extremas guardado")


# In[ ]:


# 3. Análisis de épocas secas y lluviosas (Colombia)
print("\n3. 🌧️ Análisis de épocas secas y lluviosas...")
seasonal_analysis = spark.sql("""
    SELECT 
        city_name,
        country,
        CASE 
            WHEN MONTH(weather_date) IN (12, 1, 2, 3) THEN 'Época Seca'
            WHEN MONTH(weather_date) IN (4, 5, 10, 11) THEN 'Época Lluviosa'
            WHEN MONTH(weather_date) IN (6, 7, 8) THEN 'Época Semi-seca'
            ELSE 'Época Semi-lluviosa'
        END as epoca,
        ROUND(AVG(max_temp_c), 2) as avg_temp,
        ROUND(SUM(precipitation_mm), 2) as total_precipitation,
        ROUND(AVG(precipitation_mm), 2) as avg_daily_precipitation,
        COUNT(*) as days_count,
        ROUND(AVG(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END), 3) as rainy_days_ratio
    FROM weather_data 
    WHERE max_temp_c IS NOT NULL
    GROUP BY city_name, country, 
        CASE 
            WHEN MONTH(weather_date) IN (12, 1, 2, 3) THEN 'Época Seca'
            WHEN MONTH(weather_date) IN (4, 5, 10, 11) THEN 'Época Lluviosa'
            WHEN MONTH(weather_date) IN (6, 7, 8) THEN 'Época Semi-seca'
            ELSE 'Época Semi-lluviosa'
        END
    ORDER BY city_name, epoca
""")

seasonal_analysis.show(20)
seasonal_analysis.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}seasonal_analysis/")
print("✅ Análisis de épocas climáticas guardado")


# In[ ]:


# Análisis específico para clima tropical colombiano
print("\n🌴 Análisis climático tropical...")
tropical_analysis = spark.sql("""
    WITH monthly_patterns AS (
        SELECT 
            city_name,
            country,
            MONTH(weather_date) as month,
            ROUND(AVG(max_temp_c), 2) as avg_temp,
            ROUND(SUM(precipitation_mm), 2) as total_precip,
            ROUND(AVG(precipitation_mm), 2) as avg_daily_precip,
            COUNT(*) as days_count
        FROM weather_data 
        WHERE max_temp_c IS NOT NULL
        GROUP BY city_name, country, MONTH(weather_date)
    )
    SELECT 
        city_name,
        country,
        month,
        avg_temp,
        total_precip,
        avg_daily_precip,
        days_count,
        CASE 
            WHEN month IN (12, 1, 2, 3) THEN 'Verano (Seco)'
            WHEN month IN (4, 5) THEN 'Primera Lluvia'
            WHEN month IN (6, 7, 8) THEN 'Veranillo de San Juan'
            WHEN month IN (9, 10, 11) THEN 'Segunda Lluvia'
        END as periodo_climatico,
        CASE 
            WHEN avg_daily_precip > 5 THEN 'Muy Lluvioso'
            WHEN avg_daily_precip > 2 THEN 'Lluvioso'
            WHEN avg_daily_precip > 0.5 THEN 'Moderado'
            ELSE 'Seco'
        END as clasificacion_lluvia
    FROM monthly_patterns
    ORDER BY city_name, month
""")

tropical_analysis.show(24)
tropical_analysis.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}tropical_analysis/")
print("✅ Análisis climático tropical guardado")


# In[ ]:


# Análisis de patrones de lluvia específicos para Colombia
print("\n☔ Análisis de patrones de precipitación...")
precipitation_patterns = spark.sql("""
    SELECT 
        city_name,
        country,
        ROUND(AVG(max_temp_c), 2) as temperatura_promedio,
        ROUND(SUM(precipitation_mm), 2) as precipitacion_total_anual,
        ROUND(AVG(precipitation_mm), 2) as precipitacion_promedio_diaria,
        COUNT(CASE WHEN precipitation_mm > 0 THEN 1 END) as dias_con_lluvia,
        COUNT(CASE WHEN precipitation_mm > 10 THEN 1 END) as dias_lluvia_fuerte,
        COUNT(CASE WHEN precipitation_mm > 20 THEN 1 END) as dias_lluvia_intensa,
        ROUND(COUNT(CASE WHEN precipitation_mm > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as porcentaje_dias_lluvia,
        ROUND(MAX(precipitation_mm), 2) as precipitacion_maxima,
        COUNT(*) as total_dias,
        CASE 
            WHEN AVG(precipitation_mm) > 5 THEN 'Clima Muy Húmedo'
            WHEN AVG(precipitation_mm) > 3 THEN 'Clima Húmedo'
            WHEN AVG(precipitation_mm) > 1 THEN 'Clima Moderadamente Húmedo'
            ELSE 'Clima Seco'
        END as clasificacion_humedad
    FROM weather_data 
    WHERE max_temp_c IS NOT NULL
    GROUP BY city_name, country
    ORDER BY precipitacion_total_anual DESC
""")

precipitation_patterns.show(10, truncate=False)
precipitation_patterns.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}precipitation_patterns/")
print("✅ Análisis de patrones de precipitación guardado")


# In[ ]:


# Comparación específica entre Medellín y Bogotá
print("\n🏙️ Comparación entre ciudades colombianas...")
cities_comparison = spark.sql("""
    SELECT 
        city_name,
        country,
        ROUND(AVG(max_temp_c), 2) as temp_promedio,
        ROUND(MIN(max_temp_c), 2) as temp_minima,
        ROUND(MAX(max_temp_c), 2) as temp_maxima,
        ROUND(STDDEV(max_temp_c), 2) as variabilidad_temp,
        ROUND(SUM(precipitation_mm), 2) as lluvia_total,
        ROUND(AVG(precipitation_mm), 2) as lluvia_promedio,
        COUNT(CASE WHEN precipitation_mm > 0 THEN 1 END) as dias_lluvia,
        ROUND(COUNT(CASE WHEN precipitation_mm > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as pct_dias_lluvia,
        CASE 
            WHEN city_name = 'Medellin' THEN 'Valle de Aburrá - Clima tropical de montaña'
            WHEN city_name = 'Bogota' THEN 'Altiplano Cundiboyacense - Clima de montaña'
            ELSE 'Otro'
        END as caracteristicas_geograficas
    FROM weather_data 
    WHERE max_temp_c IS NOT NULL
    GROUP BY city_name, country
    ORDER BY temp_promedio DESC
""")

cities_comparison.show(10, truncate=False)
cities_comparison.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}cities_comparison/")
print("✅ Comparación entre ciudades guardado")


# In[ ]:


# 4. Análisis de correlaciones
print("\n4. 📈 Análisis de correlaciones...")

correlation_analysis = spark.sql("""
    SELECT 
        city_name,
        country,
        ROUND(CORR(max_temp_c, precipitation_mm), 3) as temp_precipitation_corr,
        ROUND(CORR(max_temp_c, elevation), 3) as temp_elevation_corr,
        ROUND(CORR(precipitation_mm, elevation), 3) as precip_elevation_corr,
        ROUND(AVG(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END), 3) as rainy_days_ratio,
        ROUND(AVG(max_temp_c), 2) as avg_temperature,
        ROUND(AVG(precipitation_mm), 2) as avg_precipitation,
        ROUND(AVG(elevation), 0) as avg_elevation,
        COUNT(*) as total_records
    FROM weather_data 
    WHERE max_temp_c IS NOT NULL 
        AND precipitation_mm IS NOT NULL 
        AND elevation IS NOT NULL
    GROUP BY city_name, country
    HAVING COUNT(*) >= 30
    ORDER BY temp_precipitation_corr DESC
""")

correlation_analysis.show(10, truncate=False)
correlation_analysis.write.mode("overwrite").parquet(f"{DESCRIPTIVE_OUTPUT_PATH}correlations/")
print("✅ Análisis de correlaciones guardado")


# ## Análisis ML

# In[17]:


print("=== ANÁLISIS PREDICTIVO CON SPARKML ===")

# Preparar datos para ML - Adaptado a BigQuery
print("\n1. 🤖 Preparando datos para Machine Learning...")

# Verificar columnas disponibles
print("Columnas disponibles en el dataset:", df.columns)

# Usar solo las columnas que existen en BigQuery
city_features = df.groupBy("city_name", "country") \
    .agg(
        round(avg("max_temp_c"), 2).alias("avg_max_temp"),
        round(sum("precipitation_mm"), 2).alias("total_precipitation"),
        round(avg("precipitation_mm"), 2).alias("avg_precipitation"),
        count("*").alias("data_points"),
        round(stddev("max_temp_c"), 2).alias("temp_variability"),
        round(max("max_temp_c"), 2).alias("max_temp_recorded"),
        round(min("max_temp_c"), 2).alias("min_temp_recorded"),
        # Usar elevation y population si están disponibles
        round(avg("elevation"), 2).alias("avg_elevation"),
        round(avg("population"), 2).alias("avg_population")
    ).filter(col("data_points") >= 30)

print("Features por ciudad preparadas:")
city_features.show(10, truncate=False)


# In[18]:


# 2. Clustering de ciudades
print("\n2. 🔍 Clustering de ciudades por patrones climáticos...")

# Definir columnas para clustering (solo las que existen)
clustering_cols = ["avg_max_temp", "total_precipitation", "temp_variability"]
clean_data = city_features.na.drop(subset=clustering_cols)

print(f"Datos para clustering: {clean_data.count()} ciudades")
print("Columnas utilizadas para clustering:", clustering_cols)

if clean_data.count() >= 2:  # Necesitamos al menos 2 puntos para clustering
    # Vector assembler y escalador
    assembler = VectorAssembler(inputCols=clustering_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

    # Pipeline de preparación
    prep_pipeline = Pipeline(stages=[assembler, scaler])
    prep_model = prep_pipeline.fit(clean_data)
    scaled_data = prep_model.transform(clean_data)

    # CORRECCIÓN: Usar min() correctamente
    city_count = clean_data.count()
    k_clusters = city_count if city_count < 3 else 3
    
    print(f"Número de clusters a usar: {k_clusters}")
    
    # Entrenar modelo de clustering
    kmeans = KMeans(k=k_clusters, seed=42)
    cluster_model = kmeans.fit(scaled_data)
    cluster_predictions = cluster_model.transform(scaled_data)

    # Resumen de clusters
    print(f"\nResultados de clustering (K={k_clusters}):")
    cluster_summary = cluster_predictions.groupBy("prediction") \
        .agg(
            count("*").alias("cities_count"),
            round(avg("avg_max_temp"), 2).alias("cluster_avg_temp"),
            round(avg("total_precipitation"), 2).alias("cluster_avg_precip"),
            round(avg("temp_variability"), 2).alias("cluster_temp_variability")
        ).orderBy("prediction")

    cluster_summary.show()

    # Mostrar ciudades por cluster
    print("\nCiudades por cluster:")
    cluster_predictions.select("city_name", "country", "prediction", "avg_max_temp", "total_precipitation", "temp_variability") \
        .orderBy("prediction", "city_name").show(20, truncate=False)

    # Guardar resultados
    cluster_predictions.write.mode("overwrite").parquet(f"{ML_OUTPUT_PATH}clustering_results/")
    print("✅ Resultados de clustering guardados")
    
else:
    print("❌ No hay suficientes datos para realizar clustering")

# In[19]:


# 3. Modelo de regresión para predecir precipitación
print("\n3. 📊 Modelo de regresión: Predecir precipitación basada en temperatura...")

# Preparar datos para regresión
regression_data = df.select(
    col("max_temp_c").alias("temperature"),
    col("precipitation_mm").alias("precipitation"),
    col("city_name"),
    month(col("weather_date")).alias("month"),
    year(col("weather_date")).alias("year")
).filter(
    col("temperature").isNotNull() & 
    col("precipitation").isNotNull()
)

if regression_data.count() > 10:  # Necesitamos datos suficientes
    # Preparar features para regresión
    reg_assembler = VectorAssembler(
        inputCols=["temperature", "month"], 
        outputCol="features"
    )
    
    reg_data = reg_assembler.transform(regression_data)
    
    # Dividir en entrenamiento y prueba
    train_data, test_data = reg_data.randomSplit([0.8, 0.2], seed=42)
    
    print(f"Datos de entrenamiento: {train_data.count()} registros")
    print(f"Datos de prueba: {test_data.count()} registros")
    
    if train_data.count() > 0 and test_data.count() > 0:
        # Entrenar modelo de regresión lineal
        lr = LinearRegression(featuresCol="features", labelCol="precipitation")
        lr_model = lr.fit(train_data)
        
        # Hacer predicciones
        predictions = lr_model.transform(test_data)
        
        # Evaluar modelo
        evaluator = RegressionEvaluator(labelCol="precipitation", predictionCol="prediction")
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        print(f"\nResultados del modelo de regresión:")
        print(f"  RMSE: {rmse:.3f}")
        print(f"  R²: {r2:.3f}")
        
        # Mostrar algunas predicciones
        print("\nEjemplos de predicciones:")
        predictions.select("city_name", "temperature", "month", "precipitation", "prediction") \
            .show(10, truncate=False)
        
        # Guardar resultados
        predictions.write.mode("overwrite").parquet(f"{ML_OUTPUT_PATH}regression_results/")
        print("✅ Resultados de regresión guardados")
    else:
        print("❌ No hay suficientes datos para entrenamiento y prueba")
else:
    print("❌ No hay suficientes datos para regresión")


# In[22]:


# 4. Análisis de patrones temporales
print("\n4. 📅 Análisis de patrones temporales...")

# Agregar features temporales
temporal_features = df.select(
    col("city_name"),
    col("max_temp_c"),
    col("precipitation_mm"),
    month(col("weather_date")).alias("month"),
    year(col("weather_date")).alias("year"),
    # Día del año (1-365)
    date_format(col("weather_date"), "D").cast("int").alias("day_of_year")
).filter(
    col("max_temp_c").isNotNull() & 
    col("precipitation_mm").isNotNull()
)

# Agrupar por ciudad y mes para encontrar patrones
monthly_patterns = temporal_features.groupBy("city_name", "month") \
    .agg(
        round(avg("max_temp_c"), 2).alias("avg_temp"),
        round(avg("precipitation_mm"), 2).alias("avg_precip"),
        count("*").alias("days_count")
    ).orderBy("city_name", "month")

print("Patrones mensuales por ciudad:")
monthly_patterns.show(24, truncate=False)

# Guardar patrones temporales
monthly_patterns.write.mode("overwrite").parquet(f"{ML_OUTPUT_PATH}temporal_patterns/")
print("✅ Patrones temporales guardados")


# In[23]:


# 5. Detección de anomalías climáticas
print("\n5. 🚨 Detección de anomalías climáticas...")

# Calcular estadísticas por ciudad para detectar anomalías
city_stats = df.groupBy("city_name") \
    .agg(
        avg("max_temp_c").alias("mean_temp"),
        stddev("max_temp_c").alias("std_temp"),
        avg("precipitation_mm").alias("mean_precip"),
        stddev("precipitation_mm").alias("std_precip")
    )

# Unir con datos originales para calcular z-scores
anomaly_data = df.join(city_stats, on="city_name") \
    .withColumn(
        "temp_z_score", 
        (col("max_temp_c") - col("mean_temp")) / col("std_temp")
    ) \
    .withColumn(
        "precip_z_score", 
        (col("precipitation_mm") - col("mean_precip")) / col("std_precip")
    )

# Identificar anomalías (z-score > 2 o < -2)
anomalies = anomaly_data.filter(
    (abs(col("temp_z_score")) > 2) | (abs(col("precip_z_score")) > 2)
).select(
    "weather_date", "city_name", "max_temp_c", "precipitation_mm",
    "temp_z_score", "precip_z_score"
).orderBy("weather_date")

print(f"Anomalías detectadas: {anomalies.count()} registros")
if anomalies.count() > 0:
    print("\nEjemplos de anomalías:")
    anomalies.show(10, truncate=False)
    
    # Guardar anomalías
    anomalies.write.mode("overwrite").parquet(f"{ML_OUTPUT_PATH}anomalies/")
    print("✅ Anomalías guardadas")
else:
    print("No se detectaron anomalías significativas")
#!/usr/bin/env python3
"""
Script maestro para orquestar todo el pipeline ETL
Ejecuta los 4 scripts en secuencia con manejo de errores
"""

import subprocess
import logging
import sys
from datetime import datetime
import time

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/opt/etl-automation/logs/etl_pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

# Configuración del pipeline
PROJECT_ID = "tet-p3-2025"
CLUSTER_NAME = "my-etl-cluster"
REGION = "us-east1"
BUCKET_SCRIPTS = "gs://etl-scripts-00/etl-pipeline"

# Scripts en orden de ejecución
PIPELINE_SCRIPTS = [
    {
        "name": "Extract SQL Data",
        "script": f"{BUCKET_SCRIPTS}/extract_sql_data.py",
        "timeout": 600,  # 10 minutos
        "retries": 2
    },
    {
        "name": "Ingest Weather Data", 
        "script": f"{BUCKET_SCRIPTS}/ingest_weather_data.py",
        "timeout": 1800,  # 30 minutos
        "retries": 2
    },
    {
        "name": "ETL Weather Locations",
        "script": f"{BUCKET_SCRIPTS}/etl_weather_locations.py", 
        "timeout": 1200,  # 20 minutos
        "retries": 1
    },
    {
        "name": "Load to BigQuery",
        "script": f"{BUCKET_SCRIPTS}/load_to_bigquery.py",
        "timeout": 900,   # 15 minutos
        "retries": 2
    }
]

def run_dataproc_job(script_path, job_name, timeout=600, retries=2):
    """Ejecutar job en Dataproc con reintentos"""
    
    for attempt in range(retries + 1):
        try:
            logging.info(f"Ejecutando {job_name} (intento {attempt + 1}/{retries + 1})")
            
            cmd = [
                "gcloud", "dataproc", "jobs", "submit", "pyspark",
                script_path,
                f"--cluster={CLUSTER_NAME}",
                f"--region={REGION}",
                f"--project={PROJECT_ID}",
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode == 0:
                logging.info(f"✅ {job_name} completado exitosamente")
                logging.info(f"Output: {result.stdout}")
                return True
            else:
                logging.error(f"❌ Error en {job_name}: {result.stderr}")
                if attempt < retries:
                    logging.info(f"Reintentando en 30 segundos...")
                    time.sleep(30)
                    
        except subprocess.TimeoutExpired:
            logging.error(f"⏰ Timeout en {job_name} después de {timeout} segundos")
            if attempt < retries:
                logging.info(f"Reintentando...")
                time.sleep(30)
        except Exception as e:
            logging.error(f"💥 Error inesperado en {job_name}: {str(e)}")
            if attempt < retries:
                time.sleep(30)
    
    return False

def run_full_pipeline():
    """Ejecutar pipeline completo"""
    
    logging.info("🚀 Iniciando pipeline ETL completo")
    start_time = datetime.now()
    
    # Verificar cluster activo
    try:
        cluster_check = subprocess.run([
            "gcloud", "dataproc", "clusters", "describe", CLUSTER_NAME,
            f"--region={REGION}", f"--project={PROJECT_ID}"
        ], capture_output=True, text=True, timeout=30)
        
        if cluster_check.returncode != 0:
            logging.error("❌ Cluster Dataproc no disponible")
            return False
            
        logging.info("✅ Cluster Dataproc verificado")
        
    except Exception as e:
        logging.error(f"❌ Error verificando cluster: {e}")
        return False
    
    # Ejecutar scripts en secuencia
    failed_jobs = []
    
    for i, job_config in enumerate(PIPELINE_SCRIPTS, 1):
        logging.info(f"📋 Paso {i}/{len(PIPELINE_SCRIPTS)}: {job_config['name']}")
        
        success = run_dataproc_job(
            job_config['script'],
            job_config['name'], 
            job_config['timeout'],
            job_config['retries']
        )
        
        if not success:
            logging.error(f"❌ Fallo crítico en: {job_config['name']}")
            failed_jobs.append(job_config['name'])
            # Para algunos jobs, podemos continuar; para otros, debemos parar
            if job_config['name'] in ["Extract SQL Data", "ETL Weather Locations"]:
                logging.error("🛑 Job crítico falló, deteniendo pipeline")
                break
        
        # Pausa entre jobs
        if i < len(PIPELINE_SCRIPTS):
            logging.info("⏳ Pausa de 60 segundos entre jobs...")
            time.sleep(60)
    
    # Resumen final
    end_time = datetime.now()
    duration = end_time - start_time
    
    logging.info("=" * 60)
    logging.info(f"📊 RESUMEN DEL PIPELINE")
    logging.info(f"Inicio: {start_time}")
    logging.info(f"Fin: {end_time}")
    logging.info(f"Duración: {duration}")
    
    if failed_jobs:
        logging.error(f"❌ Jobs fallidos: {', '.join(failed_jobs)}")
        logging.info("🔄 Considera revisar logs y ejecutar manualmente")
        return False
    else:
        logging.info("✅ Pipeline completado exitosamente")
        return True

if __name__ == "__main__":
    success = run_full_pipeline()
    sys.exit(0 if success else 1)

#!/bin/bash

# ================================
# 🌦️ TET Weather API - Despliegue Completo
# Script unificado para desplegar desde cero
# ================================

PROJECT_ID="tet-p3-2025"
REGION="us-east1"
API_ID="tet-weather-api"
GATEWAY_URL="tet-weather-api-gateway-a9rpivv4.ue.gateway.dev"
BUCKET_NAME="tet-refined-data"

echo "� Desplegando TET Weather API completa desde cero..."
echo "📋 Configuración:"
echo "   Project: $PROJECT_ID"
echo "   Region: $REGION"
echo "   Bucket: $BUCKET_NAME"
echo "   Gateway URL: $GATEWAY_URL"
echo ""

# ================================
# 1. VERIFICAR PREREQUISITOS
# ================================
echo "🔍 1. Verificando prerequisitos..."

# Verificar archivos necesarios
if [ ! -f "requirements.txt" ]; then
    echo "📋 Creando requirements.txt..."
    cat > requirements.txt << 'EOF'
google-cloud-storage
flask
pandas
pyarrow
functions-framework
EOF
fi

# Crear main.py completo si no existe o está incompleto
if [ ! -f "main.py" ] || [ $(wc -l < main.py) -lt 50 ]; then
    echo "📝 Creando main.py completo..."
    cat > main.py << 'EOF'
import json
import io
from google.cloud import storage
from flask import jsonify, Response, request
import pandas as pd

def get_data(request):
    """Cloud Function para servir datos del bucket tet-refined-data"""
    
    # Configurar CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        bucket_name = "tet-refined-data"
        dataset = request.args.get("dataset", "")
        format_type = request.args.get("format", "json")
        limit = int(request.args.get("limit", "100"))
        
        if not dataset:
            return jsonify({
                "error": "Missing 'dataset' parameter",
                "available_datasets": [
                    "correlations", "monthly_analysis", "seasonal_analysis",
                    "cities_comparison", "extreme_cities", "precipitation_patterns", "tropical_analysis",
                    "clustering_results", "regression_results", "anomalies", "temporal_patterns"
                ]
            }), 400

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # Buscar en ambas estructuras posibles
        possible_paths = [
            f"descriptive_analytics_results/{dataset}/",
            f"ml_analytics_results/{dataset}/",
            f"{dataset}/"
        ]
        
        blobs = []
        found_path = None
        
        for path in possible_paths:
            test_blobs = list(bucket.list_blobs(prefix=path))
            if test_blobs:
                blobs = test_blobs
                found_path = path
                break
        
        if not blobs:
            return jsonify({
                "error": f"No data found for dataset '{dataset}'",
                "searched_paths": possible_paths
            }), 404

        # Filtrar solo archivos parquet
        parquet_files = [blob for blob in blobs if blob.name.endswith('.parquet') and not blob.name.endswith('_SUCCESS')]
        
        if not parquet_files:
            return jsonify({
                "error": f"No parquet files found in '{dataset}'",
                "found_path": found_path,
                "files_found": [blob.name for blob in blobs]
            }), 404

        # Leer el primer archivo parquet
        blob = parquet_files[0]
        content = blob.download_as_bytes()
        
        # Convertir parquet a DataFrame
        parquet_file = io.BytesIO(content)
        df = pd.read_parquet(parquet_file)
        
        # Aplicar límite
        if limit > 0:
            df = df.head(limit)
        
        # Devolver en el formato solicitado
        if format_type.lower() == "csv":
            csv_output = df.to_csv(index=False)
            return Response(
                csv_output,
                mimetype="text/csv",
                headers={
                    **headers,
                    "Content-Disposition": f"attachment; filename={dataset}.csv"
                }
            )
        else:  # JSON por defecto
            result = {
                "dataset": dataset,
                "found_path": found_path,
                "records_count": len(df),
                "columns": list(df.columns),
                "data": df.to_dict('records')
            }
            return jsonify(result), 200, headers
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500, headers

def list_datasets(request):
    """Listar datasets disponibles"""
    headers = {'Access-Control-Allow-Origin': '*'}
    
    if request.method == 'OPTIONS':
        headers.update({
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        })
        return ('', 204, headers)
    
    try:
        client = storage.Client()
        bucket = client.bucket("tet-refined-data")
        
        datasets = []
        
        # Buscar en descriptive_analytics_results
        desc_folders = set()
        for blob in bucket.list_blobs(prefix="descriptive_analytics_results/"):
            path_parts = blob.name.split('/')
            if len(path_parts) >= 3 and path_parts[1]:
                desc_folders.add(path_parts[1])
        
        for folder in desc_folders:
            if folder:
                file_count = len(list(bucket.list_blobs(prefix=f"descriptive_analytics_results/{folder}/")))
                if file_count > 1:
                    datasets.append({
                        "name": folder,
                        "category": "descriptive_analytics", 
                        "file_count": file_count,
                        "endpoint": f"/data?dataset={folder}"
                    })
        
        # Buscar en ml_analytics_results
        ml_folders = set()
        for blob in bucket.list_blobs(prefix="ml_analytics_results/"):
            path_parts = blob.name.split('/')
            if len(path_parts) >= 3 and path_parts[1]:
                ml_folders.add(path_parts[1])
        
        for folder in ml_folders:
            if folder:
                file_count = len(list(bucket.list_blobs(prefix=f"ml_analytics_results/{folder}/")))
                if file_count > 1:
                    datasets.append({
                        "name": folder,
                        "category": "ml_analytics",
                        "file_count": file_count, 
                        "endpoint": f"/data?dataset={folder}"
                    })
        
        datasets.sort(key=lambda x: (x["category"], x["name"]))
        
        return jsonify({
            "available_datasets": datasets,
            "total_datasets": len(datasets),
            "categories": {
                "descriptive_analytics": [d for d in datasets if d["category"] == "descriptive_analytics"],
                "ml_analytics": [d for d in datasets if d["category"] == "ml_analytics"]
            }
        }), 200, headers
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500, headers
EOF
fi

echo "✅ Archivos verificados/creados"

# ================================
# 2. HABILITAR APIs DE GCP
# ================================
echo "🔧 2. Habilitando APIs de GCP..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable apigateway.googleapis.com
gcloud services enable servicecontrol.googleapis.com
gcloud services enable servicemanagement.googleapis.com

# ================================
# 3. CONFIGURAR PERMISOS
# ================================
echo "🔐 3. Configurando permisos para acceso al bucket..."

# Obtener cuenta de servicio por defecto
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"
echo "   Cuenta de servicio: $SERVICE_ACCOUNT"

# Otorgar permisos al bucket
gsutil iam ch serviceAccount:$SERVICE_ACCOUNT:objectViewer gs://$BUCKET_NAME
gsutil iam ch serviceAccount:$SERVICE_ACCOUNT:legacyBucketReader gs://$BUCKET_NAME

# Verificar bucket existe y tiene datos
echo "   Verificando bucket..."
BUCKET_EXISTS=$(gsutil ls gs://$BUCKET_NAME/ 2>/dev/null | wc -l)
if [ $BUCKET_EXISTS -eq 0 ]; then
    echo "⚠️ ADVERTENCIA: Bucket $BUCKET_NAME parece estar vacío o no accesible"
    echo "   Continúo con el despliegue..."
else
    echo "✅ Bucket verificado con $BUCKET_EXISTS elementos"
fi

# ================================
# 4. LIMPIAR DESPLIEGUES ANTERIORES
# ================================
echo "🗑️ 4. Limpiando despliegues anteriores (opcional)..."
gcloud functions delete get_data --region=$REGION --quiet 2>/dev/null || echo "   get_data no existía"
gcloud functions delete list_datasets --region=$REGION --quiet 2>/dev/null || echo "   list_datasets no existía"

# ================================
# 5. DESPLEGAR CLOUD FUNCTIONS
# ================================
echo "☁️ 5. Desplegando Cloud Functions..."

echo "   Desplegando get_data..."
gcloud functions deploy get_data \
    --runtime python39 \
    --trigger-http \
    --allow-unauthenticated \
    --region=$REGION \
    --source=. \
    --entry-point=get_data \
    --memory=512MB \
    --timeout=60s \
    --project=$PROJECT_ID

if [ $? -ne 0 ]; then
    echo "❌ Error desplegando get_data"
    exit 1
fi

echo "   Desplegando list_datasets..."
gcloud functions deploy list_datasets \
    --runtime python39 \
    --trigger-http \
    --allow-unauthenticated \
    --region=$REGION \
    --source=. \
    --entry-point=list_datasets \
    --memory=256MB \
    --timeout=30s \
    --project=$PROJECT_ID

if [ $? -ne 0 ]; then
    echo "❌ Error desplegando list_datasets"
    exit 1
fi

# ================================
# 6. VERIFICAR Y PROBAR FUNCTIONS
# ================================
echo "✅ 6. Verificando Cloud Functions desplegadas..."
gcloud functions list --regions=$REGION --format="table(name,status,httpsTrigger.url)"

# Obtener URLs de las funciones
GET_DATA_URL=$(gcloud functions describe get_data --region=$REGION --format="value(serviceConfig.uri)")
LIST_DATASETS_URL=$(gcloud functions describe list_datasets --region=$REGION --format="value(serviceConfig.uri)")

echo "🔗 URLs de funciones:"
echo "   get_data: $GET_DATA_URL"
echo "   list_datasets: $LIST_DATASETS_URL"

# Verificar que las URLs no están vacías
if [ -z "$GET_DATA_URL" ] || [ -z "$LIST_DATASETS_URL" ]; then
    echo "❌ Error: URLs de funciones están vacías"
    exit 1
fi

# Probar funciones directamente
echo "🧪 Probando Cloud Functions directamente..."
echo "   Probando list_datasets:"
curl -s "$LIST_DATASETS_URL" | head -200

echo -e "\n   Probando get_data:"
curl -s "$GET_DATA_URL?dataset=correlations&limit=1" | head -200

# ================================
# 7. CONFIGURAR API GATEWAY
# ================================
echo "🔌 7. Configurando API Gateway..."

# Crear API si no existe
gcloud api-gateway apis create $API_ID --project=$PROJECT_ID 2>/dev/null || echo "   API ya existe"

# Crear OpenAPI spec
echo "   Creando especificación OpenAPI..."
cat > openapi_final.yaml << EOF
swagger: '2.0'
info:
  title: TET Weather Data API
  description: API para acceder a datos de análisis climático procesados
  version: '1.0.0'
host: '$GATEWAY_URL'
schemes: [https]
produces: [application/json, text/csv]

paths:
  /data:
    get:
      summary: Obtener datos de un dataset específico
      operationId: get_data
      parameters:
        - name: dataset
          in: query
          required: true
          type: string
          description: "Nombre del dataset (ej: correlations, monthly_analysis)"
        - name: format
          in: query
          required: false
          type: string
          default: json
          enum: [json, csv, parquet]
          description: "Formato de respuesta"
        - name: limit
          in: query
          required: false
          type: integer
          default: 100
          description: "Límite de registros a devolver"
      responses:
        200:
          description: Datos del dataset
        400:
          description: Parámetros inválidos
        404:
          description: Dataset no encontrado
      x-google-backend:
        address: $GET_DATA_URL
      security: []

  /datasets:
    get:
      summary: Listar todos los datasets disponibles
      operationId: list_datasets
      responses:
        200:
          description: Lista de datasets disponibles
      x-google-backend:
        address: $LIST_DATASETS_URL
      security: []
EOF

# Crear configuración de API
CONFIG_NAME="${API_ID}-$(date +%s)"
echo "   Creando configuración: $CONFIG_NAME"

gcloud api-gateway api-configs create $CONFIG_NAME \
    --api=$API_ID \
    --openapi-spec=openapi_final.yaml \
    --project=$PROJECT_ID

if [ $? -ne 0 ]; then
    echo "❌ Error creando configuración de API"
    exit 1
fi

# Crear o actualizar gateway
echo "   Configurando gateway..."
gcloud api-gateway gateways create ${API_ID}-gateway \
    --api=$API_ID \
    --api-config=$CONFIG_NAME \
    --location=$REGION \
    --project=$PROJECT_ID 2>/dev/null || \
gcloud api-gateway gateways update ${API_ID}-gateway \
    --api=$API_ID \
    --api-config=$CONFIG_NAME \
    --location=$REGION \
    --project=$PROJECT_ID

# ================================
# 8. PRUEBAS FINALES
# ================================
echo "⏳ 8. Esperando propagación de cambios (60 segundos)..."
sleep 60

echo "🧪 Probando API Gateway final..."
echo "   GET /datasets:"
curl -s "https://$GATEWAY_URL/datasets" | head -300

echo -e "\n   GET /data (correlations):"
curl -s "https://$GATEWAY_URL/data?dataset=correlations&limit=2" | head -300

# ================================
# 9. RESUMEN FINAL
# ================================
echo ""
echo "🎉 ¡DESPLIEGUE COMPLETADO EXITOSAMENTE!"
echo "=================================="
echo "🔗 API Gateway URL: https://$GATEWAY_URL"
echo ""
echo "📋 Endpoints disponibles:"
echo "   GET /datasets              - Listar todos los datasets"
echo "   GET /data?dataset=<name>   - Obtener datos específicos"
echo ""
echo "💡 Ejemplos de uso:"
echo "   curl 'https://$GATEWAY_URL/datasets'"
echo "   curl 'https://$GATEWAY_URL/data?dataset=correlations&limit=5'"
echo "   curl 'https://$GATEWAY_URL/data?dataset=monthly_analysis&format=csv'"
echo ""
echo "📱 URLs de Cloud Functions (para debugging):"
echo "   get_data: $GET_DATA_URL"
echo "   list_datasets: $LIST_DATASETS_URL"
echo ""
echo "✅ La API está lista para usar!"
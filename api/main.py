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
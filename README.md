# 🌦️ TET Weather Analytics Platform

This project is a full data engineering prototype designed to automate the lifecycle of data ingestion, processing, analysis, and visualization using weather data. It is developed as part of the ST0263 - Special Topics in Telematics course at Universidad EAFIT.

## 🧑‍🤝‍🧑 Team Members

| Name               | Email                      |
|--------------------|----------------------------|
| Jose Tordecilla      | jatordeciz@eafit.edu.co          |
| Katherine Allin        | knallinm@eafit.edu.co    |
| Juan Montoya    | jamontoya2@eafit.edu.co|

## 📌 Project Overview

The solution implements an end-to-end pipeline for climate data analysis with batch architecture principles. It uses both real-time API sources and a synthetic MySQL database to replicate a realistic multi-source data integration environment.

The final outputs are made available through:
- Google BigQuery
- An API Gateway for public access
- A frontend dashboard built with Streamlit

---

## 🏗️ Architecture Summary

### ⏩ Workflow:
<img src="https://github.com/user-attachments/assets/a758e9e1-9a3f-4802-bb89-f6cbe5231eda" alt="workflow" height=600>

### ⛅ Data Sources:
- **Open Meteo API**: Daily and historical weather data.
- **MySQL**: Synthetic complementary data for enrichment.

### 📥 Data Ingestion:
- Automated scripts pull data periodically from both the API and database.
- Raw data is stored in **Google Cloud Storage (GCS)** under the **Raw** bucket.

### 🛠️ Data Processing:
- ETL operations are executed in an automated **Google Dataproc (Spark) cluster**.
- A **trigger VM** schedules jobs using `crontab` to run Spark ETL and analytics every 12 hours.
- Processed outputs are stored in the **Trusted** bucket and **BigQuery**.

### 📊 Analytics:
Using PySpark and SparkML in `advanced_analysis/analysis.py`, we perform:

#### Descriptive Analytics:
- Monthly summaries by city
- Seasonal patterns and climate categories
- Rainfall distribution and precipitation patterns
- Inter-city comparisons (e.g., Medellín vs. Bogotá)
- Correlation studies (e.g., temperature vs. precipitation)

#### Predictive & ML Analytics:
- Clustering cities by climate indicators
- Regression models to predict precipitation based on temperature
- Detection of climate anomalies
- Temporal pattern identification

The results are stored in the **Refined** bucket.

### 📡 API Gateway:
A public REST API exposes the processed results:
- **Base URL**: `https://tet-weather-api-gateway-a9rpivv4.ue.gateway.dev`
- **Auth**: Not required
- **Methods**: `GET`
- **Formats**: JSON, CSV

#### Available Endpoints:
- `/datasets` — List available datasets
- `/data?dataset=...` — Fetch data from a specific dataset

Sample call:
```bash
curl 'https://tet-weather-api-gateway-a9rpivv4.ue.gateway.dev/data?dataset=monthly_analysis&format=json&limit=10'
```

### 📺 Frontend:
Built using **Streamlit**, the UI fetches and visualizes:
- Descriptive results from BigQuery and API Gateway
- ML outputs like clustering or anomalies
- Interactive visualizations with filtering by city and time range

---

## 🚀 How to Use

### Clone the repository
```bash
git clone https://github.com/your-org/tet-weather-analytics.git
cd tet-weather-analytics
```

### View API data
1. List datasets:
    ```bash
    curl https://tet-weather-api-gateway-a9rpivv4.ue.gateway.dev/datasets
    ```

2. Fetch data:
    ```bash
    curl https://tet-weather-api-gateway-a9rpivv4.ue.gateway.dev/data?dataset=clustering_results&format=csv
    ```

### Run the frontend
Make sure you have Python + Streamlit installed:
```bash
pip install streamlit pandas requests google-cloud-bigquery plotly
streamlit run app/streamlit_app.py
```

---

## 📁 Buckets Summary

| Bucket Name         | Purpose                         |
|---------------------|----------------------------------|
| `tet-raw-data`      | Raw ingested weather + DB data   |
| `tet-trusted-data`  | Cleaned and integrated datasets  |
| `tet-refined-data`  | Final analytics and ML outputs   |

---

## 🧠 Technologies Used

- **Apache Spark (PySpark)**
- **Google Cloud Dataproc**
- **Google Cloud Storage (GCS)**
- **BigQuery**
- **API Gateway (Cloud Functions)**
- **Streamlit**

---

## 📈 Datasets Available via API

| Dataset Name          | Type               |
|-----------------------|--------------------|
| correlations          | Descriptive        |
| monthly_analysis      | Descriptive        |
| seasonal_analysis     | Descriptive        |
| extreme_cities        | Descriptive        |
| precipitation_patterns| Descriptive        |
| cities_comparison     | Descriptive        |
| tropical_analysis     | Descriptive        |
| clustering_results    | ML                 |
| regression_results    | ML                 |
| anomalies             | ML                 |
| temporal_patterns     | ML                 |

---

## 🔁 Automation

- ETL scripts are executed on a scheduled **VM** using `cron`.
- Cluster is monitored and jobs are logged in `/home/ubuntu/dataproc_log.txt`.
- The entire pipeline is automated from raw ingestion to refined analytics delivery.

---

## 📽️ Demo & Documentation

- Refer to `api/how_to_use.txt` for complete API usage instructions.
- `advanced_analysis/analysis.py` contains all descriptive and ML logic using PySpark.

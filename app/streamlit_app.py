import streamlit as st# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Comparador de Clima entre Ciudades")
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
from datetime import date
import os
import json
import requests # Importar la librería requests

# --- BigQuery Configuration ---
PROJECT_ID = "tet-p3-2025"
DATASET_ID = "tet_analytics"
TABLE_ID = "weather_locations_data" # Tu tabla donde está el resultado del ETL
BIGQUERY_TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# --- Google Cloud Service Account Key ---
SERVICE_ACCOUNT_KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH", "credentials.json")

# Initialize BigQuery client
@st.cache_resource # Cache the client to avoid re-initializing on every rerun
def get_bigquery_client():
    if os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
        # Authenticate using the service account key file
        return bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_PATH, project=PROJECT_ID)
    else:
        # Fallback: relies on GOOGLE_APPLICATION_CREDENTIALS env var or gcloud default login
        st.warning(f"No se encontró el archivo de credenciales en {SERVICE_ACCOUNT_KEY_PATH}. Intentando autenticación por defecto (gcloud auth).")
        return bigquery.Client(project=PROJECT_ID)

client = get_bigquery_client()

# --- Helper function to fetch data from BigQuery ---
@st.cache_data(ttl=3600) # Cache the data for 1 hour to avoid repeated queries
def fetch_weather_data(cities, start_date, end_date):
    if not cities or not start_date or not end_date:
        return pd.DataFrame()

    # Format dates for SQL query
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Build the WHERE clause for cities
    city_names_str = ", ".join([f"'{city}'" for city in cities])

    query = f"""
    SELECT
        weather_date,
        city_name,
        max_temp_c,
        precipitation_mm,
        population,
        elevation
    FROM
        {BIGQUERY_TABLE_FULL_ID}
    WHERE
        city_name IN ({city_names_str})
        AND weather_date BETWEEN '{start_date_str}' AND '{end_date_str}'
    ORDER BY
        weather_date, city_name
    """
    
    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        # Ensure 'weather_date' is datetime type for plotting
        df['weather_date'] = pd.to_datetime(df['weather_date'])
        return df
    except Exception as e:
        st.error(f"Error al consultar BigQuery: {e}. Asegúrate de que la tabla '{BIGQUERY_TABLE_FULL_ID}' exista y que tu cuenta de servicio tenga los permisos adecuados (roles 'BigQuery Data Viewer' y 'BigQuery Job User' son un buen inicio).")
        return pd.DataFrame()

# --- Helper function to get unique city names for selection ---
@st.cache_data(ttl=86400) # Cache city names for 24 hours
def get_available_cities():
    query = f"""
    SELECT DISTINCT city_name
    FROM {BIGQUERY_TABLE_FULL_ID}
    ORDER BY city_name
    """
    try:
        query_job = client.query(query)
        cities_df = query_job.to_dataframe()
        return cities_df['city_name'].tolist()
    except Exception as e:
        st.error(f"Error al obtener la lista de ciudades: {e}. Verifica que la tabla de BigQuery exista y contenga datos.")
        return []

# --- NUEVA FUNCIÓN: Fetch data from the pre-calculated API ---
@st.cache_data(ttl=3600 * 24) # Cache API data for 24 hours
def fetch_pre_calculated_data_from_api(dataset_name):
    BASE_API_URL = "https://us-east1-tet-p3-2025.cloudfunctions.net/get_data"
    params = {"dataset": dataset_name}
    try:
        response = requests.get(BASE_API_URL, params=params)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        if "data" in data:
            return pd.DataFrame(data["data"])
        else:
            st.warning(f"La API no devolvió una clave 'data' para el dataset '{dataset_name}'. Respuesta completa: {data}")
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Error al consultar la API para el dataset '{dataset_name}': {e}. Asegúrate de que la URL de la API es correcta y accesible.")
        return pd.DataFrame()
    except json.JSONDecodeError:
        st.error(f"Error al decodificar la respuesta JSON de la API para el dataset '{dataset_name}'.")
        return pd.DataFrame()


st.title("☀ Comparador de Clima entre Ciudades ☀")
st.markdown("Selecciona dos ciudades y un rango de fechas para comparar sus datos climáticos históricos y explora los análisis calculados.")

available_cities = get_available_cities()

# Initial state for comparison visibility
if 'show_comparison' not in st.session_state:
    st.session_state.show_comparison = False

if not available_cities:
    st.warning("No se pudieron cargar las ciudades disponibles. Asegúrate de que la tabla de BigQuery exista y contenga datos.")
    st.info("Revisa los logs de los scripts ETL (etl_weather_locations.py) y de carga a BigQuery (load_to_bigquery.py) para verificar que los datos se hayan cargado correctamente a BigQuery.")
else:
    # Sidebar for user input
    with st.sidebar:
        st.header("Selección de Ciudades y fechas a comparar")
        
        # City 1 selection
        city_1 = st.selectbox(
            "Selecciona la primera ciudad:",
            options=available_cities,
            index=0 if available_cities else None, # Default to first city
            key="city1_select"
        )

        # City 2 selection (ensure it's different from city 1)
        cities_for_city2 = [c for c in available_cities if c != city_1]
        
        # Try to find a default index for city_2 that is not city_1
        default_city2_index = 0
        if city_1 and len(cities_for_city2) > 0:
            if available_cities.index(city_1) == 0 and len(cities_for_city2) > 0:
                default_city2_index = 0 # If city1 is the first, pick the first from remaining
            elif available_cities.index(city_1) > 0 and len(cities_for_city2) > 0:
                default_city2_index = 0 # If city1 is not first, pick the first from remaining
        
        city_2 = st.selectbox(
            "Selecciona la segunda ciudad:",
            options=cities_for_city2,
            index=default_city2_index if cities_for_city2 else None,
            key="city2_select"
        )
        
        # Date range selection (assuming data for 2022 as per ingest_weather_data.py)
        min_date_available = date(2022, 1, 1) # Adjust based on your actual data
        max_date_available = date(2022, 12, 31) # Adjust based on your actual data

        start_date = st.date_input("Fecha de inicio:", value=min_date_available, min_value=min_date_available, max_value=max_date_available, key="start_date_input")
        end_date = st.date_input("Fecha de fin:", value=max_date_available, min_value=min_date_available, max_value=max_date_available, key="end_date_input")

        # Validation for dates
        if start_date > end_date:
            st.error("Error: La fecha de inicio no puede ser posterior a la fecha de fin.")
            st.stop() # Stop execution if dates are invalid

        # Submit button
        if st.button("Comparar Clima"):
            if city_1 == city_2:
                st.warning("Por favor, selecciona dos ciudades diferentes para comparar.")
            else:
                st.session_state.show_comparison = True
                st.session_state.selected_cities = [city_1, city_2]
                st.session_state.selected_start_date = start_date
                st.session_state.selected_end_date = end_date
        
    # Main content area
    if st.session_state.show_comparison and st.session_state.get('selected_cities'): # Check if cities are selected after button click
        st.header(f"Comparación de Clima: {st.session_state.selected_cities[0]} vs {st.session_state.selected_cities[1]}")
        st.subheader(f"Desde {st.session_state.selected_start_date.strftime('%Y-%m-%d')} hasta {st.session_state.selected_end_date.strftime('%Y-%m-%d')}")

        # Fetch data
        df_weather = fetch_weather_data(
            st.session_state.selected_cities,
            st.session_state.selected_start_date,
            st.session_state.selected_end_date
        )

        if not df_weather.empty:
            st.write("---")
            st.subheader("Datos de Temperatura Máxima (°C)")
            fig_temp = px.line(
                df_weather,
                x="weather_date",
                y="max_temp_c",
                color="city_name",
                title="Temperatura Máxima Diaria (°C)",
                labels={"max_temp_c": "Temperatura Máxima (°C)", "weather_date": "Fecha", "city_name": "Ciudad"},
                hover_data=["precipitation_mm", "population", "elevation"] # Add more hover info from your data
            )
            fig_temp.update_layout(hovermode="x unified") # Show hover data for all lines at x-axis
            st.plotly_chart(fig_temp, use_container_width=True)

            st.write("---")
            st.subheader("Datos de Precipitación (mm)")
            fig_prec = px.bar(
                df_weather,
                x="weather_date",
                y="precipitation_mm",
                color="city_name",
                barmode="group", # Group bars for comparison
                title="Suma de Precipitación Diaria (mm)",
                labels={"precipitation_mm": "Precipitación (mm)", "weather_date": "Fecha", "city_name": "Ciudad"},
                hover_data=["max_temp_c", "population", "elevation"]
            )
            fig_prec.update_layout(hovermode="x unified")
            st.plotly_chart(fig_prec, use_container_width=True)

            st.write("---")
            st.subheader("Tabla de Datos")
            st.dataframe(df_weather) # Display all relevant data
        else:
            st.info("No hay datos disponibles para la selección actual. Intenta con un rango de fechas o ciudades diferentes.")
            
    else:
        st.info("Utiliza la barra lateral izquierda para seleccionar 2 ciudades y un rango de fechas a visualizar, luego haz clic en 'Comparar Clima' para ver los resultados.")

    # --- NUEVAS SECCIONES: Datos pre-calculados de la API ---
    st.markdown("---")
    st.header("Análisis Climáticos Calculados")

    # Expander para Comparación entre Ciudades (API)
    with st.expander("📊 Comparación entre Ciudades"):
        st.markdown("Este dataset contiene comparaciones y métricas pre-calculadas entre diferentes ciudades.")
        df_cities_comparison = fetch_pre_calculated_data_from_api("cities_comparison")
        if not df_cities_comparison.empty:
            st.dataframe(df_cities_comparison)
        else:
            st.info("No se pudieron cargar los datos de comparación entre ciudades desde la API.")

    # Expander para Patrones Temporales (API)
    with st.expander("📈 Patrones Temporales"):
        st.markdown("A continuación verás una muestra de patrones y tendencias temporales identificadas en los datos climáticos.")
        df_temporal_patterns = fetch_pre_calculated_data_from_api("temporal_patterns")
        if not df_temporal_patterns.empty:
            st.dataframe(df_temporal_patterns)
        else:
            st.info("No se pudieron cargar los datos de patrones temporales desde la API.")
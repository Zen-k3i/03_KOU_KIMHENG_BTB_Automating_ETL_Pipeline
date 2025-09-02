"""
## Weather ETL DAG for Phnom Penh

This DAG retrieves daily weather data from OpenWeatherMap and uploads it to
Google Cloud Storage. It uses Airflow 2.x providers and PythonOperators.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import pytz

# --- Environment Variables ---
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_OBJECT_NAME = "weather_data_phnom_penh.csv"

CITY = "Phnom Penh"
API_URL = f"/data/2.5/weather?q={CITY.replace(' ', '%20')}&appid={OPENWEATHER_API_KEY}"

# --- DAG ---
with DAG(
    dag_id="weather_etl_gcp_phnom_penh_celsius",
    start_date=datetime(2025, 8, 29),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["ETL", "weather", "GCP"]
) as dag:

    # --- Step 1: HTTP Sensor ---
    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="openweather_api",  # Create this Airflow connection
        endpoint=API_URL,
        poke_interval=10,
        timeout=30,
        mode="reschedule"
    )

    # --- Step 2: Extract ---
    def extract_data_callable():
        try:
            base_url = "https://api.openweathermap.org"
            response = requests.get(f"{base_url}{API_URL}")
            response.raise_for_status()
            data = response.json()

            # Convert Kelvin to Celsius
            def kelvin_to_c(k):
                return round(k - 273.15, 2)

            tz_offset = data.get("timezone", 0)  # in seconds
            timestamp = datetime.utcfromtimestamp(data.get("dt")) + timedelta(seconds=tz_offset)
            sunrise = datetime.utcfromtimestamp(data["sys"]["sunrise"]) + timedelta(seconds=tz_offset)
            sunset = datetime.utcfromtimestamp(data["sys"]["sunset"]) + timedelta(seconds=tz_offset)

            return {
                "City": data.get("name"),
                "Description": data.get("weather", [{}])[0].get("description"),
                "Temperature (C)": kelvin_to_c(data.get("main", {}).get("temp")),
                "Feels Like (C)": kelvin_to_c(data.get("main", {}).get("feels_like")),
                "Minimum Temp (C)": kelvin_to_c(data.get("main", {}).get("temp_min")),
                "Maximum Temp (C)": kelvin_to_c(data.get("main", {}).get("temp_max")),
                "Pressure": data.get("main", {}).get("pressure"),
                "Humidity": data.get("main", {}).get("humidity"),
                "Wind Speed": data.get("wind", {}).get("speed"),
                "Time of Record": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "Sunrise (Local Time)": sunrise.strftime("%Y-%m-%d %H:%M:%S"),
                "Sunset (Local Time)": sunset.strftime("%Y-%m-%d %H:%M:%S")
            }

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch weather data: {e}")

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_callable
    )

    # --- Step 3: Transform ---
    def transform_data_callable(ti):
        raw_data = ti.xcom_pull(task_ids="extract_data")
        # Pull values explicitly in correct column order
        transformed_data = [[
            raw_data["City"],
            raw_data["Description"],
            raw_data["Temperature (C)"],
            raw_data["Feels Like (C)"],
            raw_data["Minimum Temp (C)"],
            raw_data["Maximum Temp (C)"],
            raw_data["Pressure"],
            raw_data["Humidity"],
            raw_data["Wind Speed"],
            raw_data["Time of Record"],
            raw_data["Sunrise (Local Time)"],
            raw_data["Sunset (Local Time)"]
        ]]
        return transformed_data

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data_callable
    )

    # --- Step 4: Load to GCS ---
    def load_data_callable(ti):
        transformed_data = ti.xcom_pull(task_ids="transform_data")
        df = pd.DataFrame(transformed_data, columns=[
            "City", "Description", "Temperature (C)", "Feels Like (C)",
            "Minimum Temp (C)", "Maximum Temp (C)", "Pressure", "Humidity",
            "Wind Speed", "Time of Record", "Sunrise (Local Time)", "Sunset (Local Time)"
        ])
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        local_file = f"/tmp/weather_data_phnom_penh_{timestamp}.csv"
        df.to_csv(local_file, index=False)

        gcs = GCSHook(gcp_conn_id="google_cloud_default")
        gcs.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=f"{GCS_OBJECT_NAME}-{timestamp}",
            filename=local_file,
            mime_type="text/csv"
        )
        print(f"Uploaded {local_file} to gs://{GCS_BUCKET_NAME}/{GCS_OBJECT_NAME}-{timestamp}")

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_callable
    )

    # --- DAG Dependencies ---
    check_api >> extract_data >> transform_data >> load_data

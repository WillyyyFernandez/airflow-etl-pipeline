#Weather ETL Pipeline - Custom DAG

# This DAG extracts weather data for Merida, Yucatan from the Open-Meteo API, transforms the data, and loads it into CSV and JSON formats.

from airflow.decorators import dag, task
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import json
import logging
import pandas as pd
import requests

# Configuration

LATITUDE = 20.9754
LONGITUDE = -89.6170

BASE_URL = "https://api.open-meteo.com/v1/forecast"

DATA_DIRECTORY = "/opt/airflow/data"

logger = logging.getLogger(__name__)

# Helper functions

def categorize_temp(temp: float) -> str:
    
    #Categorize temperature in Celsius.

    # Args:
    #     temp: Temperature value in Celsius.

    # Returns:
    #     Temperature category.

    if temp < 20:
        return "Cool" # if temp is less than 20, return Cool    
    elif temp <= 30: # if temp is between 20 and 30, return Warm
        return "Warm"
    return "Hot" # if temp is greater than 30, return Hot

# DAG Definition

@dag(
    dag_id="weather_etl",
    description="Extract weather data, transform temperatures, and save results",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["weather", "etl", "student"]
)
def weather_etl_pipeline():

    @task
    def extract_weather() -> Dict[str, Any]:
        # Extract weather forecast data from Open-Meteo API.
        logger.info("Extracting weather data for Merida...")

        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "hourly": (
                "temperature_2m,"
                "apparent_temperature,"
                "precipitation_probability"
            ),
            "timezone": "auto"
        }

        try:
            response = requests.get(
                BASE_URL,
                params=params,
                timeout=15
            )

            response.raise_for_status()

            logger.info("Weather data extracted successfully.")

            return response.json()

        except requests.exceptions.RequestException as error:
            logger.error(f"Failed to fetch weather data: {error}")
            raise

    @task
    def transform_weather(
        raw_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        #Transform weather data using pandas.

        # Operations:
        # - Celsius to Fahrenheit conversion
        # - Temperature categorization
        # - Summary statistics generation

        logger.info("Transforming weather data...")

        try:
            hourly_data = raw_data["hourly"]

            df = pd.DataFrame(hourly_data)

            # Convert temperatures
            df["temp_fahrenheit"] = (
                df["temperature_2m"] * 9 / 5
            ) + 32

            df["feels_like_f"] = (
                df["apparent_temperature"] * 9 / 5
            ) + 32

            # Categorize temperatures
            df["temp_category"] = (
                df["temperature_2m"]
                .apply(categorize_temp)
            )

            # Category counts
            category_counts = (
                df["temp_category"]
                .value_counts()
                .to_dict()
            )

            # Summary statistics
            summary_stats = {
                "max_temp_c": float(
                    df["temperature_2m"].max()
                ),
                "min_temp_c": float(
                    df["temperature_2m"].min()
                ),
                "avg_temp_c": float(
                    df["temperature_2m"].mean()
                ),
                "high_rain_chance_hours": int(
                    (
                        df["precipitation_probability"] > 50
                    ).sum()
                ),
                "hot_hours": int(
                    category_counts.get("Hot", 0)
                ),
                "warm_hours": int(
                    category_counts.get("Warm", 0)
                ),
                "cool_hours": int(
                    category_counts.get("Cool", 0)
                )
            }

            logger.info(
                f"Successfully transformed {len(df)} records."
            )

            return {
                "records": df.to_dict("records"),
                "stats": summary_stats
            }

        except Exception as error:
            logger.error(
                f"Transformation failed: {error}"
            )
            raise

    @task
    def load_weather(
        transformed_data: Dict[str, Any],
        execution_date=None
    ) -> str:
        # Save transformed data into CSV and JSON files.
        logger.info("Loading weather data to files..")

        try:
            data_dir = Path(DATA_DIRECTORY)

            data_dir.mkdir(
                parents=True,
                exist_ok=True
            )

            date_str = (
                execution_date.strftime("%Y%m%d")
                if execution_date
                else datetime.now().strftime("%Y%m%d")
            )

            df = pd.DataFrame(
                transformed_data["records"]
            )

            csv_path = (
                data_dir /
                f"weather_{date_str}.csv"
            )

            df.to_csv(
                csv_path,
                index=False
            )

            logger.info(
                f"CSV file saved: {csv_path}"
            )

            json_path = (
                data_dir /
                f"weather_{date_str}.json"
            )

            with open(
                json_path,
                "w",
                encoding="utf-8"
            ) as file:
                json.dump(
                    transformed_data,
                    file,
                    indent=4
                )

            logger.info(
                f"JSON file saved: {json_path}"
            )

            return str(csv_path)

        except Exception as error:
            logger.error(
                f"Load phase failed: {error}"
            )
            raise

    raw_data = extract_weather()

    transformed_data = transform_weather(
        raw_data
    )

    load_weather(
        transformed_data
    )


weather_dag = weather_etl_pipeline()
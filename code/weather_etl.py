"""
Weather ETL Pipeline - Custom DAG

This DAG extracts weather data for Merida, Yucatan from the Open-Meteo API,
transforms the data, and loads it into CSV and JSON formats.

"""

from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
import json
from pathlib import Path

@dag(
    dag_id='weather_etl',
    description='Extracts weather data, transforms temperatures, and loads to files',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['weather', 'etl', 'student']
)
def weather_etl_pipeline():
    
    @task
    def extract_weather() -> dict:
        print("🌤️  Extracting weather data for Merida...")
        
        # Coordinates for Merida, Yucatan
        url = "https://api.open-meteo.com/v1/forecast?latitude=20.9754&longitude=-89.6170&hourly=temperature_2m,apparent_temperature,precipitation_probability&timezone=auto"
        
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            print("✅ Successfully extracted weather data!")
            return data
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data: {e}")
            raise

    @task
    def transform_weather(raw_data: dict) -> dict:
        """
        Transform weather data: convert Celsius to Fahrenheit, 
        add categories, and calculate statistics.
        
        Args:
            raw_data: Raw weather JSON from the API.
            
        Returns:
            dict: Transformed data and summary statistics.
        """
        print("🔄 Transforming weather data with pandas...")
        
        try:
            # Extract hourly data into a DataFrame
            hourly_data = raw_data['hourly']
            df = pd.DataFrame(hourly_data)
            
            # 1. Convert units: Celsius to Fahrenheit
            df['temp_fahrenheit'] = (df['temperature_2m'] * 9/5) + 32
            df['feels_like_f'] = (df['apparent_temperature'] * 9/5) + 32
            
            # 2. Add derived fields: Temperature Category based on Celsius
            def categorize_temp(temp):
                if temp < 20: return 'Cool'
                elif 20 <= temp <= 30: return 'Warm'
                else: return 'Hot'
                
            df['temp_category'] = df['temperature_2m'].apply(categorize_temp)
            
            # 3. Create summary statistics
            summary_stats = {
                "max_temp_c": float(df['temperature_2m'].max()),
                "min_temp_c": float(df['temperature_2m'].min()),
                "avg_temp_c": float(df['temperature_2m'].mean()),
                "high_rain_chance_hours": int((df['precipitation_probability'] > 50).sum())
            }
            
            print(f"✅ Transformed {len(df)} hourly records successfully.")
            
            return {
                "records": df.to_dict('records'),
                "stats": summary_stats
            }
            
        except Exception as e:
            print(f"❌ Error during transformation: {e}")
            raise

    @task
    def load_weather(transformed_data: dict, execution_date=None) -> str:
        """
        Load transformed data to CSV and JSON files in the data directory.
        
        Args:
            transformed_data: The transformed weather records and stats.
            
        Returns:
            str: Path to the saved CSV file.
        """
        print("💾 Loading weather data to files...")
        
        try:
            data_dir = Path('/opt/airflow/data')
            data_dir.mkdir(parents=True, exist_ok=True)
            
            date_str = execution_date.strftime('%Y%m%d') if execution_date else datetime.now().strftime('%Y%m%d')
            
            # Convert records back to DataFrame to save as CSV
            df = pd.DataFrame(transformed_data['records'])
            
            csv_path = data_dir / f'weather_{date_str}.csv'
            df.to_csv(csv_path, index=False)
            print(f"✅ Saved CSV file to: {csv_path}")
            
            # Save the full data (including our stats) to JSON
            json_path = data_dir / f'weather_{date_str}.json'
            with open(json_path, 'w') as f:
                json.dump(transformed_data, f, indent=4)
            print(f"✅ Saved JSON file to: {json_path}")
            
            return str(csv_path)
            
        except Exception as e:
            print(f"❌ Error saving files: {e}")
            raise

    # Define the execution flow
    raw = extract_weather()
    transformed = transform_weather(raw)
    load_weather(transformed)

# Create DAG instance
weather_dag = weather_etl_pipeline()
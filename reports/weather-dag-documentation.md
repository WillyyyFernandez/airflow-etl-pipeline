# Weather ETL DAG Documentation

## API Chosen and Why
For this ETL pipeline, I selected the **Open-Meteo API**. I chose this API because it is free, requires no authentication, and provides highly detailed hourly forecast data. It allowed me to specify exact coordinates to get localized weather data for Mérida, Yucatán, making the data extraction much more relevant and interesting to analyze.

## Transformations Applied
During the transform phase, I utilized `pandas` to apply several key operations:
1. **Unit Conversion:** The raw data provides temperatures in Celsius. I created new columns (`temp_fahrenheit` and `feels_like_f`) to convert both the actual and apparent temperatures into Fahrenheit using vectorization.
2. **Data Categorization:** I added a derived field called `temp_category` by applying a custom function that classifies the hourly weather as 'Cool', 'Warm', or 'Hot' based on the Celsius temperature.
3. **Summary Statistics:** I generated a dictionary of analytics, including the maximum, minimum, and average temperatures for the forecast, as well as a count of how many hours have a precipitation probability greater than 50%.

## Challenges Faced and Solutions
During the development and testing of the pipeline, I encountered a couple of infrastructure challenges:
* **Port Allocation Error:** When starting the Docker containers, the Airflow webserver failed to bind to port 8080 (and later 8081) because they were already allocated by other background processes. I solved this by modifying the `docker-compose.yml` file to map the webserver to port `8082:8080`.
* **DAG Not Appearing:** Initially, my custom DAG did not show up in the Airflow UI. I realized I had saved the `weather_etl.py` file in a backup folder instead of the active mapped volume. Moving the file to the correct `dags/` directory allowed the scheduler to detect it immediately.

## Interesting Insights
By extracting data specifically for Mérida, the generated statistics clearly reflect the local climate. The transformation phase automatically categorized most of the daylight hours as 'Hot', and the summary statistics confirmed a high average temperature with minimal hours showing a high probability of rain, aligning perfectly with the expected weather patterns for the region.
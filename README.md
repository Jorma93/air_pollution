# air_pollution
Creation of a pipeline to monitor air quality data from OpenAQ.
Technologies used: Docker, Apache Airflow DAG, Python, Postgre.
The database contains one table with air monitor parameters with the hourly value of the metrics and also the calculation of PM2.5AQI and PM10AQI for 24-hour rolling average.
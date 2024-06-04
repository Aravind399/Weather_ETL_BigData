# Weather Data ETL Pipeline with Apache Airflow

## Overview
This project demonstrates an ETL (Extract, Transform, Load) pipeline for weather data using Apache Airflow. The pipeline automates the following tasks:
- **Data Extraction:** Fetches weather forecast data from the WeatherAPI.
- **Data Transformation:** Converts JSON data to CSV format and performs additional data transformations.
- **Data Loading:** Loads the transformed data into MySQL and MongoDB databases.
- **Data Archiving:** Archives raw and processed data files.
- **Data Visualization:** Generates visualizations using Streamlit.

## Project Structure
- **DAGs**
  - Contains the Apache Airflow DAG file (`weather_project.py`) defining the workflow.
- **Scripts**
  - Python scripts for data extraction, transformation, and visualization.
- **Archive**
  - Directory for storing archived data files.
- **Logs**
  - Directory for storing logs generated during pipeline execution.

## Prerequisites
Ensure you have the following installed and configured:
- Python 3.x
- Apache Airflow
- MySQL Server
- MongoDB
- Streamlit
- WeatherAPI API Key

## Setup Instructions

1. **Clone the Repository**
    ```bash
    git clone https://github.com/yourusername/weather-data-etl-pipeline.git
    cd weather-data-etl-pipeline
    ```

2. **Install Python Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

3. **Configure Airflow**
    - Initialize the Airflow database:
      ```bash
      airflow db init
      ```
    - Create an Airflow user:
      ```bash
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
      ```
    - Start the Airflow web server and scheduler in separate terminals:
      ```bash
      airflow webserver --port 8080
      airflow scheduler
      ```

4. **Set Up Databases**
    - Ensure MySQL and MongoDB servers are running.
    - Create necessary databases and tables/collections in MySQL and MongoDB.

5. **Configure API Key**
    - Set your WeatherAPI API Key in the appropriate script or as an environment variable.

6. **Run the Pipeline**
    - Access the Airflow web interface at `http://localhost:8080`.
    - Trigger the DAG `weather_project` to start the ETL process.

7. **View Visualizations**
    - Run the Streamlit app to generate visualizations:
      ```bash
      streamlit run scripts/visualization.py
      ```

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## Acknowledgements
Special thanks to the developers of Apache Airflow, MySQL, MongoDB, Streamlit, and WeatherAPI for their excellent tools and services.

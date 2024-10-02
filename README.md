# Flight Data Pipeline

This project implements a data pipeline for tracking flight data using the OpenSky API, Apache Spark, and Databricks.

## Project Structure

flight_data_pipeline/
├── notebooks/
│ ├── 00_setup.py
│ ├── 01_extract.py
│ ├── 02_transform.py
│ ├── 03_load.py
│ └── 04_analysis.py
├── src/
│ ├── extract/
│ │ └── opensky_api.py
│ ├── transform/
│ │ └── flight_data_transformer.py
│ └── load/
│ └── delta_lake_loader.py
├── tests/
│ └── test_opensky_api.py
├── config/
│ └── config.yaml
├── requirements.txt
└── README.md

## Setup

1. Clone this repository to your local machine or Databricks workspace.
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. If using Databricks, create a new cluster or use an existing one.
4. Upload the project files to your Databricks workspace or use Databricks Repos to connect this GitHub repository.

## Usage

1. Run the notebooks in order:
   - `00_setup.py`: Sets up the environment and configurations
   - `01_extract.py`: Extracts data from the OpenSky API
   - `02_transform.py`: Transforms the raw flight data
   - `03_load.py`: Loads the transformed data into Delta Lake
   - `04_analysis.py`: Performs analysis on the processed data

2. Schedule the pipeline using Databricks Jobs for regular execution.

## Configuration

Adjust the `config/config.yaml` file to modify any project-specific settings.

## Testing

Run the tests using pytest:
pytest tests/



## Acknowledgments

- OpenSky Network for providing the flight data API
- Apache Spark and Databricks for the data processing infrastructure
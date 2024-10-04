# Flight Data Pipeline (IN PROGRESS...)

This project implements a data pipeline for tracking flight data using the OpenSky API, Apache Airflow, Kafka, and AWS S3.

## Project Structure
```
flight_data_pipeline/
├── dags/
│ ├── opensky_dag.py
│ ├── opensky_producer.py
  └── opensky_consumer.py

```

## Setup

1. Clone this repository to your local machine.
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Ensure Docker and Docker Compose are installed on your machine.
4. Start the Airflow and Kafka services using Docker Compose:
   ```
   docker-compose up -d
   ```

## Usage

1. Access the Airflow web interface at `http://localhost:8080` to manage and monitor your DAGs.
2. The `opensky_dag.py` DAG will:
   - Use `opensky_producer.py` to fetch data from the OpenSky API and produce messages to Kafka.
   - Use `opensky_consumer.py` to consume messages from Kafka and store them in AWS S3.

## Configuration

- Adjust the `config/config.yaml` file to modify any project-specific settings.
- Ensure your AWS credentials are configured correctly for S3 access.

## Testing

Run the tests using pytest:
```
pytest tests/
```

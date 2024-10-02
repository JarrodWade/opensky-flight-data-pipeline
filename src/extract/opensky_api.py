import requests
import json
from pyspark.sql import SparkSession

def fetch_flight_data(spark, time_interval=0):
    """
    Fetch flight data from OpenSky API
    :param spark: SparkSession
    :param time_interval: Time interval in seconds (0 for current)
    :return: Spark DataFrame with flight data
    """
    url = f"https://opensky-network.org/api/states/all?time={time_interval}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        # Convert the JSON data to a Spark DataFrame
        return spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("OpenSkyDataFetch").getOrCreate()
    df = fetch_flight_data(spark)
    df.show()
    spark.stop()
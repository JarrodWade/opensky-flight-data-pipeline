from kafka import KafkaProducer
import json
import requests
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    url = "https://opensky-network.org/api/states/all"
    
    try:
        while True:
            try:
                response = requests.get(url)
                response.raise_for_status()  # Raises an HTTPError for bad responses
                data = response.json()
                timestamp = data['time']
                us_flights = [state for state in data['states'] if state[2] == 'United States' and not state[8]]
                
                for state in us_flights:
                    flight_data = {
                        'timestamp': timestamp,
                        'icao24': state[0],
                        'callsign': state[1],
                        'origin_country': state[2],
                        'time_position': state[3],
                        'last_contact': state[4],
                        'longitude': state[5],
                        'latitude': state[6],
                        'baro_altitude': state[7],
                        'on_ground': state[8],
                        'velocity': state[9],
                        'true_track': state[10],
                        'vertical_rate': state[11],
                        'sensors': state[12],
                        'geo_altitude': state[13],
                        'squawk': state[14],
                        'spi': state[15],
                        'position_source': state[16]
                    }
                    producer.send('opensky_topic', flight_data)
                
                logging.info(f"Sent data for {len(us_flights)} US aircraft at {datetime.fromtimestamp(timestamp)}")
                time.sleep(10)
            
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching data: {e}")
                time.sleep(10)
            
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                time.sleep(10)
    
    finally:
        producer.close()

if __name__ == "__main__":
    kafka_producer()
from kafka import KafkaConsumer
import json
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def s3_consumer():
    consumer = KafkaConsumer(
        'opensky_topic',
        bootstrap_servers=['kafka:29092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    s3 = boto3.client('s3')
    bucket_name = 'your-bucket-name'

    buffer = []
    buffer_size = 1000

    try:
        for message in consumer:
            buffer.append(message.value)
            
            if len(buffer) >= buffer_size:
                df = pa.Table.from_pylist(buffer)
                
                timestamp = datetime.now()
                key = f"raw_data/{timestamp.strftime('%Y/%m/%d/%H')}/batch_{timestamp.strftime('%Y%m%d%H%M%S')}.parquet"
                
                with BytesIO() as f:
                    pq.write_table(df, f)
                    f.seek(0)
                    s3.put_object(Bucket=bucket_name, Key=key, Body=f.getvalue())
                
                logging.info(f"Uploaded batch to {key}")
                buffer.clear()
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    finally:
        consumer.close()

if __name__ == "__main__":
    s3_consumer()
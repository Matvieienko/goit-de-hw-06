from kafka import KafkaProducer
from configs import kafka_config, my_name
from datetime import datetime
import json
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
topic_name = f"{my_name}_building_sensors"

sensor_id = random.randint(10000, 99999)

print(f"Start sending data for sensor_id={sensor_id} to topic '{topic_name}'")

try:
    for i in range(200):
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": random.randint(25, 45), 
            "humidity": random.randint(15, 85),
        }

        producer.send(topic_name, key=str(sensor_id), value=data)
        producer.flush()

        print(f"[{i}] Sent message: {data}")
        time.sleep(2)

except KeyboardInterrupt:
    print("Stopped by user (Ctrl+C).")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()
    print("Producer closed.")
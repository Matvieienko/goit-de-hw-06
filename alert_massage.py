from kafka import KafkaConsumer
from configs import kafka_config, my_name
import json

# Топіки
temperature_alerts_topic = f"{my_name}_temperature_alerts"
humidity_alerts_topic = f"{my_name}_humidity_alerts"

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    auto_offset_reset="earliest",  
    enable_auto_commit=True,
    group_id=f"{my_name}_spark_alerts_consumer_fixed_v2",
)

consumer.subscribe([temperature_alerts_topic, humidity_alerts_topic])

print("Subscribed to alert topics:")
print(f"  - {temperature_alerts_topic}")
print(f"  - {humidity_alerts_topic}")
print("Waiting for alerts...\n")

try:
    for message in consumer:
        data = message.value

        window_start = data.get("window_start")
        window_end = data.get("window_end")
        
        avg_temp = data.get("avg_temp") 
        avg_hum = data.get("avg_humidity")
        
        code = data.get("code")
        msg = data.get("message")
        
        # Форматуємо вивід
        print("-" * 60)
        print(f"Topic:      {message.topic}")
        print(f"Key:        {message.key}")
        print(f"Window:     {window_start} -> {window_end}")
        print(f"Avg temp:   {avg_temp}")
        print(f"Avg hum:    {avg_hum}")
        print(f"Code:       {code}")
        print(f"Message:    {msg}")
        print("-" * 60)

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, my_name

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

building_topic = f"{my_name}_building_sensors"
temperature_alerts_topic = f"{my_name}_temperature_alerts"
humidity_alerts_topic = f"{my_name}_humidity_alerts"

num_partitions = 2
replication_factor = 1

new_topics = [
    NewTopic(name=building_topic, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name=temperature_alerts_topic, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name=humidity_alerts_topic, num_partitions=num_partitions, replication_factor=replication_factor),
]

try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print("Topics created successfully.")
except Exception as e:
    print(f"An error occurred while creating topics: {e}")

[print(topic) for topic in admin_client.list_topics() if my_name in topic]

admin_client.close()

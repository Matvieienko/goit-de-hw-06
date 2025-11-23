import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp, to_json, struct, when

# Импорт конфигов
from configs import kafka_config, my_name

# --- НАСТРОЙКА ОКРУЖЕНИЯ ---
# Используем версию 3.3.2, которая совместима с твоим окружением
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 pyspark-shell'

# --- НАЗВАНИЯ ТОПИКОВ (ИЗ CREATE_TOPIC.PY) ---
input_topic = f"{my_name}_building_sensors"
temp_alerts_topic = f"{my_name}_temperature_alerts"
hum_alerts_topic = f"{my_name}_humidity_alerts"

print(f"Input Topic: {input_topic}")
print(f"Output Temp: {temp_alerts_topic}")
print(f"Output Hum:  {hum_alerts_topic}")

# --- 1. ЗАПУСК SPARK SESSION ---
spark = SparkSession.builder \
    .appName("IoT_Streaming_Final_Fixed") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# --- 2. ЧТЕНИЕ И ОБРАБОТКА CSV (УСЛОВИЯ) ---
# Явно задаем схему, чтобы числа были числами
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", IntegerType(), True),
    StructField("humidity_max", IntegerType(), True),
    StructField("temperature_min", IntegerType(), True),
    StructField("temperature_max", IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True)
])

alerts_df = spark.read \
    .option("header", "true") \
    .schema(alerts_schema) \
    .csv("alerts_conditions.csv")

# Заменяем -999 на NULL для корректной работы условий > и <
alerts_df = alerts_df \
    .withColumn("temperature_min", when(col("temperature_min") == -999, None).otherwise(col("temperature_min"))) \
    .withColumn("temperature_max", when(col("temperature_max") == -999, None).otherwise(col("temperature_max"))) \
    .withColumn("humidity_min", when(col("humidity_min") == -999, None).otherwise(col("humidity_min"))) \
    .withColumn("humidity_max", when(col("humidity_max") == -999, None).otherwise(col("humidity_max")))

# --- 3. ЧТЕНИЕ KAFKA STREAM ---
# Схема соответствует твоему sensor_producer.py (timestamp там СТРОКА!)
sensor_schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("timestamp", StringType()),  # ВАЖНО: Producer шлет строку "2023-..."
    StructField("temperature", IntegerType()),
    StructField("humidity", IntegerType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(kafka_config['bootstrap_servers'])) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Парсим JSON и преобразуем строку времени в Timestamp
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), sensor_schema).alias("data")
).select(
    col("data.sensor_id"),
    col("data.temperature"),
    col("data.humidity"),
    to_timestamp(col("data.timestamp"), "yyyy-MM-dd HH:mm:ss").alias("event_time") # Парсинг формата из producer
)

# --- 4. АГРЕГАЦИЯ (СКОЛЬЗЯЩЕЕ ОКНО) ---
windowed_df = parsed_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    )

# --- 5. JOIN С УСЛОВИЯМИ И ФИЛЬТРАЦИЯ ---
joined_df = windowed_df.crossJoin(alerts_df)

# Логика: Если лимит задан (не null) И значение вышло за него -> Оставляем
filter_condition = (
    (col("temperature_min").isNotNull() & (col("avg_temp") < col("temperature_min"))) |
    (col("temperature_max").isNotNull() & (col("avg_temp") > col("temperature_max"))) |
    (col("humidity_min").isNotNull() & (col("avg_humidity") < col("humidity_min"))) |
    (col("humidity_max").isNotNull() & (col("avg_humidity") > col("humidity_max")))
)

alerts_result = joined_df.filter(filter_condition)

# Подготовка JSON для отправки
output_df = alerts_result.select(
    col("sensor_id").cast("string").alias("key"),
    to_json(struct(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temp"),
        col("avg_humidity"),
        col("code"),
        col("message")
    )).alias("value"),
    col("code") # Оставляем код колонкой, чтобы по нему разделить потоки
)

# --- 6. ЗАПИСЬ В РАЗНЫЕ ТОПИКИ ---

# Поток 1: Температурные алерты (коды 103, 104)
query_temp = output_df.filter((col("code") == 103) | (col("code") == 104)) \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(kafka_config['bootstrap_servers'])) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("topic", temp_alerts_topic) \
    .option("checkpointLocation", "/tmp/checkpoints/temp_alerts_fixed") \
    .start()

# Поток 2: Алерты влажности (коды 101, 102)
query_hum = output_df.filter((col("code") == 101) | (col("code") == 102)) \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(kafka_config['bootstrap_servers'])) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("topic", hum_alerts_topic) \
    .option("checkpointLocation", "/tmp/checkpoints/hum_alerts_fixed") \
    .start()

print("Streaming started properly. Waiting for data...")
spark.streams.awaitAnyTermination()
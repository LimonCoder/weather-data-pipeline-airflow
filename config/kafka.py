from kafka import KafkaProducer
from kafka import KafkaConsumer
import json

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094', 'localhost:9095', 'localhost:9096']


def init_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
    )


def init_consumer(topic, consumer_group_id: str = 'weather_monitor_group'):
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda m: m.decode('utf-8'),
        enable_auto_commit=False,
        group_id=consumer_group_id
    )

from kafka import KafkaConsumer
import redis
import json
from config import settings


r = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, charset='utf-8', decode_responses=True)

consumer = KafkaConsumer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
                         auto_offset_reset='latest', )
consumer.subscribe([settings.KAFKA_NER_TOPIC, settings.KAFKA_KEYWORDS_TOPIC])


def write_to_redis_table(self, table, timestamp, data):
    data = {k: " ".join(v) for k, v in data.items()}
    r.hset(f"{table}:{timestamp}", data)

def consume():
    while True:
        messages = consumer.poll()
        for topic, messages in messages.items():
            if topic.topic == settings.KAFKA_NER_TOPIC:
                for msg in messages:
                    write_to_redis_table(settings.KAFKA_NER_TOPIC, msg.timestamp, msg.value)
            elif topic.topic == settings.KAFKA_KEYWORDS_TOPIC:
                for msg in messages:
                    write_to_redis_table(settings.KAFKA_KEYWORDS_TOPIC, msg.timestamp, msg.value)


if __name__ == '__main__':
    consume()
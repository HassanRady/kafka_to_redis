from confluent_kafka import Consumer, KafkaError, KafkaException
import redis
import json

from logger import get_file_logger
from config import settings

logger = get_file_logger(__name__, "logs")

r = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, charset='utf-8', decode_responses=True)

consumer_conf = {
    'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
    'group.id': settings.KAFKA_REDIS_CONSUMER_GROUP,
    'auto.offset.reset': settings.KAFKA_AUTO_OFFSET_RESET,
}
consumer = Consumer(consumer_conf)


def value_deserializer(value):
    return json.loads(value.decode('utf-8'))

def write_to_redis(table, data):
    r.lpush(table, json.dumps(data,).encode('utf-8'))

running = True

def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        while running:
            msg = consumer.poll()
            if msg is None: 
                 continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.error('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                if msg.value() is None:
                    continue
                elif msg.topic() == settings.KAFKA_NER_TOPIC:
                        write_to_redis(settings.KAFKA_NER_TOPIC, value_deserializer(msg.value()))
                elif msg.topic() == settings.KAFKA_KEYWORDS_TOPIC:
                        write_to_redis(settings.KAFKA_KEYWORDS_TOPIC, value_deserializer(msg.value()))
                elif msg.topic() == settings.KAFKA_SENTIMENT_TOPIC:
                        write_to_redis(settings.KAFKA_SENTIMENT_TOPIC, value_deserializer(msg.value()))
                elif msg.topic() == settings.KAFKA_EMOTION_TOPIC:
                        write_to_redis(settings.KAFKA_EMOTION_TOPIC, value_deserializer(msg.value()))
    finally:
        consumer.close()

def shutdown():
    running = False


if __name__ == '__main__':
    consume_loop(consumer, [settings.KAFKA_NER_TOPIC, settings.KAFKA_KEYWORDS_TOPIC, settings.KAFKA_SENTIMENT_TOPIC, settings.KAFKA_EMOTION_TOPIC])
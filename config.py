from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str
    REDIS_HOST: str
    REDIS_PORT: int
    KAFKA_NER_TOPIC: str
    KAFKA_KEYWORDS_TOPIC: str
    KAFKA_SENTIMENT_TOPIC: str
    KAFKA_EMOTION_TOPIC: str
    KAFKA_REDIS_CONSUMER_GROUP: str
    KAFKA_AUTO_OFFSET_RESET: str


@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()
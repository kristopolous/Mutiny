import json
import redis
from config import REDIS_URL

_client = None

def get_redis_client():
    global _client
    if _client is None:
        _client = redis.from_url(REDIS_URL)
    return _client

def cache_get(key):
    client = get_redis_client()
    value = client.get(key)
    if value:
        return json.loads(value)
    return None

def cache_set(key, value, ttl=604800):  # 7 days in seconds
    client = get_redis_client()
    client.setex(key, ttl, json.dumps(value))
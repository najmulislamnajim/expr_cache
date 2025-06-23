import redis 

# connect to redis
try:
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    r= redis.Redis(connection_pool=redis_pool)
    print('Connected to Redis')
except Exception as e:
    print(f'Failed to connect to Redis: {e}')
    
cache_key = '14054859_14000398_6002025'
data = r.hgetall(cache_key)
print(data['billing_doc_no'])
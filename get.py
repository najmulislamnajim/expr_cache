import redis, sys 

# Ensure two dates are passed
if len(sys.argv) != 2:
    print("Usage: python main.py cache_key")
    sys.exit(1)

cache_key = sys.argv[1]

# connect to redis
try:
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    r= redis.Redis(connection_pool=redis_pool)
    print('Connected to Redis')
except Exception as e:
    print(f'Failed to connect to Redis: {e}')
    
data = r.hgetall(cache_key)
print(data['billing_doc_no'])
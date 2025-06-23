import redis, zlib, json, sys

# Ensure cache key passed
if len(sys.argv) != 2:
    print("Usage: python main.py cache_key")
    sys.exit(1)

key = sys.argv[1]

# connect to redis
try:
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    r= redis.Redis(connection_pool=redis_pool)
    print('Connected to Redis')
except Exception as e:
    print(f'Failed to connect to Redis: {e}')
def decompress_value(compressed_data):
    decompressed = zlib.decompress(compressed_data).decode('utf-8')
    return json.loads(decompressed)

compressed = r.get(key)
if compressed:
    data = decompress_value(compressed)
    print("Billing Doc No:", data['billing_doc_no'])
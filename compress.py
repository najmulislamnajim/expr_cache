import sys, redis, pymysql, decimal, json, zlib
from datetime import date

# Ensure two dates are passed
if len(sys.argv) != 3:
    print("Usage: python main.py <start_date> <end_date>")
    sys.exit(1)

start_date = sys.argv[1]
end_date = sys.argv[2]

# connect to redis
try:
    redis_pool = redis.ConnectionPool(
        host='127.0.0.1', port=6379, db=0
    )
    r = redis.Redis(connection_pool=redis_pool)
    print('Connected to Redis')
except Exception as e:
    print(f'Failed to connect to Redis: {e}')
    sys.exit(1)

# connect to odms_db
try:
    conn = pymysql.connect(
        host='10.16.49.185',
        user='root',
        password='5w5A0V&eWP',
        database='odms_db',
    )
    cursor = conn.cursor()
    print('Connected to odms_db')
except Exception as e:
    print(f'Failed to connect to odms_db: {e}')
    sys.exit(1)

# query to get all the data from the table
query = f"""
SELECT billing_doc_no, partner, matnr, batch, territory_code, quantity, MAX(billing_date) AS billing_date
FROM rpl_sales_info_sap
WHERE billing_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY partner, matnr, batch;
"""

# execute the query
cursor.execute(query)
results = cursor.fetchall()
print('Query executed successfully')

# close the cursor and connection
cursor.close()
conn.close()

# converter to convert decimal to float and datetime to string
def converter(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj

# Compress value using zlib
def compress_value(value_dict):
    json_data = json.dumps(value_dict, default=converter).encode('utf-8')
    return zlib.compress(json_data)

# Cache the data in Redis
for result in results:
    billing_doc_no, partner, matnr, batch, territory_code, quantity, billing_date = result
    cache_key = f"{partner}_{matnr}_{batch}"

    value = {
        'billing_doc_no': billing_doc_no,
        'partner': partner,
        'matnr': matnr,
        'batch': batch,
        'territory_code': territory_code,
        'quantity': float(quantity),
        'billing_date': billing_date.isoformat()
    }

    compressed_data = compress_value(value)
    r.set(cache_key, compressed_data)
    print(f"Cached {cache_key} ({len(compressed_data)} bytes)")
print("Data caching completed.")
import sys, redis, pymysql, decimal
import pymysql
import decimal 
from datetime import date

# Ensure two dates are passed
if len(sys.argv) != 3:
    print("Usage: python main.py <start_date> <end_date>")
    sys.exit(1)

# Get the start and end dates from the command line arguments
start_date = sys.argv[1]
end_date = sys.argv[2]

# connect to redis
try:
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    r= redis.Redis(connection_pool=redis_pool)
    print('Connected to Redis')
except Exception as e:
    print(f'Failed to connect to Redis: {e}')
    
# connect to odms_db
try:
    conn = pymysql.connect(
        host='10.16.49.185',
        user='root',
        password='5w5A0V&eWP',
        database='odms_db',
    )
    cursor = conn.cursor()
    print('Connected to MySQL')
except Exception as e:
    print(f'Failed to connect to MySQL: {e}')
    
# query to get all the data from the table
query = f"""SELECT billing_doc_no, partner, matnr, batch, territory_code, quantity, MAX(billing_date) AS latest_date
FROM rpl_sales_info_sap
WHERE billing_date BETWEEN '{start_date}' AND '{end_date}' 
GROUP BY partner, matnr, batch;"""

# execute the query
cursor.execute(query)
results = cursor.fetchall()

# close the cursor and connection
cursor.close()
conn.close()

# converter to convert decimal to float and datetime to string
def converter(obj):
    if isinstance(obj, date):
        return obj.isoformat()  # Convert date to string (YYYY-MM-DD format)
    if isinstance(obj, decimal.Decimal):
        return float(obj)  # Convert decimal to float
    return obj

# cache the data in redis
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
    r.hset(cache_key, mapping=value)
    print(f"Cached {cache_key}")
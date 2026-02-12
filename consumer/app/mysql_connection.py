import os
from mysql.connector import connect

HOST = os.getenv("MYSQL_HOST", "localhost")
USER = os.getenv("MYSQL_USER", "root")
PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
DB = os.getenv("MYSQL_DB", "test_week_17")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

def get_conn():
    return connect(host=HOST, user=USER, password=PASSWORD, database=DB)





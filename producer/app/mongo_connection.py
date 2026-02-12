from pymongo import MongoClient
import os


uri = os.getenv("MONGO_HOST", "mongodb://localhost:27017")
db_name = os.getenv("DB_NAME", "test1_week_17")

client = MongoClient(uri)

db = client[db_name]

def get_col():
    return db["transactions"]
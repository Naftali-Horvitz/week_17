import json
import logging
import os
from sqls import *


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_config():
    bootstrap = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    return {
        "bootstrap.servers": bootstrap,
        "group.id": "transactions-consumer",
        "auto.offset.reset": "earliest",
    } 


def pull_msg(consumer):
    msg = consumer.poll(1.0)
    
    if msg is None:
        logger.error(f"❌ msg error: not data in message")
        return

    data = json.loads(msg.value().decode("utf-8"))
    logger.info(f"📦 Received user: {data}")
    
    try:
        insert_item(data)
    except Exception as e:
        logger.info("🔴 Saving to SQL failed. %s", str(e))


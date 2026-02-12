import logging
from mysql_connection import get_conn
from sqls import *
import kafka_consumer 
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def main():
    try:
        customers = create_customer_table()
        orders = create_order_table()
        my_conn = get_conn()
        cursor = my_conn.cursor()
        cursor.execute(customers)
        my_conn.commit()
        cursor.execute(orders)
        my_conn.commit()
        my_conn.close()
        config = kafka_consumer.get_config()

        consumer = Consumer(config)
        consumer.subscribe(["transactions"])
        if consumer:
            logger.info("🟢 Consumer is running and subscribed to transactions topic %s")
        while True:
            kafka_consumer.pull_msg(consumer)
            
    except KeyboardInterrupt:
        logger.info("🔴 Stopping consumer")
if __name__ == "__main__":

    
    main()
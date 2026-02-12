import json
from mongo_connection import get_col
from kafka_publisher import send_msg
from time import sleep
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_file(path: str):
    with open(path, "r") as file:
        return json.load(file)

  
def init_data(path: str, my_col):
    data = load_file(path)
    my_col.insert_many(data)


def load_data(my_col, num_skip, num_limit):
    return list(my_col.find({},{'_id': 0}).skip(num_skip).limit(num_limit))


def main():
    
    NUM_PULLS = 50
    topic = "transactions"
    
    my_col = get_col()
    
    # first running
    
    path = "suspicious_customers_orders.json"
    init_data(path, my_col)
    
    num_skip, num_limit = 0, NUM_PULLS
    
    while True:
        data = load_data(my_col, num_skip, num_limit)
        num_skip +=  NUM_PULLS
                
        if not data:
            logger.info("✅ Sending to Kafka is complete.✅")
            break
        
        for item in data:
            send_msg(topic, json.dumps(item).encode("utf-8"))
            sleep(0.5)





if __name__ == "__main__":
    main()
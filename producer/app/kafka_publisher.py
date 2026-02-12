from confluent_kafka import Producer
import os
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


bootstrap = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")


producer_config = {
    "bootstrap.servers": bootstrap,
}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"❌ Delivery failed: {err}")
    else:
        logger.info(f"✅ Delivered to {msg.topic()} ")


def send_msg(topic, data):
    try:
        producer.produce(
            topic,
            value=data,
            on_delivery=delivery_report
        )

        producer.poll(0)
        producer.flush()
        
    except Exception as e:
        logger.error(f"Kafka produce error: {e}")
        raise

# consumers/kafka_consumer_kabore.py
"""
kafka_consumer_kabore.py

Consume messages from a Kafka topic and process them.
(Aligned to the denisecase template: utils.utils_consumer + utils.utils_logger)
"""

#####################################
# Import Modules
#####################################

# Python Standard Library
import os
import json

# External packages
from dotenv import load_dotenv

# Local modules (from your fork)
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "kabore-group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Message Processing
#####################################


def _try_parse_json(s: str):
    """Return dict if s is JSON, else None."""
    s = (s or "").strip()
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def process_message(message: str) -> None:
    """
    Process a single message.

    - Logs every message
    - If JSON with hr/gait_score/steps, raise simple alerts
    - If plain text, still supports the class “special message” alert
    """
    logger.info(f"Processing message: {message}")

    evt = _try_parse_json(message)
    if evt is not None:
        hr = evt.get("hr")
        gait = evt.get("gait_score")
        steps = evt.get("steps")
        pid = evt.get("patient_id")
        ts = evt.get("ts")

        if isinstance(hr, int) and hr > 120:
            alert = f"ALERT Tachycardia: patient={pid} hr={hr} ts={ts}"
            print(alert); logger.warning(alert)

        if isinstance(gait, (int, float)) and float(gait) < 0.40:
            alert = f"ALERT Low gait_score: patient={pid} gait_score={gait} ts={ts}"
            print(alert); logger.warning(alert)

        if isinstance(steps, int) and steps > 12:
            alert = f"ALERT Step surge: patient={pid} steps={steps} ts={ts}"
            print(alert); logger.warning(alert)
        return

    # Plain-text path (matches the class example)
    special = "I just loved a movie! It was funny."
    if special in message:
        alert = f"ALERT: The special message was found!\n{message}"
        print(alert); logger.warning(alert)


#####################################
# Main
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads topic & group id from env
    - Creates consumer via utils.create_kafka_consumer
    - Polls and processes messages
    """
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: topic='{topic}', group='{group_id}'")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value  # utils sets deserializer; should be str
            logger.debug(f"Received offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()

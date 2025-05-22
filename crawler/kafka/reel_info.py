import os
import json
from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kol-reel-topic")
SOURCE_DIR = [
    # os.path.join(os.getcwd(), "10052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "11052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "12052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "13052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "14052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "15052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "16052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "17052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "19052025", "reel", "reel_info"),
    # os.path.join(os.getcwd(), "21052025", "reel", "reel_info"),
    os.path.join(os.getcwd(), "20052025", "reel", "reel_info"),    
]

producer = Producer({
    'bootstrap.servers': KAFKA_BROKERS,
    'linger.ms': 10,
    'acks': 'all'
})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Failed to deliver {msg.key().decode('utf-8')}: {err}")
    else:
        print(f"✅ Delivered {msg.key().decode('utf-8')} to {msg.topic()} [{msg.partition()}]")

def process_file(file_path):
    file_name = os.path.basename(file_path)
    if not file_name.endswith(".json"):
        return False

    key = os.path.splitext(file_name)[0]
    if "_" not in key:
        print(f"⚠️ Skipped malformed filename: {file_name}")
        return False

    page_id, reel_id = key.split("_", 1)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["page_id"] = page_id
        data["reel_id"] = reel_id
        data["type"] = "reel"

        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=json.dumps(data, ensure_ascii=False),
            callback=delivery_report
        )
        return True

    except Exception as e:
        print(f"❌ Error in {file_name}: {e}")
        return False

def produce_reel_files():
    count = 0
    for folder in SOURCE_DIR:
        if not os.path.exists(folder):
            print(f"⚠️ Source directory does not exist: {folder}")
            continue

        for file_name in os.listdir(folder):
            full_path = os.path.join(folder, file_name)
            if os.path.isfile(full_path):
                if process_file(full_path):
                    count += 1

    producer.flush()
    print(f"🔁 Done. Total reel files sent: {count}")

if __name__ == "__main__":
    print(f"🚀 Producing reel_info from folder: {SOURCE_DIR}")
    produce_reel_files()

import os
import json
from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "kol-post-topic")

base_info = os.path.join(os.getcwd(), "info")
SOURCE_DIRS = []
if os.path.isdir(base_info):
    for date_dir in os.listdir(base_info):
        date_path = os.path.join(base_info, date_dir)
        if not os.path.isdir(date_path):
            continue
        for content_type in ("post", "video"):
            dir_path = os.path.join(date_path, content_type, "post_info")
            if os.path.isdir(dir_path):
                SOURCE_DIRS.append((dir_path, content_type))

producer = Producer({
    'bootstrap.servers': KAFKA_BROKERS,
    'linger.ms':        10,
    'acks':             'all',
})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Failed to deliver {msg.key().decode('utf-8')}: {err}")
    else:
        print(f"✅ Delivered {msg.key().decode('utf-8')} to {msg.topic()} [{msg.partition()}]")

def process_file(file_path, type_label="post"):
    """Đọc JSON, gán thêm page_id/post_id/type, và produce"""
    file_name = os.path.basename(file_path)
    if not file_name.endswith(".json"):
        return False

    key = os.path.splitext(file_name)[0]
    if "_" not in key:
        print(f"⚠️ Skipped malformed filename: {file_name}")
        return False

    page_id, post_id = key.split("_", 1)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["page_id"] = page_id
        data["post_id"] = post_id
        data["type"]    = type_label

        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=json.dumps(data, ensure_ascii=False),
            callback=delivery_report
        )
        producer.poll(0)
        return True

    except Exception as e:
        print(f"❌ Error in {file_name}: {e}")
        return False

def produce_post_info():
    total = 0
    print("🚀 Producing post_info from folders:")
    for folder, label in SOURCE_DIRS:
        print(f" - {folder} (as {label})")
        for fn in os.listdir(folder):
            full = os.path.join(folder, fn)
            if os.path.isfile(full) and process_file(full, label):
                total += 1

    producer.flush()
    print(f"🔁 Done. Total post_info files sent: {total}")

if __name__ == "__main__":
    produce_post_info()

import os
import json
from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "kol-post-topic")
BASE_INFO = os.path.abspath("info")

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

def produce_post_info(source_dirs):
    total = 0
    print("🚀 Producing post_info from folders:")
    for folder, label in source_dirs:
        print(f" - {folder} (as {label})")
        for fn in os.listdir(folder):
            full = os.path.join(folder, fn)
            if os.path.isfile(full) and process_file(full, label):
                total += 1

    producer.flush()
    print(f"🔁 Done. Total post_info files sent: {total}")

def main(current_timestamp):
    source_dirs = []
    for content_type in ("post", "video"):
        dir_path = os.path.join(BASE_INFO, str(current_timestamp), content_type, "post_info")
        if os.path.isdir(dir_path):
            source_dirs.append((dir_path, content_type))

    if not source_dirs:
        print(f"⚠️ No valid post_info directories found for timestamp: {current_timestamp}")
    else:
        produce_post_info(source_dirs)

if __name__ == "__main__":
    test_timestamp = "1749832796"
    main(test_timestamp)

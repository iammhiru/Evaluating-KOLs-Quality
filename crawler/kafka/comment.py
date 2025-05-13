import os
import json
from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kol-comment-topic")
SOURCE_DIRS = [
    os.path.join(os.getcwd(), "10052025", "reel", "comment"),
    os.path.join(os.getcwd(), "10052025", "post", "comment"),
    os.path.join(os.getcwd(), "10052025", "video", "comment")
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

def process_file(file_path, post_type):
    file_name = os.path.basename(file_path)
    if not file_name.endswith(".json"):
        return False

    key = os.path.splitext(file_name)[0] 
    parts = key.split("_")
    if len(parts) != 3:
        print(f"⚠️ Skipped malformed filename: {file_name}")
        return False

    page_id, post_id, comment_id = parts

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["page_id"] = page_id
        data["post_id"] = post_id
        data["comment_id"] = comment_id
        data["type"] = "comment"
        data["post_type"] = post_type

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

def produce_comment_files():
    count = 0
    for folder in SOURCE_DIRS:
        if "post/comment" in folder:
            post_type = "post"
        elif "video/comment" in folder:
            post_type = "video"
        elif "reel/comment" in folder:
            post_type = "reel"
        else:
            print(f"⚠️ Unknown folder type: {folder}")
            continue

        for file_name in os.listdir(folder):
            full_path = os.path.join(folder, file_name)
            if os.path.isfile(full_path):
                if process_file(full_path, post_type):
                    count += 1

    producer.flush()
    print(f"🔁 Done. Total comment files sent: {count}")

if __name__ == "__main__":
    print("🚀 Producing comment files from folders:")
    for d in SOURCE_DIRS:
        print(f"   - {d}")
    produce_comment_files()

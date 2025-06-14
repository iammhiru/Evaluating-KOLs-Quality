import os
import json
from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "kol-comment-topic")
BASE_INFO = os.path.abspath("info")

producer = Producer({
    'bootstrap.servers':             KAFKA_BROKERS,
    'linger.ms':                     10,
    'acks':                          'all',
    'queue.buffering.max.messages':  200000, 
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

        data.update({
            "page_id":    page_id,
            "post_id":    post_id,
            "comment_id": comment_id,
            "type":       "comment",
            "post_type":  post_type,
        })

        if data["comment_text"] != "" and data["comment_text"] is not None:
            data["comment_text"] = data["comment_text"].replace("\n", " ")
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

def produce_comment_files(source_dirs):
    count = 0
    for folder, post_type in source_dirs:
        print(f"📂 Processing: {folder} (as {post_type})")
        for file_name in os.listdir(folder):
            full_path = os.path.join(folder, file_name)
            if os.path.isfile(full_path) and process_file(full_path, post_type):
                count += 1

    producer.flush()
    print(f"🔁 Done. Total comment files sent: {count}")

def main(current_timestamp):
    source_dirs = []
    for post_type in ("post", "video", "reel"):
        comment_dir = os.path.join(BASE_INFO, str(current_timestamp), post_type, "comment")
        if os.path.isdir(comment_dir):
            source_dirs.append((comment_dir, post_type))

    if not source_dirs:
        print(f"⚠️ No comment folders found for timestamp: {current_timestamp}")
    else:
        produce_comment_files(source_dirs)

if __name__ == "__main__":
    test_timestamp = "1749832796"
    main(test_timestamp)

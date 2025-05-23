import os
import json
from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "kol-comment-topic")

BASE_INFO = os.path.join(os.getcwd(), "info")
SOURCE_DIRS = []
if os.path.isdir(BASE_INFO):
    for date_dir in os.listdir(BASE_INFO):
        date_path = os.path.join(BASE_INFO, date_dir)
        if not os.path.isdir(date_path):
            continue
        for post_type in ("post", "video", "reel"):
            comment_dir = os.path.join(date_path, post_type, "comment")
            if os.path.isdir(comment_dir):
                SOURCE_DIRS.append(comment_dir)

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
            producer.poll(0)  # phục vụ callback, tránh buffer full
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
            if os.path.isfile(full_path) and process_file(full_path, post_type):
                count += 1

    producer.flush()
    print(f"🔁 Done. Total comment files sent: {count}")

if __name__ == "__main__":
    print("🚀 Producing comment files from folders:")
    for d in SOURCE_DIRS:
        print(f"   - {d}")
    produce_comment_files()

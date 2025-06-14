import os
import json
from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "kol-profile-topic")
BASE_INFO = os.path.abspath("info")

producer = Producer({
    'bootstrap.servers':            KAFKA_BROKERS,
    'queue.buffering.max.messages': 50000, 
})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for {msg.key().decode()}: {err}")
    else:
        print(f"✅ Sent {msg.key().decode()} to {msg.topic()} [{msg.partition()}]")

def produce_profile_files(source_dirs):
    file_count = 0
    for folder in source_dirs:
        for file_name in os.listdir(folder):
            if not file_name.endswith(".json"):
                continue

            key = os.path.splitext(file_name)[0]
            path = os.path.join(folder, file_name)

            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                data.update({
                    "page_id": key,
                    "type":    "profile"
                })

                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=json.dumps(data, ensure_ascii=False),
                    callback=delivery_report
                )
                producer.poll(0) 
                file_count += 1

            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}")

    producer.flush()
    print(f"🔁 Done. Total files sent: {file_count}")

def main(current_timestamp):
    profile_dir = os.path.join(BASE_INFO, str(current_timestamp), "profile")
    if os.path.isdir(profile_dir):
        source_dirs = [profile_dir]
        print(f"🚀 Producing from folder: {profile_dir}")
        produce_profile_files(source_dirs)
    else:
        print(f"⚠️ Folder {profile_dir} does not exist.")

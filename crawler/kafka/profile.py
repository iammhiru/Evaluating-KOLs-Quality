from confluent_kafka import Producer
import os, json

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kol-profile-topic")
SOURCE_DIR = [
    os.path.join(os.getcwd(), "10052025", "profile"),
    os.path.join(os.getcwd(), "11052025", "profile"),
    os.path.join(os.getcwd(), "12052025", "profile"),
    os.path.join(os.getcwd(), "13052025", "profile"),
    os.path.join(os.getcwd(), "14052025", "profile"),
    os.path.join(os.getcwd(), "15052025", "profile"),
    os.path.join(os.getcwd(), "16052025", "profile"),
    os.path.join(os.getcwd(), "17052025", "profile"),
    os.path.join(os.getcwd(), "18052025", "profile"),
    os.path.join(os.getcwd(), "19052025", "profile"),
    os.path.join(os.getcwd(), "21052025", "profile"),
]

producer = Producer({'bootstrap.servers': KAFKA_BROKERS})

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed for {msg.key().decode()}: {err}")
    else:
        print(f"✅ Sent {msg.key().decode()} to {msg.topic()} [{msg.partition()}]")

def produce_profile_files():
    file_count = 0
    for folder in SOURCE_DIR:
        for file_name in os.listdir(folder):
            if not file_name.endswith(".json"):
                continue

            file_path = os.path.join(folder, file_name)
            key = os.path.splitext(file_name)[0]

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                data["page_id"] = key
                data["type"] = "profile"

                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=json.dumps(data, ensure_ascii=False),
                    callback=delivery_report
                )
                file_count += 1

            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}")

    producer.flush()
    print(f"🔁 Done. Total files sent: {file_count}")

if __name__ == "__main__":
    print(f"🚀 Producing from '{SOURCE_DIR}' to topic '{KAFKA_TOPIC}'")
    produce_profile_files()
from confluent_kafka import Producer
import os, json

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kol-profile-topic")
SOURCE_DIR = os.path.join(os.getcwd(), "10052025", "profile")
print("Đường dẫn hiện tại:", SOURCE_DIR)

producer = Producer({'bootstrap.servers': KAFKA_BROKERS})

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed for {msg.key().decode()}: {err}")
    else:
        print(f"✅ Sent {msg.key().decode()} to {msg.topic()} [{msg.partition()}]")

def produce_profile_files():
    file_count = 0
    for file_name in os.listdir(SOURCE_DIR):
        if not file_name.endswith(".json"):
            continue

        file_path = os.path.join(SOURCE_DIR, file_name)
        key = os.path.splitext(file_name)[0]

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.dumps(json.load(f), ensure_ascii=False)

            producer.produce(KAFKA_TOPIC, key=key, value=data, callback=delivery_report)
            file_count += 1

        except Exception as e:
            print(f"❌ Error in {file_name}: {e}")

    producer.flush()
    print(f"🔁 Done. Total files sent: {file_count}")

if __name__ == "__main__":
    print(f"🚀 Producing from '{SOURCE_DIR}' to topic '{KAFKA_TOPIC}'")
    produce_profile_files()
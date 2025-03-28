from kafka import KafkaProducer
import time

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Adjust if necessary (use broker addresses)
    value_serializer=lambda x: x.encode('utf-8')  # Serialize the message as a string
)

# Send test messages to the 'kol_user' topic
for i in range(10):
    message = f"User {i},interaction_{i}"  # Format: User{ID},interaction{ID}
    producer.send('kol_user', value=message)
    print(f"Sent: {message}")
    time.sleep(1)  # Send a message every second

producer.close()

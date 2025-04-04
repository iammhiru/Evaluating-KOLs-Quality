from kafka import KafkaProducer
import time
import json
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for i in range(10):
    message = {
        'id': i,
        'userid': random.randint(1, 100),
        'like': random.randint(0, 500), 
        'comment': random.randint(0, 50), 
        'content': f'Interaction content {i}' 
    }

    producer.send('kol-posts', value=message)
    print(f"Sent: {message}")
    time.sleep(0.5)
producer.close()

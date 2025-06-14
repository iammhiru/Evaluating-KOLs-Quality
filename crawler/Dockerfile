FROM crawler:1.0

RUN pip install flask confluent-kafka

WORKDIR /app

COPY server.py /app/server.py

CMD ["python", "server.py"]

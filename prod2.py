# producer.py
from kafka import KafkaProducer
import json
import random
from time import sleep

# Configura o produtor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Enviando mensagens para o tópico 'kafka-python-topic'... Pressione Ctrl+C para parar.")
try:
    while True:
        value = {"valor": random.randint(1, 999)}
        producer.send('kafka-python-topic', value)
        print(f"Mensagem enviada: {value}")
        producer.flush()
        sleep(1)
except KeyboardInterrupt:
    print("Produtor encerrado.")


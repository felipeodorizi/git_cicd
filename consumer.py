# consumer.py
from kafka import KafkaConsumer
import json

# Configura o consumidor Kafka
consumer = KafkaConsumer(
    'kafka-python-topic',                # nome do tópico
    bootstrap_servers='localhost:9092',  # endereço do broker
    auto_offset_reset='earliest',        # começa a ler do início se não houver offset
    enable_auto_commit=True,             # confirma automaticamente o offset
    group_id='meu-grupo-consumidor',     # identificador do grupo de consumidores
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # transforma bytes em JSON
)

print("Consumindo mensagens do tópico 'kafka-python-topic'... Pressione Ctrl+C para parar.")
try:
    for message in consumer:
        print(f"Mensagem recebida: {message.value}")
except KeyboardInterrupt:
    print("Consumidor encerrado.")


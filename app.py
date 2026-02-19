from flask import Flask, request
import requests
import json
import time
import telebot
import subprocess
import hmac, hashlib
from kafka import KafkaProducer

bot = telebot.TeleBot("7576015296:AAHPoWA5p6WPrYdcwk_MXXB-EWaN2SBPlLA")
#chat_id = 8552683290
chat_id = -5290134232

# Configuração do Kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:9094'],  # ajuste para o host/porta corretos
    value_serializer=lambda v: v.encode('utf-8')
)

def send_telegram_message(message):
    """Envia mensagem Telegram"""
    bot.send_message(chat_id, message)

def send_kafka_message(message):
    """Envia mensagem para Kafka"""
    producer.send('github_webhooks', value=json.dumps(message))
    producer.flush() # força envio imediato
    print("Mensagem enviada para o tópico github_webhooks!")


app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def github_webhook():
    event = request.headers.get("X-GitHub-Event")
    data = request.json

    print(f"Evento recebido: {event}")
    # Combina evento + payload 
    message = { 
        "event": event, 
        "payload": data 
    } 
    send_kafka_message(message)
    send_telegram_message(f"GitHub Evento: {event} - {data['repository']['full_name']} ")


    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

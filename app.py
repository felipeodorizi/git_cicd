from flask import Flask, request
import requests
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
    producer.send('github_webhooks', message)
    producer.flush() # força envio imediato
    print("Mensagem enviada para o tópico github_webhooks!")


app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def github_webhook():
    event = request.headers.get("X-GitHub-Event")
    data = request.json

    print(f"Evento recebido: {event}")


    # Detalhar conforme o tipo de evento
    if event == "push":
        branch = data.get("ref")
        commits = data.get("commits", [])
        msg = f"📦 Push na branch: {branch}"
        print(msg)
        send_telegram_message(msg)
        send_kafka_message(msg)
        for commit in commits:
            send_telegram_message("GITHUB")
            msg = f"📦 - Autor: {commit['author']['name']}"
            print(msg)
            send_telegram_message(msg)
            send_kafka_message(msg)
            msg = f"📦  Mensagem: {commit['message']}"
            print(msg)
            send_telegram_message(msg)
            send_kafka_message(msg)
            msg = f"📦  Arquivos alterados: {commit.get('modified', [])}"
            print(msg)
            send_telegram_message(msg)
            send_kafka_message(msg)
            # Chamar script bash 
            subprocess.run(["./ci_cd.sh"])


    elif event == "pull_request":
        action = data.get("action")
        pr = data.get("pull_request", {})
        send_telegram_message("GITHUB")
        msg = f"📦 Pull Request {action}: #{pr.get('number')} - {pr.get('title')}"
        print(msg)
        send_telegram_message(msg)
        send_kafka_message(msg)

    elif event == "issues":
        action = data.get("action")
        issue = data.get("issue", {})
        msg = f"📦 Issue {action}: #{issue.get('number')} - {issue.get('title')}"
        send_telegram_message("GITHUB")
        print(msg)
        send_telegram_message(msg)
        send_kafka_message(msg)

    else:
        # Para qualquer outro evento, imprime o JSON completo
        msg = f"📦 Payload bruto: {data}"
        send_telegram_message("📦 GITHUB")
        print(msg)
        send_telegram_message(msg)
        send_kafka_message(msg)

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

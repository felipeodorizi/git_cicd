from flask import Flask, request
import requests

app = Flask(__name__)

# Se quiser apenas proxy para outro serviço interno:
TARGET = "http://localhost:8000/webhook"

@app.route("/webhook", methods=["POST"])
def github_webhook():
    event = request.headers.get("X-GitHub-Event")
    data = request.json

    print(f"Evento recebido: {event}")

    # Se quiser repassar para outro serviço interno:
    try:
        resp = requests.post(TARGET, json=data, headers=request.headers)
        return resp.text, resp.status_code
    except Exception as e:
        return f"Erro ao encaminhar para {TARGET}: {e}", 500

@app.route("/", methods=["GET"])
def healthcheck():
    return "Proxy ativo e pronto para receber webhooks!", 200

if __name__ == "__main__":
    # No CodeSandbox, sempre use 0.0.0.0
    app.run(host="0.0.0.0", port=5000, debug=True)

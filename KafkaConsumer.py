from kafka import KafkaConsumer

#Define o comportamento quando não existe offset salvo para esse grupo:
#'latest' → começa a ler a partir das próximas mensagens
#'earliest' → lê todas as mensagens desde o início

#   enable_auto_commit=True
#   Faz o Kafka marcar automaticamente que a mensagem foi lida.

# cria o consumidor
consumer = KafkaConsumer(
    'kafka-python-topic',                      # nome do tópico
    bootstrap_servers=['localhost:9092'],  # endereço do broker
    auto_offset_reset='latest',      # começa a ler a partir das próximas mensagens
    enable_auto_commit=True,           # confirma leitura automaticamente
    group_id='meu-grupo',              # grupo de consumidores
    value_deserializer=lambda x: x.decode('utf-8')  # converte bytes em string
)

print("Consumidor iniciado. Aguardando mensagens...")

# loop infinito para consumir mensagens
for message in consumer:
    print(f"Mensagem recebida: {message.value}")
    
    # aqui você coloca a ação desejada
    # exemplo: salvar em banco, chamar API, processar dados
    if "alerta" in message.value.lower():
        print("⚠️ Ação especial: mensagem contém alerta!")

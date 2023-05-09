# Importa a classe KafkaConsumer da biblioteca kafka-python
from kafka import KafkaConsumer

# Cria uma instância de um consumidor Kafka e configura o endereço do servidor de bootstrap e o nome do tópico a ser consumido
consumer = KafkaConsumer(
    'my_topic', 
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest'
)

consumer.subscribe(['my_topic'])

while True:
    
    messages = consumer.poll(max_records=2)
    # Itera sobre as mensagens recebidas pelo consumidor
    for tp, msgs in messages.items():
        print(tp)

        for msg in msgs:
            # Decodifica a mensagem e a imprime no console
            print(msg.value.decode('utf-8'))

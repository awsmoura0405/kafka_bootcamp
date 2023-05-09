from kafka import KafkaProducer   # Importa a classe KafkaProducer da biblioteca kafka-python
import datetime as dt   # Importa a biblioteca datetime para trabalhar com datas e horas
import time   # Importa a biblioteca time para adicionar atrasos ao envio das mensagens
import random

# Configura os brokers
bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

# Cria o produtor
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: x.encode('utf-8')
)


# Funções de callback
def on_send_success(record_metadata):
    print(f'Mensagem enviada com sucesso para o tópico {record_metadata.topic} na partição {record_metadata.partition} com offset {record_metadata.offset}')

def on_send_error(ex):
    print(f'Erro ao enviar mensagem: {ex}')

# Lista de tópicos e partições
topic_partitions = [
    {'topic': 'topic_1', 'partitions': 4},
    {'topic': 'topic_2', 'partitions': 3},
    {'topic': 'topic_3', 'partitions': 2}
]

# Envia mensagens aleatoriamente para tópicos e partições
while True:

    # Escolhe o tópico
    topic_partition = random.choice(topic_partitions)
    topic = topic_partition['topic']

    # Cria a mensagem
    time_stamp = dt.datetime.strftime(dt.datetime.now(), format='%Y-%m-%d %H:%M:%S.%f') 
    value_ = f'{time_stamp} Essa mensagem é enviada por KafkaProducer.'

    # Envia a mensagem para o Kafka
    future = producer.send(topic, value=value_)

    # Espera a confirmação da entrega
    try:
        record_metadata = future.get(timeout=10)
        on_send_success(record_metadata)
    except Exception as ex:
        on_send_error(ex)
    
    time.sleep(1)

# Encerra o produtor
producer.flush()
producer.close()
# Importando as bibliotecas necessárias
from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
import uuid
import datetime as dt

# Instanciando o gerador de dados falsos
fake = Faker()

# Funções de callback
def on_send_success(record_metadata):
    print(f'Mensagem enviada com sucesso para o tópico {record_metadata.topic} na partição {record_metadata.partition} com offset {record_metadata.offset}')

def on_send_error(ex):
    print(f'Erro ao enviar mensagem: {ex}')


# Função para gerar uma transação fictícia
def generate_transaction():
    trans_time = dt.datetime.now()
    return f'''
        "customer_id": {random.randint(1, 10)},
        "timestamp": {trans_time.strftime('%Y-%m-%d %H:%M:%S.%f')},
        "transaction_id": {fake.uuid4()},
        "amount": {round(random.uniform(1, 1000), 2)},
        "currency": "USD",
        "card_brand": {random.choice(["Visa", "MC", "Elo", "Amex"])},
    '''

# Função principal para enviar transações para o tópico Kafka
def main():
    # Configura os brokers
    bootstrap_servers = ['localhost:9096', 'localhost:9097', 'localhost:9098']

    # Cria o produtor
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: x.encode('utf-8')
    )
 
    while True:
        # Gerar uma transação
        transaction = generate_transaction()
        print(transaction)

        # Adormecer por um período aleatório de tempo entre 0.5 e 2 segundos
        time.sleep(random.uniform(0.5, 2))

        # Cria a mensagem
        time_stamp = dt.datetime.strftime(dt.datetime.now(), format='%Y-%m-%d %H:%M:%S.%f') 
        value_ = transaction
        # Lista de tópicos e partições
        topic = 'topic_transactions'
        '''
        topic_partitions = [
            {'topic': 'topic_transactions', 'partitions': 1}
        ]
        '''

        # Envia a mensagem para o Kafka
        future = producer.send(topic, value=value_)

        # Espera a confirmação da entrega
        try:
            record_metadata = future.get(timeout=10)
            on_send_success(record_metadata)
        except Exception as ex:
            on_send_error(ex)

    # Encerra o produtor
    producer.flush()
    producer.close()

# Certificando-se de que o script é executado apenas quando é o script principal
if __name__:
    main()
  


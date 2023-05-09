# Importa a classe KafkaConsumer da biblioteca kafka-python
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
import time
import json
import random
import uuid
import datetime as dt

# Funções de callback
def on_send_success(record_metadata):
    print(f'Mensagem enviada com sucesso para o tópico {record_metadata.topic} na partição {record_metadata.partition} com offset {record_metadata.offset}')

def on_send_error(ex):
    print(f'Erro ao enviar mensagem: {ex}')

# Configura os brokers
bootstrap_servers = ['localhost:9096', 'localhost:9097', 'localhost:9098']

# Cria uma instância de um consumidor Kafka e configura o endereço do servidor de bootstrap e o nome do tópico a ser consumido
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    group_id='1'
)

# Atribuir a partição e o offset desejados
tp = TopicPartition('topic_transactions', 0) 
consumer.assign([tp])

consumer.seek(tp, 1160)  # move para o offset X

current_offset = consumer.position(tp)
end_offset = consumer.end_offsets([tp])[tp]

print(f'current_offset: {current_offset} - end_offset: {end_offset}')


# Cria o produtor
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: x.encode('utf-8')
)


while True:
    
    current_offset = consumer.position(tp)
    end_offset = consumer.end_offsets([tp])[tp]

    print(f'current_offset: {current_offset} - end_offset: {end_offset}')

    # Lê as mensagens do Kafka em lotes, com um limite de X mensagens por lote.
    messages = consumer.poll(max_records=5, timeout_ms=3000)
    
    
    # Itera pelos lotes de mensagens lidos.
    for tp, msgs in messages.items():
        print(f'------ Batch limit ------')

        # Itera pelas mensagens de cada lote.
        for msg in msgs:
        
            print(f"Offset: {msg.offset}, Chave: {msg.key}, Valor: {msg.value.decode('utf-8')}")

            # commita o offset da última mensagem consumida para todas as partições atribuídas
            consumer.commit()

            # Adormecer por um período aleatório de tempo entre 0.5 e 2 segundos
            time.sleep(random.uniform(0.5, 2))

            # Cria a mensagem
            time_stamp = dt.datetime.strftime(dt.datetime.now(), format='%Y-%m-%d %H:%M:%S.%f') 
            value_ = str(msg)
            # Lista de tópicos e partições
            topic = 'topic_enrichment'

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
 
    time.sleep(1)
 
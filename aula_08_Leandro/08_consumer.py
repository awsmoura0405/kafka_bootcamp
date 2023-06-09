# Importa a classe KafkaConsumer da biblioteca kafka-python
from kafka import KafkaConsumer, TopicPartition
import time

# Cria uma instância de um consumidor Kafka e configura o endereço do servidor de bootstrap e o nome do tópico a ser consumido
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    auto_offset_reset='earliest',
    group_id='1'
)

# Atribuir a partição e o offset desejados
tp = TopicPartition('topic_1', 3)  # partição 0 de 'my_topic'
consumer.assign([tp])


consumer.seek(tp, 0)  # move para o offset X

current_offset = consumer.position(tp)
end_offset = consumer.end_offsets([tp])[tp]

print(f'current_offset: {current_offset} - end_offset: {end_offset}')

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
    
    time.sleep(1)
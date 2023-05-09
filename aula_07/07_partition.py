from kafka import KafkaConsumer, TopicPartition

# Configurações do consumidor
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    group_id='1',
    )


# # Define o tópico e a partição desejada
topic = 'my_topic'
partition = 0

# Definir a partição que deseja consultar
tp = TopicPartition(topic, partition)

# Atribuir a partição ao consumidor
consumer.assign([tp])

# Obter a posição atual do offset
current_offset = consumer.position(tp)

# Obter o offset mais recente da partição
end_offset = consumer.end_offsets([tp])[tp]

# Calcular a diferença entre a posição atual e o offset mais recente
unconsumed_messages = end_offset - current_offset

print(f"A posição atual do offset para a partição {partition} do tópico {topic} é {current_offset}")
print(f"{unconsumed_messages} mensagens não lidas na partição {tp}")
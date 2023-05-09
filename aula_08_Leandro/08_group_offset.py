from kafka import KafkaConsumer, TopicPartition


groups = ['1']

for group_id_ in groups:

    # Configurações do consumidor
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        auto_offset_reset='earliest',
        group_id=group_id_,
        )

    topics = ['topic_1', 'topic_2','topic_3']

    # Define o tópico e a partição desejada
    for topic in topics:
        partitions = consumer.partitions_for_topic(topic)
        
        try:
            for partition in partitions:

            
            
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
                
                print(f'group_id: {group_id_} topic: {topic} partition: {partition:3} current_offset: {current_offset:5} end_offset: {end_offset:5} unconsumed_messages: {unconsumed_messages:5}')
        except Exception as ex:
            print(f'Erro: {ex}')
    print('\n')
from kafka.admin import KafkaAdminClient, NewTopic

# Configura os brokers
bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

# Cria o objeto KafkaAdminClient
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Cria a lista de tópicos
topic_list = [NewTopic(name='topic_1', num_partitions=4, replication_factor=1),
              NewTopic(name='topic_2', num_partitions=3, replication_factor=2),
              NewTopic(name='topic_3', num_partitions=2, replication_factor=3)]


# Cria os tópicos
for topic in topic_list:
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f'Topic {topic.name} created successfully.')
        print('\n')

    except Exception as ex:
        print(f'Erro: {ex}')
        print('\n')


# Encerra o objeto KafkaAdminClient
admin_client.close()

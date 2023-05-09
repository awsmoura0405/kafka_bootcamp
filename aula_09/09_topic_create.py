from kafka.admin import KafkaAdminClient, NewTopic

# Configura os brokers
bootstrap_servers = ['localhost:9096', 'localhost:9097', 'localhost:9098']

# Cria o objeto KafkaAdminClient
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Cria a lista de tópicos
topic_list = [NewTopic(name='topic_transactions', num_partitions=1, replication_factor=3),
              NewTopic(name='topic_fraud_detection', num_partitions=1, replication_factor=3),
              NewTopic(name='topic_technical_validation', num_partitions=1, replication_factor=3),
              NewTopic(name='topic_authorizations', num_partitions=1, replication_factor=3),
              NewTopic(name='topic_enrichment', num_partitions=1, replication_factor=3)]


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

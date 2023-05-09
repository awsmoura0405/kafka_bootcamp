from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(
    bootstrap_servers=['localhost:9096','localhost:9097','localhost:9098'],
)

topic_metadata = admin_client.list_topics()
print(topic_metadata)
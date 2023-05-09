from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers=["localhost:9092"]
)

topic = NewTopic(
    name="my_topic",
    num_partitions=3,
    replication_factor=2,
    topic_configs={
        "cleanup.policy": "compact",
        "retention.ms": "86400000"
    }
)
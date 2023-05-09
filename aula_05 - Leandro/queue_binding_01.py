import pika

# Establish a connection with RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

# Create a channel
channel = connection.channel()

# Define exchange and queue name s
exchange_name = 'exchange_topic'
queue_name = 'Q1'
routing_key = 'it.*.alta.*'

channel.queue_bind(
    exchange=exchange_name,
    queue=queue_name,
    routing_key=routing_key
)

# Close the connection
connection.close()
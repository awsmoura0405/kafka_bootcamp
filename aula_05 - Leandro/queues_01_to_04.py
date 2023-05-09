import pika

# Establish a connection with RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

# Create a channel
channel = connection.channel()

# Declare a queue to consume messages from
result = channel.queue_declare(
    queue='Q1', 
    durable=True
)

# Close the connection
connection.close()
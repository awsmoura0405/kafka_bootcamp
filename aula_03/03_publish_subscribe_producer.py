import pika
import datetime as dt
import time
import random

# Establish a connection with RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

# Create a channel
channel = connection.channel()

# Define exchange name
exchange_name = 'direct_logs'

# Declare exchange
channel.exchange_declare(
    exchange=exchange_name,
    exchange_type='direct'
)


# Create and publish messages
for i in range (100_000):

    # Assemble message
    time_stamp = dt.datetime.strftime(dt.datetime.now(), format='%Y-%m-%d %H:%M:%S.%f')
    message = f'{time_stamp} {i:6} {"."*random.randint(1,10)}'

    priority = message.count('.') - 1

    if priority in [1,2]:
        severity = 'error'
    elif priority in [3,4]:
        severity = 'warning'
    else:
        severity = 'info'

    # Publish message
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=severity,
        body=message
    )

    time.sleep(1)

    print(f" [x] Sent {message}")


# Close the connection
connection.close()
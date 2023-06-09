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


categories = ['ERRORS']
priorities = ['ALTA', 'MEDIA', 'BAIXA']
statuses = ['ABERTA', 'ENCERRADA']
department = 'IT'

# Create a message 
for i in range (10_000):

    # Assemble message
    category = random.choice(categories)
    priority = random.choice(priorities)
    status = random.choice(statuses)

## REGRAS DE ENVIO
## 1. Todos os logs são publicados no modo *fanout*.
    message = f'{time_stamp} {i:6} Mensagem criada por {department} com prioridade {priority} para informar sobre {category}.'
    route_ = 'fanout'
    exchange_name = 'exchange_fanout'
# Publish message
    exchange_name = 'exchange_fanout'
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=route_,
        body=message
    )

## 2. Todos erros de alta prioridade em aberto são publicados no modo *topic*, rota *department.category.priority.status*.

    if priority == "ALTA" & statuses == "ABERTA":
        time_stamp = dt.datetime.strftime(dt.datetime.now(), format='%Y-%m-%d %H:%M:%S.%f')
        message = f'{time_stamp} {i:6} Mensagem criada por {department} com prioridade {priority} para informar sobre {category}.'
        route_ = f'{department.lower()}.{category.lower()}.{priority.lower()}.{status.lower()}'

    # Publish message
        exchange_name = 'exchange_topic'
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=route_,
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
        )

    print(f" [x] Sent {message}")
    time.sleep(random.randint(0,3))


# Close the connection
connection.close()
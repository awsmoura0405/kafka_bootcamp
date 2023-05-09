import pika

# 1. Estabelecendo uma conexão com o servidor RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

channel = connection.channel()

# 2. Criando a fila
channel.queue_declare(queue='queue_hello')

# 3. Publicando a mensagem
channel.basic_publish(exchange='',
                      routing_key='queue_hello',
                      body='Hello RabbitMQ 06!')

print(" [x] Sent 'Hello a RabbitMQ!'")

# 4. Garantindo a entrega para o servidor
connection.close()

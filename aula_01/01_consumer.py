import pika

# 1. Estabelecendo uma conexão com o servidor RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

channel = connection.channel()

queue_name = 'queue_hello'

# 2. Criando a fila
channel.queue_declare(queue=queue_name)

def callback(ch, method, properties, body):
    print(f" [x] Received {body}.")

# 3. Indicar ao servidor qual função deve receber mensagens de uma fila específica
channel.basic_consume(queue=queue_name,
                      auto_ack=True,
                      on_message_callback=callback
                      )

#4. Iniciar o processo de *espera para consumo*.
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

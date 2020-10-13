from confluent_kafka import Consumer
from time import sleep

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group-id',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['topic_test1', 'topic_test2'])

i = 0

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    print(msg.topic())
    # sleep(10)
    # print(f"{i} - сообщений")
    # i+=1
    # print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
from confluent_kafka import Producer
from faker import Faker
import time

fake = Faker()

p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
while True:
    for data in fake.text().split(' '):
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        try:
            p.produce('topic_test_1', data.encode('utf-8'), callback=delivery_report)
            p.produce('topic_test_2', data.encode('utf-8'), callback=delivery_report)
        except BufferError:
            print("Ждемс")
            time.sleep(5)
# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
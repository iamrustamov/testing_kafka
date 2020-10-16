from time import sleep
from to_json import dumps
from kafka import KafkaProducer
import requests


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# for j in range(9999):
#     print("Iteration", j)
#     data = {'counter': j}
#     producer.send('topic_test', value=data)
#     sleep(0.5)


class RandomData:
    def __init__(self, adr, count):
        self.adr = adr
        self.count = count

    def __iter__(self):
        return self

    def __next__(self):
        if self.count > 0:
            self.count -= 1
            res = requests.get(self.adr).json()
            return res
        else:
            raise StopIteration

data_generator = RandomData("http://api.randomdatatools.ru/", 1000)

for data in data_generator:
    producer.send('random_data', value=data)
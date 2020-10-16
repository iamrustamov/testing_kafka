import to_json

from kafka import KafkaConsumer
from to_json import loads

consumer = KafkaConsumer(
    'random_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

write_file = open("data_file.json", "w")


for event in consumer:
    event_data = event.value
    # Do whatever you want
    # print(event.topic)
    print(event_data)
    to_json.dump(event_data, write_file)

write_file.close()
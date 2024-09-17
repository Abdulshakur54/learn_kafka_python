from confluent_kafka import Producer
from config import config

def callback(err, event):
    if err:
        print(f"Produce to topic {event.topic()} failed for key {event.key()} with value {event.value()}\nError: {err}")
    else:
        print(f"Success: Message {event.value().decode('utf-8')} with key {event.key().decode('utf-8')} was sent to partition {event.partition()}")

if __name__ == '__main__':
    keys = ['Amy', 'Brenda', 'Cindy', 'Derrick', 'Elaine', 'Fred']
    topic = 'hello_topic'
    producer = Producer(config)
    [producer.produce(topic, f"Hello {key}", key, on_delivery = callback) for key in keys]
    producer.flush()
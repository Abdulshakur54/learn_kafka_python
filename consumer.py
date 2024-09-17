from confluent_kafka import Consumer, KafkaException
from config import config

#appending the properties mandatory to be used by the Consumer class
def set_consumer_config(config: str):
    config['group.id'] = 'hello_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False
    return config

#method to be called after topic partitions have been asigned to consumers. it will be called by the subscribe method 
#as well as any subsequent rebalancing

def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f"Assigned to topic '{p.topic}', partition {p.partition}")


if __name__ == '__main__':
    config = set_consumer_config(config=config)
    consumer = Consumer(config)
    consumer.subscribe(['hello_topic'], on_assign = assignment_callback)

    try:
        while True:
            event = consumer.poll(1.0)
            if event is None:
                continue
            elif event.error():
                raise KafkaException(event.error())
            else:
                print(f"Received: {event.value().decode('utf-8')} from partition {event.partition()}")
                consumer.commit()
    except KeyboardInterrupt:
        print('Cancelled by the user')
    finally:
        consumer.close()
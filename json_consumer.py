from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from config import config, sr_config, schema_str
from temperature import Temperature


def dict_to_temp(dict, cont):
    return Temperature(dict['city'], dict['reading'], dict['unit'], timestamp=dict['timestamp'])

def set_consumer_config(config):
    config['group.id'] = 'temp_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False
    return config

if __name__ == '__main__':
    json_deserializer = JSONDeserializer(schema_str, dict_to_temp)
    config = set_consumer_config(config)
    consumer = Consumer(config)
    topics = ["temp_readings"]
    consumer.subscribe(topics)
    while True:
        try:
            ev = consumer.poll(1.0)
            if ev is None:
                continue
            elif ev.error():
                print(f"Error: {ev.error()}")
            else:
                temp = json_deserializer(ev.value(), MessageField.VALUE)
                print(f'Latest temp in {temp.city} is {temp.reading} {temp.unit}.')
                consumer.commit(ev)
        except KeyboardInterrupt:
            print('user interrupted by pressing a key')
        
    consumer.close()
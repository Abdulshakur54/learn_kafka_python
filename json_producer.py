from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from config import config, sr_config, schema_str
from temperature import Temperature
import time

def to_dict(obj: Temperature, cont) -> dict:
    return dict(city = obj.city, reading = obj.reading, unit = obj.unit, timestamp = obj.timestamp)


def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Temp reading for {event.key().decode("utf8")} produced to {event.topic()}')




if __name__ == '__main__':
    sch_reg = SchemaRegistryClient(sr_config)
    json_serializer = JSONSerializer(schema_str=schema_str, schema_registry_client=sch_reg, to_dict=to_dict)
    topic = "temp_readings"
    producer = Producer(config)
    temperatures = [Temperature('London', 12, 'C', round(time.time()*1000)),
        Temperature('Chicago', 63, 'F', round(time.time()*1000)),
        Temperature('Berlin', 14, 'C', round(time.time()*1000)),
        Temperature('Madrid', 18, 'C', round(time.time()*1000)),
        Temperature('Phoenix', 78, 'F', round(time.time()*1000))]
    
    for temperature in temperatures:
        value = json_serializer(temperature, SerializationContext(topic=topic, field=MessageField.VALUE))
        producer.produce(topic, value, str(temperature.city), on_delivery = delivery_report)
    producer.flush()





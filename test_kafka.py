from kafka import KafkaProducer, KafkaConsumer
import json

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

kafka_host = 'localhost'
kafka_port = '9092'
producer = KafkaProducer(bootstrap_servers=[f'{kafka_host}:{kafka_port}'],  value_serializer = json_serializer)

producer.send(
    'test_topic', {"message": 'helloworld'}
)
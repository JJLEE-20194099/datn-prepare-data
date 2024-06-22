import threading
import func_timeout
import time
from kafka import KafkaProducer, KafkaConsumer
from meeyland_util import transferMeeyland
import json
from tqdm import tqdm
from consume.utils import Redis
from dotenv import load_dotenv
import os
load_dotenv(override=True)

class Kafka:
    def __init__(self):
        self.kafka_host = os.getenv('KAFKA_HOST')
        self.kafka_port = os.getenv('KAFKA_PORT')
        self.producer = KafkaProducer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'])
        #self.consumer = KafkaConsumer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=self.kafka_group_id,value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    def kafka_consumer(self, kafka_group_id, kafka_topic):
        """_summary_

        Args:
            kafka_group_id (_type_): group id of consumer
            kafka_topic (_type_): list topic to subscribe

        Returns:
            _type_: consumer
        """
        consumer = KafkaConsumer(
            bootstrap_servers=[f"{self.kafka_host}:{self.kafka_port}"],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=kafka_group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            max_poll_records=10
        )
        consumer.subscribe(kafka_topic)
        return consumer

    def send_data(self, data,kafka_topic):
        """_summary_

        Args:
            data (_type_): data to send to kafka
            kafka_topic (_type_): topic to send data

        Returns:
            _type_: False if send fail, True if send success
        """
        status = self.producer.send(kafka_topic, value = json.dumps(data).encode('utf-8'))
        self.producer.flush()
        if status.is_done == True:
            return True
        else:
            return False


    def create_consumer_and_subscribe(self, kafka_group_id, kafka_topic):
        """_summary_

        Args:
            kafka_group_id (_type_): group id of consumer
            kafka_topic (_type_): list topic to subscribe

        Returns:
            _type_ : consumer
        """
        consumer = KafkaConsumer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=kafka_group_id,value_deserializer=lambda x: x.decode('utf-8'))
        consumer.subscribe(kafka_topic)
        return consumer


KafkaInstance = Kafka()
MAX_THREAD = 10

with open('streets.json', encoding='utf-8') as f:
   streets = json.load(f)


locationql = [f'{item["STREET"].lower()}, {item["WARD"].lower()}, {item["DISTRICT"].lower()}' for item in tqdm(streets)]

def processMeeyland(msg):
    data = msg.value
    dataMeeyland = transferMeeyland(data)
    if dataMeeyland != None:
        print("Process New Message Ok")
        KafkaInstance.send_data(dataMeeyland, "datn_meeyland")


def runMeeyland():
    consumer = KafkaInstance.kafka_consumer("raw_meeyland", ["raw_meeyland"])
    for msg in tqdm(consumer):

        if Redis().check_id_exist(f'meeyland_offset_{msg.offset}', 'meeyland_clean_rawdata'):
            print("Ignore Processed Messages")
            continue
        Redis().add_id_to_set(f'meeyland_offset_{msg.offset}', 'meeyland_clean_rawdata')
        processMeeyland(msg)

runMeeyland()
import threading
import func_timeout
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
from meeyland_util import transferMeeyland
from kafka import TopicPartition
import json
from tqdm import tqdm
from consume.utils import Redis
from dotenv import load_dotenv
import os
load_dotenv(override=True)

class Kafka:
    def __init__(self, broker_id):
        self.kafka_host = os.getenv('KAFKA_HOST')
        self.broker_id = broker_id
        self.kafka_port = os.getenv(f'KAFKA_PORT_{self.broker_id}')
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'])
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
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=kafka_group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            max_poll_records=10
        )
        consumer.assign([TopicPartition(topic, 0) for topic in kafka_topic])
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
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'], auto_offset_reset='earliest', enable_auto_commit=False, group_id=kafka_group_id,value_deserializer=lambda x: x.decode('utf-8'))
        consumer.subscribe(kafka_topic)
        return consumer


MAX_THREAD = 10

with open('streets.json', encoding='utf-8') as f:
   streets = json.load(f)


locationql = [f'{item["STREET"].lower()}, {item["WARD"].lower()}, {item["DISTRICT"].lower()}' for item in tqdm(streets)]

def processMeeyland(msg, KafkaInstance):
    data = msg.value
    dataMeeyland = transferMeeyland(data)
    if dataMeeyland != None:
        status = KafkaInstance.send_data(dataMeeyland, "datn_meeyland")
        if status:
            print("Process New Message and Send Message Done")
            return dataMeeyland
    else:
        print("None None None")
    return None


def clean():
    KafkaInstance = Kafka(broker_id = 0)
    consumer = KafkaInstance.kafka_consumer("raw_meeyland", ["raw_meeyland"])
    # for msg in tqdm(consumer):
    #     consumer.commit()
    #     print(msg)
    #

    n_tries = 0
    while True:
        msg_pack = consumer.poll(timeout_ms=60000)
        cnt = 0
        for tp, messages in msg_pack.items():
            for message in messages:
                # message value and key are raw bytes -- decode if necessary!
                # e.g., for unicode: `message.value.decode('utf-8')`

                hash_str = hash(message.value['content'])

                if Redis().check_id_exist(f'meeyland_offset_{tp.partition}_{hash_str}', 'meeyland_clean_rawdata'):
                    print("Ignore Processed Messages")
                    continue
                print("Consume Message in Topic:", tp.topic, "Partition:", tp.partition, "Offset:", message.offset)
                Redis().add_id_to_set(f'meeyland_offset_{tp.partition}_{hash_str}', 'meeyland_clean_rawdata')
                processMeeyland(message,KafkaInstance)

                cnt += 1
        if cnt == 0:
            n_tries += 1

        if n_tries >= 10:
            break




clean()
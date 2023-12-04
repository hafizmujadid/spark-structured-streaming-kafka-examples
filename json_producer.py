import os
import time
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

TOPIC_NAME = "test2"
SASL_MECHANISM = 'PLAIN'

HOST_NAME = ['KAFKA_ENDPOINT']
USERNAME = 'USERNAME']
PASSWORD = 'PASSWORD']


def produce():

    producer = KafkaProducer(
        bootstrap_servers=HOST_NAME,
        sasl_mechanism=SASL_MECHANISM,
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        security_protocol="SASL_SSL",
    )



    for i in range(100):
        name=fake.name(),
        age=fake.random.randint(15, 85),
        favorite_color=fake.random.choice(['red', 'green', 'blue', 'black', 'brown', 'gray', 'yellow', 'white']),
        favorite_number=fake.random.randint(10, 100))
        message = f"""{"name":"{name}","favorite_color":"{favorite_color}", "favorite_number":{favorite_number}, "age":{age}}"""
        producer.send(TOPIC_NAME, message.encode('utf-8'))
        print(f"Message sent: {message}")

    producer.close()


if __name__ == '__main__':
    produce()

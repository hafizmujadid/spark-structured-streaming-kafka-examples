from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from faker import Faker

fake = Faker()

KAFKA_HOST = "ADFGH"
KAFKA_USERNAME = "ADFGH"
KAFKA_PASSWORD = "ADFGH"

conf = {
    "bootstrap.servers": KAFKA_HOST,
    "sasl.mechanism": "PLAIN",
    "sasl.username": KAFKA_USERNAME,
    "sasl.password": KAFKA_PASSWORD,
    "security.protocol": "SASL_SSL",
    "acks": 1,
}

schema_registry_conf = {
    "url": f"https://${KAFKA_USERNAME}:${KAFKA_PASSWORD}@${KAFKA_HOST}"
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

valuejsonFormatSchema = open("./schema/key_schema.avsc", "r").read()
keyjsonFormatSchema = open("./schema/value_schema.avsc", "r").read()

class User:
    def __init__(self, name, age, favorite_color, favorite_number):
        self.name, self.age, self.favorite_color, self.favorite_number = name, age, favorite_color, favorite_number

class Key:
    def __init__(self, id):
        self.id = id

def user_to_dict(user, ctx):
    return dict(name=user.name, age=user.age, favorite_color=user.favorite_color, favorite_number=user.favorite_number)

def key_to_dict(key, ctx):
    return dict(id=key.id)

avro_serializer_value = AvroSerializer(schema_registry_client, valuejsonFormatSchema, user_to_dict)
avro_serializer_key = AvroSerializer(schema_registry_client, keyjsonFormatSchema, key_to_dict)
string_serializer = StringSerializer("utf_8")

producer = Producer(conf)

for i in range(1, 100):
    key, payload = Key(id=i), User(fake.name(), fake.random.randint(15, 85), fake.random.choice(['red', 'green', 'blue', 'black', 'brown', 'gray', 'yellow', 'white']), fake.random.randint(10, 100))

    topic = "spark_avro"
    producer.produce(
        topic=topic,
        key=avro_serializer_key(key, SerializationContext(topic, MessageField.KEY)),
        value=avro_serializer_value(payload, SerializationContext(topic, MessageField.VALUE)),
    )

producer.flush()

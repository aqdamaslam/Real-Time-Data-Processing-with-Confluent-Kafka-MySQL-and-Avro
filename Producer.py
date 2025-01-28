# Make sure to install confluent-kafka python package
# pip install confluent-kafka
# pip install pandas
# pip install requests
# pip install fastavro

import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    print("=====================")

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': '<YOUR-SERVER-NAME>',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '<YOUR-USERNAME>',
    'sasl.password': 'YOUR-PWD'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': '<URL>',
  'basic.auth.user.info': '{}:{}'.format('<YOUR-KEY>', '<YOUR-SECRET-KEY>')
})

# Fetch the latest Avro schema for the value
subject_name = 'YOUR-SUBJECT-NAME'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema from Registery---")
print(schema_str)
print("=====================")

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

# MySQL library
from sqlalchemy import create_engine

# MySQL connection details
host = "<HOSTNAME>"
port = 3306  # Default MySQL port
user = "<YOUR-USER-NAME>"
password = "<YOUR-CONNECTION-PWD>"
database = "<YOUR-DATABASE-NAME>"

# Table name to load data from
table_name = "<YOU-TABLE-NAME>"

# Create a connection string
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

# Create a SQLAlchemy engine
engine = create_engine(connection_string)

# Query the table and load it into a DataFrame
try:
    query = f"SELECT id, name, category, price, last_updated FROM product WHERE last_updated > {last_read_timestamp};"
    df = pd.read_sql(query, engine)
    print(df.head())  # Display the first few rows of the DataFrame
except Exception as e:
    print("Error:", e)

# DataFrame Preprocessing
if 'last_updated' in df.columns:
    df['last_updated'] = df['last_updated'].astype(str)

if 'price' in df.columns:
    df['price'] = df['price'].astype(float)

if 'id' in df.columns:
    df['id'] = df['id'].astype(int)

# Iterate over DataFrame rows and produce to Kafka
for id, row in df.iterrows():
    # Create a dictionary from the row values
    data_value = row.to_dict()
    print(data_value)
    # Produce to Kafka
    producer.produce(
        topic='kafka_assignment', 
        key=str(id), 
        value=data_value, 
        on_delivery=delivery_report
    )
    producer.flush()
    time.sleep(2)

print("All Data successfully published to Kafka")

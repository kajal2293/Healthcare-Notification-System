from kafka import KafkaConsumer
import json
from json import loads
import sys
import boto3

### Setting up the Python consumer
bootstrap_servers = ['localhost:9092']
topicName = 'doctors-queue'
consumer = KafkaConsumer (topicName, group_id = 'my_group_id',bootstrap_servers = bootstrap_servers,
auto_offset_reset = 'earliest',value_deserializer=lambda m: json.loads(m.decode('utf-8')))  
# Create an SNS client
subclient = boto3.client(
    "Doctorqueue"
) 
subclient.subscribe(TopicArn="arn:aws:sns:us-east-1:383394651111:Doctorqueue",protocol='Email',Endpoint="akilaneduusa@gmail.com")

for message in consumer:
    subclient.publish(message,TopicArn="arn:aws:sns:us-east-1:383394651111:Doctorqueue")

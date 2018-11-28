#!/usr/bin/env python
# pip install kafka-python
import json
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from dateutil import parser as dateparser
import time
import pickle
import argparse

parser = argparse.ArgumentParser(description='Get Kafka Messages')
parser.add_argument(
    '--bootstrap_server', help='kafka bootstrap servers', default='localhost:9092')
parser.add_argument('--date', help='get messages after date in utc')
parser.add_argument('--topic', help='topic to consume from', required=True)
parser.add_argument('--max_messages', help='number of messages to consume', default=None)
parser.add_argument('--use_json_deserializer', help='Use json deserializer', default=False)

args = parser.parse_args()

bootstrap_servers = 'localhost:9092'
value_deserializer = None
if args.use_json_deserializer:
    value_deserializer = lambda v: json.loads(v.decode('utf8'))
consumer = KafkaConsumer(fetch_max_bytes=21000000, group_id=None,
                         bootstrap_servers=args.bootstrap_server, value_deserializer=value_deserializer)

produceList = []
count = 0
if args.date is not None:
    ts = time.mktime(dateparser.parse(args.date).timetuple())*1000
    partitions = consumer.partitions_for_topic(args.topic)
    topicPartitions = [TopicPartition(args.topic, partition) for partition in partitions]
    consumer.assign(topicPartitions)
    timestamps = {}
    for tp in topicPartitions:
        timestamps.update({tp: ts})
    offsets = consumer.offsets_for_times(timestamps)
    for tp in offsets:
        if offsets[tp] is not None:
            print("Setting offset {} on partition {}".format(offsets[tp].offset,tp.topic))
            consumer.seek(tp,offsets[tp].offset)
else:
    consumer.subscribe(args.topic)

for message in consumer:
    print(message)
    count+=1
    if (args.max_messages is not None) and (count >= int(args.max_messages)):
        break
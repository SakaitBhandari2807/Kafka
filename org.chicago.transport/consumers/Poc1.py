"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer


def display_data(topic_name):
    c = Consumer({
        "bootstrap.servers":"localhost:9092"
        ,"group.id":"1"
    })
    c.subscribe([topic_name])
    message = c.poll(5)
    print(message)


display_data("com.udacity.project1.weather.v1")
"""Methods pertaining to weather data"""
from dataclasses import asdict
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer

logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):

        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas

        super().__init__(
            "com.udacity.project1.weather.v1",  # TODO: Come up with a better topic name
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=1,
            num_replicas=1
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        #
        # TODO: Define this value schema in `schemas/weather_value.json
        #
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        # TODO: Complete the function by posting a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.

        # TODO: Set the appropriate headers
        #       See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
        headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
        # TODO: Update the below payload to include the Avro Schema string
        #       See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
        data = {"value_schema": Weather.value_schema,
                "key_schema": Weather.key_schema,
                "records": [{
                    "key": {"timestamp": str(self.time_millis())},
                    "value": {
                        "temperature": str(self.temp),
                        "status": str(self.status.name)
                    }}]}
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/com.udacity.project1.weather.v1",  # TODO
            data=json.dumps(data),
            headers=headers
        )

        if resp.status_code == 200:
            logger.info("Data sent successfully to kafka weather topic")
        try:
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send data to REST Proxy due to {e}")

        # print(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")

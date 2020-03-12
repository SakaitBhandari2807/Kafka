"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        if 'com.udacity.project1.weather.v1' in message.topic():
            weather_info = message.value()
            self.temperature = weather_info['temperature']
            self.status = weather_info['status']
            print(
                f"temp {self.temperature},status{self.status}"
            )

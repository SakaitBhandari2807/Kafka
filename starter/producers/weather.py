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
        print("weather process_message is - Running")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        weather_info = message.value()
        self.temperature = weather_info["temperature"]
        self.status = weather_info["status"]
        
        print(f"Running process weather : temperature :{self.temperature},status :{self.status}")

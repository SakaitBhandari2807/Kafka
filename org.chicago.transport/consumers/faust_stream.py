"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("app1", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("com.udacity.project1.stations",value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("com.udacity.project1.table.v1", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "com.udacity.project1.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


def add_line(stationevents):
    if stationevents.red:
        table["line"] = "red"
    elif stationevents.blue:
        table["line"] = "blue"
    elif stationevents.green:
        table["line"] = "green"
    return stationevents

#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def transformedstation(stationevents):
    #
    # TODO: Group By station-name
    #
    # stationevents.add_processor(add_line)
    # async for event in stationevents.group_by(Station.station_name):
    async for event in stationevents:
        #
        # TODO: Use the URI as key, and add the number for each click event
        #
        # table[event.station_id] = event.station_id
        # table[event.station_name] = event.station_name
        # table[event.order] = event.order
        # # table["line"] = event.line
        # print(event)
        # if event.red:
        #     table["line"] = "red"
        # elif event.blue:
        #     table["line"] = "blue"
        # elif event.green:
        #     table["line"] = "green"

        if event.red:
            line = "red"
        elif event.green:
            line = "green"
        elif event.blue:
            line = "blue"
        else:
            line = "N/A"
            
        table[event.station_id] = TransformedStation(event.station_id,event.station_name,event.order,event.line)


if __name__ == "__main__":
    app.main()

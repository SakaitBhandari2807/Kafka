import faust


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

class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App('temp-app2', broker="localhost:9092")

topic = app.topic('connectorstations', value_type=Station)

table = app.Table("tab1", default=TransformedStation, partitions=1)

@app.agent(topic)
async def process(streams):

    async for stream in streams.group_by(Station.station_id):
        # print(f"table[station_id]: {table[stream.station_id]}")
        # print(f"table[station_name]: {table[stream.station_name]}")
        # print(f"table[order]: {table[stream.order]}")
        # print(f"table[line]: {table[stream.line]}")

        print(f"table[station_id]: {stream.station_id}")
        print(f"table[station_name]: {stream.station_name}")
        print(f"table[order]: {stream.order}")
        # print(f"table[line]: {stream.line}")


if __name__ == "__main__":
    app.main()
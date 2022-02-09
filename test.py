from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI
token = "CqA_ChKWMqafO1JXt6Zv8KcuF-o1BN8lgD62J9gtnWQ-7318hFTNR-00p3Ba4Mbrch1EoDAFRSersrimToX33A=="
org = "Developer"
bucket = "Test"

with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as client:
    point = Point("my_measurement").tag("location", "Stockholm").field("temperature", 1.0 ).time(datetime.utcnow(),
                                                                                          WritePrecision.MS)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    write_api.write(bucket, org, point)

    # query = 'from(bucket: "Test") |> range(start: -1h)'
    # tables = client.query_api().query(query, org=org)
    # for table in tables:
    #     for record in table.records:
    #         print(record)

client.close()

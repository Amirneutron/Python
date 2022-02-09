from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.extras import pd, np

# You can generate an API token from the "API Tokens Tab" in the UI
token = "CqA_ChKWMqafO1JXt6Zv8KcuF-o1BN8lgD62J9gtnWQ-7318hFTNR-00p3Ba4Mbrch1EoDAFRSersrimToX33A=="
org = "Developer"
bucket = "Amir"
tag_columns = ['location', 'sensor_id', 'sensor_type', 'lat', 'lon']

with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as client:
    query_api = client.query_api()

    data = pd.read_parquet('snappy.parquet', engine='fastparquet')
    data.set_index(pd.DatetimeIndex(data.timestamp), inplace=True)
    del data['timestamp']
    data.sort_index(inplace=True, ascending=True)


    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write("Amir", "Developer", data, data_frame_measurement_name='ppd42ns', data_frame_tag_columns=tag_columns,tags=None, batch_size=10240)
    print()

    print("Wait to finishing ingesting DataFrame...")
    print()
    print()


    #result = client.query_api('select * from ppd42ns')

    """
        Query: using Pandas DataFrame
        """
    data = query_api.query_data_frame('''
         from(bucket:"Amir")
             |> range(start: -10m)
             |> filter(fn: (r) => r["_measurement"] == "ppd42ns")
             |> pivot(rowKey:["timestamp"], columnKey: ["_field"], valueColumn: "_value")
             |> keep(columns: ["timestamp","location", "sensor_id"])
         ''')
    print(data.to_string())

client.close()



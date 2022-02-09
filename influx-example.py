import sys
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.extras import pd, np

token = "CqA_ChKWMqafO1JXt6Zv8KcuF-o1BN8lgD62J9gtnWQ-7318hFTNR-00p3Ba4Mbrch1EoDAFRSersrimToX33A=="
org = "Developer"
bucket = "Amir"


with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as client:
    query_api = client.query_api()

    filename = 'snappy.parquet'
    df = pd.read_parquet(filename)

    print('Begin:', df.timestamp.min())
    print('End:  ', df.timestamp.max())

    # Sorting I
    # df.sort_values(by=['timestamp'], inplace=True, ascending=True)

    # Sorting II
    df.set_index(pd.DatetimeIndex(df.timestamp), inplace=True)
    del df['timestamp']
    df.sort_index(inplace=True, ascending=True)

    tag_columns = ['location', 'sensor_id', 'sensor_type', 'lat', 'lon']
    write_api = client.write_api()
    write_api.write(df, measurement="my_measurement", tag_columns=tag_columns, tags=None, batch_size=10240)

    print("Data Saved" ,df)

client.close()
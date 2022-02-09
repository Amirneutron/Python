import random
import time
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.extras import pd
import dask.dataframe as dd


# You can generate an API token from the "API Tokens Tab" in the UI
token = "CqA_ChKWMqafO1JXt6Zv8KcuF-o1BN8lgD62J9gtnWQ-7318hFTNR-00p3Ba4Mbrch1EoDAFRSersrimToX33A=="
org = "Developer"
bucket = "ericsson_data"
tag_columns = ['satellite_class', 'telemetry_log']


dates = []
def randomDate(start, end):
    format = '%d-%m-%Y %H:%M:%S'

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + random.random() * (etime - stime)
    dt = datetime.fromtimestamp(time.mktime(time.localtime(ptime)))
    return dt


with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as client:
    query_api = client.query_api()

    print("=== Generating DataFrame ===")
    ericsson = dd.read_parquet('data.parquet', engine='pyarrow')

    datesList = []
    newList = []
    dates = range(0, len(ericsson))


    ericsson["timestamp"] = randomDate("01-01-2022 00:01:00", "31-01-2022 23:59:34").utcnow()

    print('Begin:', ericsson.timestamp.min())
    print('End:  ', ericsson.timestamp.max())

    ericsson.set_index(ericsson.timestamp, sorted=True)
    #del ericsson['timestamp']


    for row in ericsson.iterrows():

        point = Point("ericsson_data") \
            .tag(tag_columns) \
            .field(row.satelitte,row.telemetry_log ) \
            .time(row.timestamp)

    print(point)
   # print(type(pandas_df))
  #  print(pandas_df.head())



    # print("Writing data to influxDB")
    # write_api = client.write_api(write_options=SYNCHRONOUS)
    # write_api.write("ericsson_data", "Developer", , data_frame_measurement_name='data',
    #              data_frame_tag_columns=tag_columns, tags=None, batch_size=10240)
    # print("Data has been processed")

client.close()

   # ericsson.set_index(pd.DatetimeIndex(ericsson.Timestamp), inplace=True)
   # ericsson.sort_index(inplace=True, ascending=True)

    #print(ericsson.columns)

   # ericsson.index = dd.Series()
   # data = ericsson.to_parquet('myData.parquet', engine='fastparquet')


   # print(ericsson)
   # write_api = client.write_api(write_options=SYNCHRONOUS)
   # write_api.write("Sensor", "Developer", ericsson, data_frame_measurement_name='ppd42ns', data_frame_tag_columns=tag_columns,tags=None, batch_size=10240)





    # data = pd.read_parquet('snappy.parquet', engine='fastparquet')
    # data.set_index(pd.DatetimeIndex(data.timestamp), inplace=True)
    # del data['timestamp']
    # data.sort_index(inplace=True, ascending=True)
    #
    #
    # write_api = client.write_api(write_options=SYNCHRONOUS)
    # write_api.write("Amir", "Developer", data, data_frame_measurement_name='ppd42ns', data_frame_tag_columns=tag_columns,tags=None, batch_size=10240)
    # print()
    #
    # print("Wait to finishing ingesting DataFrame...")
    # print()
    # print()


    #result = client.query_api('select * from ppd42ns')






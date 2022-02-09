import codecs
from datetime import datetime

from influxdb_client import WritePrecision, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

token = "CqA_ChKWMqafO1JXt6Zv8KcuF-o1BN8lgD62J9gtnWQ-7318hFTNR-00p3Ba4Mbrch1EoDAFRSersrimToX33A=="
org = "Developer"
bucket = "Project"

with InfluxDBClient(url="http://localhost:8086", token=token, org=org, debug=False) as client:
    query_api = client.query_api()

    p = Point("my_measurement").tag("location", "Prague").field("temperature", 27.3 ).time(datetime.utcnow(),
                                                                                          WritePrecision.MS)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # write using point structure
    write_api.write(bucket="Project", record=p)

    line_protocol = p.to_line_protocol()
    print(line_protocol)

    # write using line protocol string
    write_api.write(bucket="Project", record=line_protocol)

    # using Table structure
    tables = query_api.query('from(bucket:"Project") |> range(start: -10m)')
    for table in tables:
        print(table)
        for record in table.records:
            # process record
            print(record.values)

    
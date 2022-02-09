import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI
token = "CqA_ChKWMqafO1JXt6Zv8KcuF-o1BN8lgD62J9gtnWQ-7318hFTNR-00p3Ba4Mbrch1EoDAFRSersrimToX33A=="
org = "Developer"
bucket = "Project"
# Store the URL of your InfluxDB instance
url="http://localhost:8086"

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)

write_api = client.write_api(write_options=SYNCHRONOUS)

p = influxdb_client.Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
write_api.write(bucket=bucket, org=org, record=p)
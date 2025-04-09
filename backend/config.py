from dotenv import load_dotenv
import os
from pymongo import MongoClient
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


load_dotenv("../.env")
mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
mongo_client = MongoClient(f"mongodb://admin:{mongo_password}@localhost:27017/")
db = mongo_client['flashes']
sources_collection = db['sources']

# connect to influxdb
influx_token = os.getenv("INFLUXDB_ADMIN_TOKEN")
influx_org = "flashes"
influx_bucket = "flashes_data"
client = InfluxDBClient(url="http://localhost:8086", token=influx_token, org=influx_org, timeout=30000)
batch_size = 100 # to balance load during upload

query_api = client.query_api()
write_api = client.write_api(write_options=SYNCHRONOUS)
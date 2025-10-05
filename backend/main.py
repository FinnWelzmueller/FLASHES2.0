import logging
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from download import update
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper())
    )

logging.info("Connecting to MongoDB...")
client = MongoClient(f"mongodb://admin:{os.getenv("MONGO_INITDB_ROOT_PASSWORD")}@localhost:27017/")
db = client['flashes']
sources_collection = db['sources']
sources_collection.find({})
logging.info("Success.")

logging.info("Connecting to InfluxDB...")
token = os.getenv("INFLUXDB_ADMIN_TOKEN")
client = influxdb_client.InfluxDBClient(
   url= "http://localhost:8086",
   token=token,
   org="flashes"
)
write_api = client.write_api(write_options=SYNCHRONOUS)
logging.info("Success.")
update(sources_collection, write_api, os.getenv("TEMP_DIR", "./_temp"))
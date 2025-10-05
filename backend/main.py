import logging
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from download import update
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from fastapi import FastAPI, Path

load_dotenv()

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper())
    )

logging.info("Connecting to MongoDB...")
client = MongoClient(f"mongodb://admin:{os.getenv('MONGO_INITDB_ROOT_PASSWORD')}@mongodb:27017/")
db = client['flashes']
sources_collection = db['sources']
sources_collection.find({})
logging.info("Success.")

logging.info("Connecting to InfluxDB...")
token = os.getenv("INFLUXDB_ADMIN_TOKEN")
client = influxdb_client.InfluxDBClient(
   url= "http://influxdb:8086",
   token=token,
   org="flashes"
)
write_api = client.write_api(write_options=SYNCHRONOUS)
logging.info("Success.")
#update(sources_collection, write_api, os.getenv("TEMP_DIR", "./_temp"))

app = FastAPI()
@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/sources/{source_id}")
async def get_sources(source_id: str = Path(..., description="The ID from the mongoDB of the source to retrieve")):
    """
        Get a source by its ID.
    """
    return sources_collection.find({"_id": source_id})[0]

@app.get("/health/mongo")
async def get_health_mongo():
    logging.info("Performing health check on MongoDB...")
    try:
        if (sources_collection.count_documents({})) > 0:
            logging.info("MongoDB is healthy.")
            return {"status": "ok"}
        return {"status": "error"}
    except Exception as e:
        logging.error(f"MongoDB health check failed: {e}")
        return {"status": "error"}
    
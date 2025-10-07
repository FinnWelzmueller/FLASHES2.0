import logging
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from download import update
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from fastapi.responses import JSONResponse
import requests
from fastapi import FastAPI, Path, Query

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
query_api = client.query_api()
logging.info("Success.")
#update(sources_collection, write_api, os.getenv("TEMP_DIR", "./_temp"))

app = FastAPI()
@app.get("/")
async def root():
    return {"message": "Hello World"}

### Healthchecks endpoints

@app.get("/health/mongo")
async def health_mongo():
    """
        Healthcheck for MongoDB. Tries to count documents in the sources collection. Returns 200 if successful, 503 otherwise.
    """
    logging.info("Healthcheck MongoDB called.")
    try:
        sources_collection.count_documents({})
        logging.info("Healthcheck MongoDB successful.")
        return {"status": "ok"}
    except PyMongoError as e:
        logging.error(f"Healthcheck MongoDB failed: {e}")
        return JSONResponse(status_code=503, content={"status": "error", "details": str(e)})

@app.get("/health/influx")
async def health_influx():
    """
        Healthcheck for InfluxDB. Tries to get the health status of the InfluxDB server. Returns 200 if successful, 503 otherwise.
    """
    logging.info("Healthcheck InfluxDB called.")
    try:
        health = client.health()
        if health.status == "pass":
            logging.info("Healthcheck InfluxDB successful.")
            return {"status": "ok"}
        logging.error(f"Healthcheck InfluxDB failed: {health.message}")
        return JSONResponse(status_code=503, content={"status": "error", "details": health.message})
    except ApiException as e:
        logging.error(f"Healthcheck InfluxDB failed: {e}")
        return JSONResponse(status_code=503, content={"status": "error", "details": str(e)})

@app.get("/health/frontend")
async def health_frontend():
    """
        Healthcheck for the frontend. Tries to connect to the frontend service. Returns 200 if successful, 503 otherwise.
    """
    logging.info("Healthcheck Frontend called.")
    try:
        resp = requests.get("http://frontend:80", timeout=2)
        if resp.status_code == 200:
            logging.info("Healthcheck Frontend successful.")
            return {"status": "ok"}
        logging.error(f"Healthcheck Frontend failed: {resp.text}")
        return JSONResponse(status_code=503, content={"status": "error", "details": resp.text})
    except Exception as e:
        logging.error(f"Healthcheck Frontend failed: {e}")
        return JSONResponse(status_code=503, content={"status": "error", "details": str(e)})
    
@app.get("/health/grafana")
async def health_grafana():
    """
        Healthcheck for the Grafana dashboards. Tries to connect to the Grafana service. Returns 200 if successful, 503 otherwise.
    """
    logging.info("Healthcheck Grafana called.")
    try:
        resp = requests.get("http://grafana:3000/api/health", timeout=2)
        if resp.status_code == 200 and resp.json().get("database") == "ok":
            logging.info("Healthcheck Grafana successful.")
            return {"status": "ok"}
        logging.error(f"Healthcheck Grafana failed: {resp.text}")
        return JSONResponse(status_code=503, content={"status": "error", "details": resp.text})
    except Exception as e:
        logging.error(f"Healthcheck Grafana failed: {e}")
        return JSONResponse(status_code=503, content={"status": "error", "details": str(e)})


### Source information endpoints

@app.get("/sources")
async def get_all_sources():
    """
        Get all sources.
    """
    out = dict()
    for source in sources_collection.find({}):
        out[str(source["_id"])] = source
    return out

@app.get("/sources/{source_id}")
async def get_sources(source_id: str = Path(..., description="The ID from the mongoDB of the source to retrieve")):
    """
        Get a source by its ID.
    """
    return sources_collection.find({"_id": source_id})[0]

### Timeseries data endpoints

@app.get("/timeseries/{influx_key}")
async def load_timeseries(influx_key : str, 
                          start : str = Query(None, description="Start time as ISO format (YYYY-MM-DD)"),
                          end : str = Query(None, description="End time as ISO format (YYYY-MM-DD)")):
    """
        Load timeseries data for a given source and telescope from InfluxDB. Start and End can be given as MJD. 
        If no start is given, data from the last year is returned. If no end is given, data up to the current time is returned.
        :param influx_key: InfluxDB key for the source and telescope (e.g. "cygx1_swift").
        :param start: Start time as MJD (optional).
        :param end: End time as MJD (optional).
        :return: Dictionary with timeseries data.
    """
    
    if start is None:
        start = "-1y"
    if end is not None:
        range_str = f'|> range(start: {start}, stop: {end})'
    else:
        range_str = f'|> range(start: {start})'

    query_flux = f"""
        from(bucket: "flashes_data")
            {range_str}
            |> filter(fn: (r) => r._measurement == "flux data")
            |> filter(fn: (r) => r.source == "{influx_key}")
    """
    swift_keys = ["error (15-150 keV)", "flux (15-150 keV)"]
    maxi_keys = ["error (10-20 keV)", "error (2-20 keV)", "error (2-4 keV)", "error (4-10 keV)",
            "flux (10-20 keV)", "flux (2-20 keV)", "flux (2-4 keV)", "flux (4-10 keV)"]
    fermi_keys = ["error (12-50 keV)", "flux (12-50 keV)"]
    combined_keys = ["combined error", "combined flux"]
    hardness_keys = ["hardness error", "hardness ratio"]

    telescope_dict = {
        "swift": swift_keys,
        "maxi": maxi_keys,
        "fermi": fermi_keys,
        "combined": combined_keys,
        "hardness": hardness_keys
    }
    telescope = influx_key.split("_")[0]
    keys = telescope_dict.get(telescope)
    if not keys:
        raise ValueError(f"Unbekanntes Teleskop: {telescope}")
    result = {key: [] for key in keys}

    tables = query_api.query(org="flashes", query=query_flux)

    for key, table in zip(keys, tables):
        for record in table.records:
            result[key].append((record.get_time(), record.get_value()))
    return result
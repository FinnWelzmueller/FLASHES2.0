import logging
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from download import update
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from fastapi.responses import JSONResponse, StreamingResponse
import requests
from fastapi import FastAPI, Path, Query
from datetime import timezone
from scheduler import start_scheduler
from utils import iso_to_mjd

### SETUP ###
load_dotenv()
os.makedirs("./download", exist_ok=True)

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

do_initial_update = os.getenv("DO_INITIAL_UPDATE", "True").lower() in ["true", "1", "yes"]
logging.debug(f"DO_INITIAL_UPDATE is set to {do_initial_update}")
if os.getenv("DO_INITIAL_UPDATE", "True").lower() in ["true", "1", "yes"]:
    logging.info("Running initial update...")
    update(sources_collection, write_api, os.getenv("TEMP_DIR", "./_temp"))
else:
    logging.info("Skipping initial update.")

### Scheduler ###

start_scheduler(args=[sources_collection, write_api, os.getenv("TEMP_DIR", "./_temp")])

### FastAPI ###
logging.info("Starting API...")
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
@app.get("/timeseries")
async def load_timeseries(influx_key : str = Query(None, description="Influx Key of the source from the MongoDB"),
                          channel : str = Query(None, description="Channel of the data"),
                          start : str = Query(None, description = "Start time as ISO format (YYYY-MM-DD)"),
                          end : str = Query(None, description="End time as ISO format (YYYY-MM-DD)")):
    
    channel_in_influx = f"flux ({channel} keV)"
    swift_data_cols = ["flux (15-150 keV)", "flux (15-150 keV) max", "flux (15-150 keV) min"]
    maxi_data_cols = ["flux (10-20 keV)", "flux (10-20 keV) max", "flux (10-20 keV) min",
                      "flux (2-20 keV)", "flux (2-20 keV) max", "flux (2-20 keV) min",
                      "flux (2-4 keV)", "flux (2-4 keV) max", "flux (2-4 keV) min",
                      "flux (4-10 keV)", "flux (4-10 keV) max", "flux (4-10 keV) min"]
    fermi_data_cols = ["flux (12-50 keV)", "flux (12-50 keV) max", "flux (12-50 keV) min"]
    combined_data_cols = ["combined flux", "combined flux max", "combined flux min"]
    hardness_data_cols = ["hardness ratio", "hardness ratio max", "hardness ratio min"]

    data_cols_dict = {
        "swift": swift_data_cols,
        "maxi": maxi_data_cols,
        "fermi": fermi_data_cols,
        "combined": combined_data_cols,
        "hardness": hardness_data_cols
    }

    channel_dict = {
        "swift": ["15-150"],
        "maxi": ["10-20", "2-20", "2-4", "4-10"],
        "fermi": ["12-50"],
        "combined": [],
        "hardness": []
    }

    telescope = influx_key.split("_")[0]
    if not list(data_cols_dict.keys()):
        return {"message": f"Unknown telescope: {telescope}, please check your URL"}
    if telescope in list(channel_dict.keys()):
        if (channel not in channel_dict[telescope]) and (telescope in ["swift" , "maxi", "fermi"]):
            return {"message": f"Unknown channel {channel} for telescope {telescope}, please check your URL"}
        
    if start is None:
        start_influx = "-1y"
    else:
        start_influx = start + "T00:00:00Z"
    if end is not None:
        range_str = f'|> range(start: {start_influx}, stop: {end + "T00:00:00Z"})'
    else:
        range_str = f'|> range(start: {start_influx})'

    query_flux = f"""
        from(bucket: "flashes_data")
            {range_str}
            |> filter(fn: (r) => r._measurement == "flux data")
            |> filter(fn: (r) => r.source == "{influx_key}")
            |> keep(columns: ["_time", "_field", "_value"])
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
            |> drop(columns: ["_start","_stop"])
    """

    # Getting data
    out = []
    for idx, row in query_api.query_data_frame(org="flashes", query=query_flux).iterrows():
        timestamp_dict = {"time": row["_time"]}

        if channel in list(channel_dict[telescope]): # Telescope case (Swift, MAXI, Fermi)
            timestamp_dict[channel_in_influx] = row[channel_in_influx]
            timestamp_dict[channel_in_influx + " max"] = row[channel_in_influx] + row[channel_in_influx.replace("flux", "error")]
            timestamp_dict[channel_in_influx + " min"] = row[channel_in_influx] - row[channel_in_influx.replace("flux", "error")]
        
        else: # Hardness, Combined
            if telescope == "hardness":
                timestamp_dict["hardness ratio"] = row["hardness ratio"]
                timestamp_dict["hardness ratio" + " max"] = row["hardness ratio"] + row["hardness error"]
                timestamp_dict["hardness ratio" + " min"] = row["hardness ratio"] - row["hardness error"]
            if telescope == "combined":
                timestamp_dict["combined flux"] = row["combined flux"]
                timestamp_dict["combined flux" + " max"] = row["combined flux"] + row["combined flux".replace("flux", "error")]
                timestamp_dict["combined flux" + " min"] = row["combined flux"] - row["combined flux".replace("flux", "error")]

        out.append(timestamp_dict)

        # correct timestamp object: rewrite as grafana-understandable string
    for entry in out:
        t = entry["time"]
        if hasattr(t, "to_pydatetime"):
            t = t.to_pydatetime()
        entry["time"] = t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return out

@app.get("/download/{influx_key}")
async def load_download(influx_key : str, 
                          start : str = Query(None, description="Start time as ISO format (YYYY-MM-DD)"),
                          end : str = Query(None, description="End time as ISO format (YYYY-MM-DD)")):
    """
        Load timeseries data for a given source and telescope from InfluxDB. Start and End can be given as MJD. 
        If no start is given, data from the last year is returned. If no end is given, data up to the current time is returned.
        :param influx_key: InfluxDB key for the source and telescope (e.g. "swift_smcx-3").
        :param start: Start time in iso format (YYYY-MM-DD) (optional).
        :param end: End time in iso format (YYYY-MM-DD) (optional).
        :return: Dictionary with timeseries data.
    """
    
    if start is None:
        start = "-1y"
    else:
        start = start + "T00:00:00Z"
    if end is not None:
        range_str = f'|> range(start: {start}, stop: {end + "T00:00:00Z"})'
    else:
        range_str = f'|> range(start: {start})'

    query_flux = f"""
        from(bucket: "flashes_data")
            {range_str}
            |> filter(fn: (r) => r._measurement == "flux data")
            |> filter(fn: (r) => r.source == "{influx_key}")
            |> keep(columns: ["_time", "_field", "_value"])
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
            |> drop(columns: ["_start","_stop"])
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
        raise ValueError(f"Unknown telescope: {telescope}")
    
    # Getting data
    out = []
    for idx, row in query_api.query_data_frame(org="flashes", query=query_flux).iterrows():
        timestamp_dict = {"time": row["_time"]}

        for col in telescope_dict[telescope]:
            timestamp_dict[col] = row[col]
        out.append(timestamp_dict)

    if not out or len(out) == 0:
        return JSONResponse(status_code=404, content={"message": "No data for given query"})
    
    for entry in out:
        t = entry["time"]
        if hasattr(t, "to_pydatetime"):
            t = t.to_pydatetime()
        entry["time"] = t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        entry["mjd"] = iso_to_mjd(entry["time"])

    # bring things into the right order
    download = list()
    for row in out:
        row_dict = dict()
        row_dict["time"] = row["time"]
        row_dict["mjd"] = row["mjd"]
        for k in keys:
            row_dict[k] = row.get(k, None)
        download.append(row_dict)

    # start download
    keys = download[0].keys()

    def generate():
        # Header
        yield ("\t".join(keys) + "\n").encode("utf-8")
        # Reihen
        for row in download:
            vals = [str(row.get(k)) for k in keys]
            yield ("\t".join(vals) + "\n").encode("utf-8")
    
    filename = f"{influx_key}_data.txt"
    logging.info(f"Preparing download for {filename} with {len(download)} rows.")
    return StreamingResponse(generate(), media_type="text/tab-separated-values", headers={
        "Content-Disposition": f'attachment; filename="{filename}"'
    })
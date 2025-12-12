import logging
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from download import update
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from fastapi.responses import JSONResponse, StreamingResponse, RedirectResponse
import requests
from fastapi import FastAPI, Path, Query
from fastapi.staticfiles import StaticFiles
from datetime import timezone
from scheduler import start_scheduler
from utils import iso_to_mjd, flux_to_mcrab
from urllib.parse import quote_plus


### SETUP ###
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
app.mount("/static", StaticFiles(directory="./static"), name="static")


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
        resp = requests.get("http://grafana:3001/api/health", timeout=2)
        if resp.status_code == 200 and resp.json().get("database") == "ok":
            logging.info("Healthcheck Grafana successful.")
            return {"status": "ok"}
        logging.error(f"Healthcheck Grafana failed: {resp.text}")
        return JSONResponse(status_code=503, content={"status": "error", "details": resp.text})
    except Exception as e:
        logging.error(f"Healthcheck Grafana failed: {e}")
        return JSONResponse(status_code=503, content={"status": "error", "details": str(e)})

### Tags endpoints
@app.get("/tags")
async def get_all_tags():
    """
        Gets all tags.
        :return: List of all unique tags in the sources collection.
    """
    tags = set()
    for source in sources_collection.find({}):
        source_tags = source.get("labels_constant", [])
        for tag in source_tags:
            tags.add(tag)
    return sorted(list(tags))

@app.get("/tags/{tag_name}")
async def get_sources_by_tag(tag_name: str = Path(..., description="The tag to filter sources by")):
    """
        Get all sources with a given tag. The tag name is converted to uppercase and hyphens are replaced with spaces to match the format in the database.
        :param tag_name: The tag to filter sources by.
        :return: List of sources with the given tag.
    """
    if tag_name.upper().replace("-", " ") not in await get_all_tags():
        return JSONResponse(status_code=404, content={"message": f"Tag {tag_name} not found."})
    sources = []
    for source in sources_collection.find({}):
        if tag_name.upper().replace("-", " ") in source.get("labels_constant", []):
            sources.append(source)
    return sources
### Source information endpoints

@app.get("/sources")
async def get_all_sources():
    """
        Get all sources.
    """
    return list(source for source in sources_collection.find({}))

@app.get("/sources/{source_id}")
async def get_sources(source_id: str = Path(..., description="The ID from the mongoDB of the source to retrieve")):
    """
        Get a source by its ID.
    """
    source_id = source_id.replace("%2B", "+") # supresses the wrong decoding from the URL in the backend
    return sources_collection.find({"_id": source_id})[0]



### Timeseries data endpoints
@app.get("/timeseries")
async def load_timeseries(influx_key : str = Query(None, description="Influx Key of the source from the MongoDB"),
                          channel : str = Query(None, description="Channel of the data"),
                          start : str = Query(None, description = "Start time as ISO format (YYYY-MM-DD)"),
                          end : str = Query(None, description="End time as ISO format (YYYY-MM-DD)")):
    
    def calculate_hardness(swift_flux, swift_error, maxi_flux, maxi_error):
        """
            handles hardness ratio calculation and error propagation for the swift and maxi data. No div-by-zero check needs to be done as this was already checked in load_timeseries before this function is called.
            :param swift_flux: swift flux value:
            :param swift_error: swift error value
            :param maxi_flux: maxi flux value
            :param maxi_error: maxi error value

            :return: hardness ratio, hardness ratio + hardness error, hardness ratio - hardness error

        """
        
        # convert to mCrab
        swift_flux = flux_to_mcrab(swift_flux, "15-50")
        swift_error = flux_to_mcrab(swift_error, "15-50")
        maxi_flux = flux_to_mcrab(maxi_flux, "2-20")
        maxi_error = flux_to_mcrab(maxi_error, "2-20")

        # calculate hardness 
        hardness = swift_flux / maxi_flux
        hardness_error = hardness * np.sqrt((swift_error / swift_flux) ** 2 + (maxi_error / maxi_flux) ** 2)
        return hardness, hardness + hardness_error, hardness - hardness_error
            
    def calculate_combined(swift_flux, swift_error, maxi_flux, maxi_error):
        """
            handles combined flux calculation and error propagation for the swift and maxi data
            :param swift_flux: swift flux value:
            :param swift_error: swift error value
            :param maxi_flux: maxi flux value
            :param maxi_error: maxi error value
        """

        # convert to mCrab
        swift_flux = flux_to_mcrab(swift_flux, "15-50")
        swift_error = flux_to_mcrab(swift_error, "15-50")
        maxi_flux = flux_to_mcrab(maxi_flux, "2-20")
        maxi_error = flux_to_mcrab(maxi_error, "2-20")

        # calculate combined flux
        combined_flux = swift_flux + maxi_flux
        combined_error = combined_flux * np.sqrt((swift_error / swift_flux) ** 2 + (maxi_error / maxi_flux) ** 2)
        return combined_flux, combined_flux + combined_error, combined_flux - combined_error
    
    channel_in_influx = f"flux ({channel} keV)"
    swift_data_cols = ["flux (15-50 keV)", "flux (15-50 keV) max", "flux (15-50 keV) min"]
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
        "swift": ["15-50"],
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
        start = "-1y"
    if end is not None:
        range_str = f'|> range(start: {start}, stop: {end})'
    else:
        range_str = f'|> range(start: {start})'

    out = []
    if telescope in ["swift", "maxi", "fermi"]: # Telescope case: no further calculation needed
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
        for idx, row in query_api.query_data_frame(org="flashes", query=query_flux).iterrows():
            timestamp_dict = {"time": row["_time"]}

            if channel in list(channel_dict[telescope]): # Telescope case (Swift, MAXI, Fermi)
                timestamp_dict[channel_in_influx] = row[channel_in_influx]
                timestamp_dict[channel_in_influx + " max"] = row[channel_in_influx] + row[channel_in_influx.replace("flux", "error")]
                timestamp_dict[channel_in_influx + " min"] = row[channel_in_influx] - row[channel_in_influx.replace("flux", "error")]
                out.append(timestamp_dict)
        
            else: # Hardness, Combined
                if telescope == "hardness":
                    if row["hardness ratio"] > 0:
                        timestamp_dict["hardness ratio"] = row["hardness ratio"]
                        timestamp_dict["hardness ratio" + " max"] = row["hardness ratio"] + row["hardness error"]
                        timestamp_dict["hardness ratio" + " min"] = row["hardness ratio"] - row["hardness error"]
                        out.append(timestamp_dict)
                if telescope == "combined":
                    if row["combined flux"] > 0:
                        timestamp_dict["combined flux"] = row["combined flux"]
                        timestamp_dict["combined flux" + " max"] = row["combined flux"] + row["combined flux".replace("flux", "error")]
                        timestamp_dict["combined flux" + " min"] = row["combined flux"] - row["combined flux".replace("flux", "error")]
                        out.append(timestamp_dict)
    else: # Combined and Hardness case: need to get both swift and maxi data
        # Getting data
        influx_key = influx_key.replace(telescope, "swift")
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
        df_swift = query_api.query_data_frame(org="flashes", query=query_flux)
        influx_key= influx_key.replace("swift", "maxi")
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
        df_maxi = query_api.query_data_frame(org="flashes", query=query_flux)
        merged = pd.merge(df_swift, df_maxi, left_on="_time",right_on="_time", how='outer')

        for idx, row in merged.iterrows():
            if not (pd.isna(row['flux (2-20 keV)']) or pd.isna(row['flux (15-50 keV)'])):
                timestamp_dict = {"time": row["_time"]}
                if telescope == "hardness":
                    hardness, hardness_max, hardness_min = calculate_hardness(swift_flux=row["flux (15-50 keV)"], swift_error=row["error (15-50 keV)"], maxi_flux=row["flux (2-20 keV)"], maxi_error=row["error (2-20 keV)"])
                    if hardness > 0:
                        timestamp_dict["hardness ratio"] = hardness
                        timestamp_dict["hardness ratio" + " max"] = hardness_max
                        timestamp_dict["hardness ratio" + " min"] = hardness_min
                        out.append(timestamp_dict)
                if telescope == "combined":
                    combined, combined_max, combined_min = calculate_combined(swift_flux=row["flux (15-50 keV)"], swift_error=row["error (15-50 keV)"], maxi_flux=row["flux (2-20 keV)"], maxi_error=row["error (2-20 keV)"])
                    if combined > 0:
                        timestamp_dict["combined flux"] = combined
                        timestamp_dict["combined flux" + " max"] = combined_max
                        timestamp_dict["combined flux" + " min"] = combined_min
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
    if end is not None:
        range_str = f'|> range(start: {start}, stop: {end})'
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

    swift_keys = ["error (15-50 keV)", "flux (15-50 keV)"]
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
            timestamp_dict[col+" mCrab"] = flux_to_mcrab(row[col], col.split()[1].replace("(", ""))
        out.append(timestamp_dict)
    print(out)
    if not out or len(out) == 0:
        return JSONResponse(status_code=404, content={"message": "No data for given query"})
    
    for entry in out:
        t = entry["time"]
        if hasattr(t, "to_pydatetime"):
            t = t.to_pydatetime()
        entry["time"] = t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        entry["mjd"] = iso_to_mjd(entry["time"])

    # bring things into the right order
        download = []
    mcrab_keys = [f"{k} mCrab" for k in keys]

    for row in out:
        row_dict = {"time": row["time"], "mjd": row["mjd"]}
        for k in keys + mcrab_keys:
            row_dict[k] = row.get(k)
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

@app.get("/download/hid/{hardness_influxkey}/{combined_influxkey}")
async def download_hid(hardness_influxkey: str, 
                        combined_influxkey: str, 
                        start : str = Query(None, description="Start time as ISO format (YYYY-MM-DD)"),
                        end : str = Query(None, description="End time as ISO format (YYYY-MM-DD)")):
    """
    Downloads data from a HID. 
    The file comtains the columns _time, hardness ratio, hardness error, combined flux, combined error in a given time frame.
    Returns only rows where both hardness ratio and combined flux are available.
    :param hardness_influxkey: InfluxDB source key for hardness ratio data.
    :param combined_influxkey: InfluxDB source key for combined flux data.
    :param start: Start time for data retrieval.
    :param end: End time for data retrieval.
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
            |> filter(fn: (r) => r.source == "{hardness_influxkey}")
            |> keep(columns: ["_time", "_field", "_value"])
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
            |> drop(columns: ["_start","_stop"])
    """
    hardness_data = query_api.query_data_frame(org="flashes", query=query_flux)
    query_flux = f"""
        from(bucket: "flashes_data")
            {range_str}
            |> filter(fn: (r) => r._measurement == "flux data")
            |> filter(fn: (r) => r.source == "{combined_influxkey}")
            |> keep(columns: ["_time", "_field", "_value"])
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
            |> drop(columns: ["_start","_stop"])
    """
    combined_data = query_api.query_data_frame(org="flashes", query=query_flux)
    merge = hardness_data.merge(combined_data, on="_time", how="inner")
    download = merge[["_time", "hardness ratio" ,"hardness error", "combined flux", "combined error"]]
    cols = download.columns.tolist()
    cols[0] = "time"

    def generate():
        # Header
        yield ("\t".join(cols) + "\n").encode("utf-8")

        # Zeilen
        for _, row in download.iterrows():
            t = row["_time"]
            if hasattr(t, "to_pydatetime"):
                t = t.to_pydatetime()
            # als UTC-ISO-String ausgeben
            t = t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            values = [t] + [str(row[c]) for c in cols[1:]]
            yield ("\t".join(values) + "\n").encode("utf-8")

    name = hardness_influxkey.replace("hardness_", "")
    filename = f"{name}_hid.txt"
    logging.info(f"Preparing HID download for {filename} with {len(download)} rows.")

    return StreamingResponse(
        generate(),
        media_type="text/tab-separated-values",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )
    

@app.get("/plots/{source_id}")
def plot_redirect(source_id: str):
    """
    Redirect to the appropriate Grafana dashboard for a given source based on its available data.
    """
    GRAFANA_BASE_URL = "http://localhost:3001"
    doc = sources_collection.find_one({"_id": source_id})
    if not doc:
        return JSONResponse(status_code=404, content={"message": f"Source with INTEGRAL name {source_id} not found."})

    # Robuste Fall-Erkennung: existiert ein Key?
    has_swift = bool(doc.get("swift"))
    has_maxi  = bool(doc.get("maxi"))
    has_fermi = bool(doc.get("fermi"))

    combos = {
        (True,  False, False): "flashes-swift",
        (False, True,  False): "flashes-maxi",
        (True,  True,  False): "flashes-swift-maxi",
        (True,  False, True ): "flashes-swift-fermi",
        (True,  True,  True ): "flashes-swift-maxi-fermi",
    }
    dashboard_uid = combos.get((has_swift, has_maxi, has_fermi), "")
    if not dashboard_uid:
        return JSONResponse(status_code=400, content={"message": "No available dashboards for this source."})

    base = f"{GRAFANA_BASE_URL}/d/{dashboard_uid}/{dashboard_uid}" if GRAFANA_BASE_URL else f"/d/{dashboard_uid}/{dashboard_uid}"

    params = [f"var-integral_name={quote_plus(doc['integral_name'])}"]

    params.append("kiosk")
    params.append("theme=dark")
    
    for telescope, subkey in [
        ("swift", "swift_influxkey"),
        ("maxi", "maxi_influxkey"),
        ("fermi", "fermi_influxkey"),
        ("hardness", "hardness_influxkey"),
        ("combined", "combined_influxkey")
    ]:
        influx_key = None
        if telescope == "hardness":
            telescope = "hardness_ratio"
        # variant: nested dict like doc["swift"]["influx_key"] or doc["swift"]["influxkey"]
        tval = doc.get(telescope)
        if isinstance(tval, dict):
            influx_key = tval.get("influx_key") 
        if influx_key:
            params.append(f"var-{subkey}={quote_plus(influx_key)}")

    return RedirectResponse(base + "?" + "&".join(params))

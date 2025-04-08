import requests
from pymongo import MongoClient
import gzip
import numpy as np
import shutil
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv
from astropy.time import Time
from astropy.table import Table

# connect to mongodb
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


def get_utc_time(mjd: pd.Series) -> pd.Series:
    dt_series = pd.to_datetime(Time(mjd, format="mjd").to_datetime())
    dt_series = pd.Series(dt_series).dt.floor("D")  # set to midnight
    return dt_series


def download_fermi(url, temp_dir="./_temp"):
    """Lädt eine .gz-komprimierte FITS-Datei herunter und entpackt sie."""
    os.makedirs(temp_dir, exist_ok=True)

    filename = url.split("/")[-1]  # Filename is last part of the url
    compressed_path = os.path.join(temp_dir, filename)
    extracted_path = compressed_path.replace(".gz", "")

    # Download file
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(compressed_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded file: {compressed_path}")

        # Entpacken
        with gzip.open(compressed_path, "rb") as f_in:
            with open(extracted_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Unziped file: {extracted_path}")
        os.remove(compressed_path)
        dat = Table.read(extracted_path, format='fits', hdu=2)
        names = [name for name in dat.colnames if len(dat[name].shape) <= 1]
        df = dat[names].to_pandas()
        df = df[['PSRTIME', 'AMPLITUDE', 'AMPLITUDE_ERR']]
        df.columns = ["TIME", "FLUX 12-50", "ERROR 12-50"]
        df["UTC TIME"] = get_utc_time(df["TIME"])
        shutil.rmtree(temp_dir)
        return df
    else:
        print(f"Failed to download {url}, status code: {response.status_code}")
        return None


def download_to_dataframe(url, telescope):
    try:
        response = requests.get(url)
        if response.status_code == 200: # response ok
            data = StringIO(response.text)
            df = pd.read_csv(data, sep=r"\s+", comment="#", header=None)

            if telescope == "swift":
                df.columns = [
                "TIME", "FLUX 15-150", "ERROR 15-150", "YEAR", "DAY", "STAT_ERR", "SYS_ERR", 
                "DATA_FLAG", "TIMEDEL_EXPO", "TIMEDEL_CODED", "TIMEDEL_DITHERED"
                ]
            elif telescope == "maxi":
                df.columns = [
                "TIME", "FLUX 2-20", "ERROR 2-20", "FLUX 2-4", "ERROR 2-4", "FLUX 4-10", "ERROR 4-10", "FLUX 10-20", "ERROR 10-20"
                ]
            else:
                raise KeyError(f"Telescope {telescope} not found")
            
            df["UTC TIME"] = get_utc_time(df["TIME"])
            return df
        else:
            print(f"Failed to download {url}, status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return None

def write_hardness_combined_to_influx(source, df):  # hardness key is swiftkey but with "hardness" instead of "swift" at the front
    if df is None:
        print(f"Skipping hardness ratio upload for source {source["integral_name"]}: no data received.")
    try:
        hkey = source["hardness_ratio"]["influx_key"]
    except Exception:
        raise KeyError(f"Source {source["integral_name"]} should not have a defined hardness ratio.")
    try:
        ckey = source["combined"]["influx_key"]
    except Exception:
        raise KeyError(f"Source {source["integral_name"]} should not have a defined combined flux.")
    hpoints, cpoints = [], []
    for idx, row in df.iterrows():
        hpoint = Point("hardness data").tag("source", hkey) \
        .field("hardness ratio", row["hardness"]).field("hardness error", row["hardness_error"])\
        .time(row["Time"])
        hpoints.append(hpoint)

        cpoint = Point("combined flux data").tag("source", ckey) \
        .field("combined swift maxi flux", row["combined_flux"]).field("combined swift maxi error", row["combined_error"])\
        .time(row["Time"])
        cpoints.append(cpoint)
    for i in range(0, len(hpoints), batch_size):
        write_api.write(bucket=influx_bucket, org=influx_org, record=hpoints[i:i + batch_size],write_precision="ns")
    print(f"Wrote {source["integral_name"]} (hardness data) to influxDB")
    for i in range(0, len(cpoints), batch_size):
        write_api.write(bucket=influx_bucket, org=influx_org, record=cpoints[i:i + batch_size],write_precision="ns")
    print(f"Wrote {source["integral_name"]} (combined flux data) to influxDB")


def write_flux_to_influx(source, telescope, df):
    if df is None:
        print(f"Skipping {telescope}-data upload for source {source["integral_name"]}: no data received.")
        return
    try:
        key = source[telescope]["influx_key"]
    except KeyError:
        raise KeyError(f"Telescope {telescope} not found")
    
    points = list()
    for idx, row in df.iterrows():
        if telescope == "swift":
            point = Point("flux data").tag("source", key) \
            .field("flux (15-150 keV)", row["FLUX 15-150"]).field("error (15-150 keV)", row["ERROR 15-150"]) \
            .time(row["UTC TIME"])
        elif telescope == "maxi":
            point = Point("flux data").tag("source", key) \
            .field("flux (2-20 keV)", row["FLUX 2-20"]).field("error (2-20 keV)", row["ERROR 2-20"]) \
            .field("flux (2-4 keV)", row["FLUX 2-4"]).field("error (2-4 keV)", row["ERROR 2-4"]) \
            .field("flux (4-10 keV)", row["FLUX 4-10"]).field("error (4-10 keV)", row["ERROR 4-10"]) \
            .field("flux (10-20 keV)", row["FLUX 10-20"]).field("error (10-20 keV)", row["ERROR 10-20"]) \
            .time(row["UTC TIME"])
        else:
            point = Point("flux data").tag("source", key) \
            .field("flux (12-50 keV)", row["FLUX 12-50"]).field("error (12-50 keV)", row["ERROR 12-50"]) \
            .time(row["UTC TIME"])
        points.append(point)
    
    for i in range(0, len(points), batch_size):
        write_api.write(bucket=influx_bucket, org=influx_org, record=points[i:i + batch_size],write_precision="ns")
    print(f"Wrote {source["integral_name"]} ({telescope} data) to influxDB")

def update():
    for source in sources_collection.find({}):
        if source["swift"]:
            print(f"Updating {source["integral_name"]} (Swift data)...")
            swift_df = download_to_dataframe(source["swift"]["data_url"], "swift")
            last_ts_swift = find_last_timestamp(source["integral_name"], source["swift"]["influx_key"])
            new_swift_df = filter_new_data(swift_df, last_ts_swift) if last_ts_swift else swift_df
            write_flux_to_influx(source, "swift", new_swift_df)
            print(f"Updating {source["integral_name"]} (Swift data) done.")

        if source["maxi"]:
            print(f"Updating {source["integral_name"]} (MAXI data)...")
            maxi_df = download_to_dataframe(source["maxi"]["data_url"], "maxi")
            last_ts_maxi = find_last_timestamp(source["integral_name"], source["maxi"]["influx_key"])
            new_maxi_df = filter_new_data(maxi_df, last_ts_maxi) if last_ts_maxi else maxi_df
            write_flux_to_influx(source, "maxi", new_maxi_df)
            print(f"Updating {source["integral_name"]} (MAXI data) done.")
            
        if source["fermi"]:
            print(f"Updating {source["integral_name"]} (Fermi data)...")
            fermi_df = download_fermi(source["fermi"]["data_url"], temp_dir="./_temp")
            last_ts_fermi = find_last_timestamp(source["integral_name"], source["fermi"]["influx_key"])
            new_fermi_df = filter_new_data(fermi_df, last_ts_fermi) if last_ts_fermi else fermi_df
            write_flux_to_influx(source, "fermi", new_fermi_df)
            print(f"Updating {source["integral_name"]} (Fermi data) done.")
        
        #hardness calculation
        if (source.get("swift") and source.get("maxi") and not new_swift_df.empty and not new_maxi_df.empty):
            print(f"Updating {source["integral_name"]} (Hardness data)...")
            calculate_hardness(source)
            print(f"Updating {source["integral_name"]} (Hardness data) done.")

def filter_new_data(df: pd.DataFrame, last_timestamp: pd.Timestamp) -> pd.DataFrame:
    return df[df["UTC TIME"] > last_timestamp]

def find_last_timestamp(sourcename, key):
    query = f"""
    from(bucket: "{influx_bucket}")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement == "flux data")
      |> filter(fn: (r) => r.source == "{key}")
      |> keep(columns: ["_time"])
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1)
    """
    tables = query_api.query(org=influx_org, query=query)
    last_timestamp = None
    
    for table in tables:
        for record in table.records:
            last_timestamp = record.get_time()
            break
        if last_timestamp:
            break

    if last_timestamp:
        print(f"Letzter Timestamp für {sourcename}: {last_timestamp}")
    else:
        print(f"Keine Daten für {sourcename} in InfluxDB gefunden, lade alle.")


def initialize():
    # iterate over sources
    for source in sources_collection.find({}):
        if source["swift"]:
            swift_df = download_to_dataframe(source["swift"]["data_url"], "swift")
            write_flux_to_influx(source, "swift", swift_df)
        if source["maxi"]:
            maxi_df = download_to_dataframe(source["maxi"]["data_url"], "maxi")
            write_flux_to_influx(source, "maxi", maxi_df)
        if source["fermi"]:
            fermi_df = download_fermi(source["fermi"]["data_url"], temp_dir="./_temp")
            write_flux_to_influx(source, "fermi", fermi_df)
    for source in sources_collection.find({"swift": {"$ne": None},"maxi": {"$ne": None}}):
        calculate_hardness(source)


def influx_to_pandas(influxtable):
    df = pd.DataFrame(columns=["Time", "Value"])
    times = []
    values = []
    try:
        for table in influxtable:
            for entry in table:
                times.append(entry.get_time())
                values.append(entry.get_value())
    except Exception as e:
        print(e)
    df["Time"] = times
    df["Value"] = values
    return df



def calculate_hardness(source):
    maxi_key = source["maxi"]["influx_key"]
    swift_key = source["swift"]["influx_key"]
    df = pd.DataFrame(columns=["Time", "flux combined", "error combined", "hardness ratio", "hardness ratio error"])

    # get MAXI data (2-20 keV)
    query = f"""
    from(bucket: "{influx_bucket}")\
      |> range(start: 0)\
      |> filter(fn: (r) => r._measurement == "flux data")\
      |> filter(fn: (r) => r._field == "flux (2-20 keV)")\
      |> filter(fn: (r) => r.source == "{maxi_key}")
    """
    maxi_flux_table = query_api.query(org=influx_org, query=query)
    maxi_flux = influx_to_pandas(maxi_flux_table)
    
    query = f"""
    from(bucket: "{influx_bucket}")\
      |> range(start: 0)\
      |> filter(fn: (r) => r._measurement == "flux data")\
      |> filter(fn: (r) => r._field == "error (2-20 keV)")\
      |> filter(fn: (r) => r.source == "{maxi_key}")
    """
    maxi_error_table = query_api.query(org=influx_org, query=query)
    maxi_error = influx_to_pandas(maxi_error_table)

    # get Swift/BAT data (15-50 keV)
    query = f"""
    from(bucket: "{influx_bucket}")\
      |> range(start: 0)\
      |> filter(fn: (r) => r._measurement == "flux data")\
      |> filter(fn: (r) => r._field == "flux (15-150 keV)")\
      |> filter(fn: (r) => r.source == "{swift_key}")
    """
    swift_flux_table = query_api.query(org=influx_org, query=query)
    swift_flux = influx_to_pandas(swift_flux_table)

    query = f"""
    from(bucket: "{influx_bucket}")\
      |> range(start: 0)\
      |> filter(fn: (r) => r._measurement == "flux data")\
      |> filter(fn: (r) => r._field == "error (15-150 keV)")\
      |> filter(fn: (r) => r.source == "{swift_key}")
    """
    swift_error_table = query_api.query(org=influx_org, query=query)
    swift_error = influx_to_pandas(swift_error_table)

    # merge dataframes
    df = maxi_flux.merge(maxi_error, on='Time', suffixes=('_maxi_flux', '_maxi_err')) \
                  .merge(swift_flux, on='Time') \
                  .merge(swift_error, on='Time', suffixes=('_swift_flux', '_swift_err'))


    # calculations on hardness and sum
    df['hardness'] = df['Value_swift_flux'] / df['Value_maxi_flux']
    df['hardness_error'] = df['hardness'] * np.sqrt((df['Value_swift_err'] / df['Value_swift_flux'])**2 + (df['Value_maxi_err'] / df['Value_maxi_flux'])**2)
    df['combined_flux'] = df['Value_swift_flux'] + df['Value_maxi_flux']
    df['combined_error'] = np.sqrt(df['Value_swift_err']**2 + df['Value_maxi_err']**2)

    write_hardness_combined_to_influx(source, df)


update()
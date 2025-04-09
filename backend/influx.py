from influxdb_client import Point
from backend.config import batch_size, influx_bucket, influx_org, write_api, query_api
from backend.utils import influx_to_pandas
import pandas as pd
import numpy as np

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


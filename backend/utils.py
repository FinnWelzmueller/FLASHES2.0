import pandas as pd
from astropy.time import Time
from backend.config import influx_bucket, influx_org, query_api

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
        print(f"\t Last timestamp for {sourcename}: {last_timestamp}")
    else:
        print(f"\t No data for {sourcename} found in database, uploading everything..")


def get_utc_time(mjd: pd.Series) -> pd.Series:
    dt_series = pd.to_datetime(Time(mjd, format="mjd").to_datetime())
    dt_series = pd.Series(dt_series).dt.floor("D")  # set to midnight
    return dt_series





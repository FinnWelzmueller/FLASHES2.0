import logging
import pandas as pd
from astropy.time import Time

def set_last_timestamp(sources_collection, source: dict, telescope: str, timestamp: int) -> None:
    """
    Sets the last timestamp for a given source and telescope in the MongoDB.
    :param db: MongoDB database object.
    :param source: Source dictionary from MongoDB.
    :param telescope: "swift", "maxi" or "fermi"
    :param timestamp: Last timestamp to be set.
    :return: None
    """
    try:
        sources_collection.update_one(
            {"_id": source['_id']},
            {"$set": {f"{telescope}.last_timestamp": int(timestamp)}}
        )
    except Exception as e:
        logging.error(f"Error setting last timestamp for {source['integral_name']} from {telescope}: {e}")
        return
    logging.debug(f"Set last timestamp for {source['integral_name']} from {telescope} to {timestamp}.")


def get_utc_time(mjd: pd.Series) -> pd.Series:
    """
    Converts MJD to UTC time at midnight.
    :param mjd: Series of MJD times.
    :return: Series of UTC times at midnight.
    """
    dt_series = pd.to_datetime(Time(mjd, format="mjd").to_datetime())
    dt_series = pd.Series(dt_series).dt.floor("D")  # set to midnight
    return dt_series

def mjd_to_iso(mjd):
    """
    Converts a MJD int to an ISO8601 UTC string.
    """
    t = Time(mjd, format="mjd")
    return t.to_datetime().strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_to_mjd(iso):
    """
    Converts an ISO8601 UTC string to a MJD itn.
    """
    t = Time(iso, format="isot", scale="utc")
    return int(t.mjd)
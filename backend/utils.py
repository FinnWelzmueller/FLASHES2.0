import logging
import pandas as pd
from astropy.time import Time

def set_last_timestamp(sources_collection, source: dict, telescope: str, timestamp: int) -> None:
    """
    Sets the last timestamp for a given source and telescope in the MongoDB. 
    From the timestamp, 3 days are subtracted to account for possible changes (esp. in Swift/BAT data).
    :param db: MongoDB database object.
    :param source: Source dictionary from MongoDB.
    :param telescope: "swift", "maxi" or "fermi"
    :param timestamp: Last timestamp to be set.
    :return: None
    """
    timestamp -= 3 # Subtract 3 to account for account for changes in the last datapoints (see Issue #29)
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

def flux_to_mcrab(flux: float, channel:str) -> float:
    """
    Converts a number in counts/cm2/s into mCrab based on the conversion factors from FLASHES1.0 
    :param flux: flux in counts/cm2/s
    :param channel: energy channel
    :return: number in mCrab
    """
    factors = {
        "2-4": 1.0/2.1 * 1000, # MAXI
        "4-10": 1.0/1.2 * 1000, # MAXI
        "10-20": 1.0/0.4 * 1000, # MAXI
        "2-20": 1.0/3.2 * 1000, # MAXI
        "15-50": 1.0/0.22 * 1000, # Swift-BAT
        "12-50": 1.0/4.5 * 1000 # Fermi-GBM
    }
    return flux * factors[channel]
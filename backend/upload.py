from influxdb_client import Point
import pandas as pd
import logging

def write_to_influx(df: pd.DataFrame, write_api, source: dict, telescope: str, sources_collection) -> None:
    """
    Writes data from a dataframe to the InfluxDB. The parameter telescope describes the data source. 
    It can be "swift", "maxi" or "fermi" for lightcurve data or "hardness" or "combined" for additinoal calculations.
    :param df: DataFrame containing the data to be written.
    :param source: Source dictionary from MongoDB.
    :param telescope: "swift", "maxi", "fermi", "hardness" or "combined"
    :param influx_token": Token for InfluxDB authentication.
    :return: None
    """
    logging.debug(f"Writing data to InfluxDB for {source['integral_name']} from {telescope}...")

    try:
        key = source[telescope]['influx_key']
    except KeyError:
        raise KeyError(f"Telescope {telescope} not found")
    points = []
    if telescope == "swift":
        for index, row in df.iterrows():
            p = Point("flux data").tag("source", key) \
            .field("flux (15-150 keV)", row['FLUX 15-150']).field("error (15-150 keV)", row['ERROR 15-150']) \
            .time(row['UTC TIME'])
            points.append(p)

    if telescope == "maxi":
        for index, row in df.iterrows():
            p = Point("flux data").tag("source", key) \
            .field("flux (2-20 keV)", row['FLUX 2-20']).field("error (2-20 keV)", row['ERROR 2-20']) \
            .field("flux (2-4 keV)", row['FLUX 2-4']).field("error (2-4 keV)", row['ERROR 2-4']) \
            .field("flux (4-10 keV)", row['FLUX 4-10']).field("error (4-10 keV)", row['ERROR 4-10']) \
            .field("flux (10-20 keV)", row['FLUX 10-20']).field("error (10-20 keV)", row['ERROR 10-20']) \
            .time(row['UTC TIME'])
            points.append(p)

    if telescope == "fermi":
        for index, row in df.iterrows():
            Point("flux data").tag("source", key) \
            .field("flux (12-50 keV)", row['FLUX 12-50']).field("error (12-50 keV)", row['ERROR 12-50']) \
            .time(row['UTC TIME'])
            points.append(p)

    if telescope == "hardness":
        for index, row in df.iterrows():
            continue
    if telescope == "combined":
        for index, row in df.iterrows():
            continue
    try:
        write_api.write(bucket="flashes_data", org="flashes", record=points,write_precision="ns")
    except Exception as e:
        logging.error(f"Error writing to InfluxDB for {source['integral_name']} from {telescope}: {e}")
        return
    logging.info(f"Successfully wrote {len(points)} points to InfluxDB for {source['integral_name']} from {telescope}.")
    set_last_timestamp(sources_collection=sources_collection, source=source, telescope=telescope, timestamp=df['TIME'].max())

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
        {"integral_name": source['integral_name']},
        {"$set": {f"{telescope}.last_timestamp": timestamp}}
    )
    except Exception as e:
        logging.error(f"Error setting last timestamp for {source['integral_name']} from {telescope}: {e}")
        return
    logging.debug(f"Set last timestamp for {source['integral_name']} from {telescope} to {timestamp}.")
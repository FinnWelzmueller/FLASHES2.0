from influxdb_client import Point
import pandas as pd
import logging
from utils import set_last_timestamp

def write_to_influx(df: pd.DataFrame, write_api, source: dict, telescope: str, sources_collection) -> None:
    """
    Writes data from a dataframe to the InfluxDB. The parameter telescope describes the data source. 
    It can be "swift", "maxi" or "fermi" for lightcurve data or "hardness" or "combined" for additinoal calculations.
    :param df: DataFrame containing the data to be written.
    :param write_api: InfluxDB write API object.
    :param source: Source dictionary from MongoDB.
    :param telescope: "swift", "maxi", "fermi", "hardness" or "combined"
    :param influx_token": Token for InfluxDB authentication.
    :param sources_collection: MongoDB collection object for sources.
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
        sources_collection.update_one({'_id': source['_id']}, {'$set': {f'swift.last_flux': row['FLUX 15-150']}})
        sources_collection.update_one({'_id': source['_id']}, {'$set': {f'swift.last_error': row['ERROR 15-150']}})

    if telescope == "maxi":
        for index, row in df.iterrows():
            p = Point("flux data").tag("source", key) \
            .field("flux (2-20 keV)", row['FLUX 2-20']).field("error (2-20 keV)", row['ERROR 2-20']) \
            .field("flux (2-4 keV)", row['FLUX 2-4']).field("error (2-4 keV)", row['ERROR 2-4']) \
            .field("flux (4-10 keV)", row['FLUX 4-10']).field("error (4-10 keV)", row['ERROR 4-10']) \
            .field("flux (10-20 keV)", row['FLUX 10-20']).field("error (10-20 keV)", row['ERROR 10-20']) \
            .time(row['UTC TIME'])
            points.append(p)
        sources_collection.update_one({'_id': source['_id']}, {'$set': {f'maxi.last_flux': row['FLUX 2-20']}})
        sources_collection.update_one({'_id': source['_id']}, {'$set': {'maxi.last_error': row['ERROR 2-20']}})
    if telescope == "fermi":
        for index, row in df.iterrows():
            p = Point("flux data").tag("source", key) \
            .field("flux (12-50 keV)", row['FLUX 12-50']).field("error (12-50 keV)", row['ERROR 12-50']) \
            .time(row['UTC TIME'])
            points.append(p)
        sources_collection.update_one({'_id': source['_id']}, {'$set': {f'fermi.last_flux': row['FLUX 12-50']}})
        sources_collection.update_one({'_id': source['_id']}, {'$set': {'fermi.last_error': row['ERROR 12-50']}})

    if telescope == "hardness_ratio":
        for index, row in df.iterrows():
            p = Point("flux data").tag("source", key) \
            .field("hardness ratio", row['HARDNESS RATIO']).field("hardness error", row['HARDNESS ERROR']) \
            .time(row['UTC TIME'])
            points.append(p)
    if telescope == "combined":
        for index, row in df.iterrows():
            p = Point("flux data").tag("source", key) \
            .field("combined flux", row['COMBINED FLUX']).field("combined error", row['COMBINED ERROR']) \
            .time(row['UTC TIME'])
            points.append(p)
    try:
        write_api.write(bucket="flashes_data", org="flashes", record=points,write_precision="ns")
    except Exception as e:
        logging.error(f"Error writing to InfluxDB for {source['integral_name']} from {telescope}: {e}")
        return
    logging.info(f"Successfully wrote {len(points)} points to InfluxDB for {source['integral_name']} from {telescope}.")
    if len(points) == 0:
        return
    set_last_timestamp(sources_collection=sources_collection, source=source, telescope=telescope, timestamp=df['TIME'].max())


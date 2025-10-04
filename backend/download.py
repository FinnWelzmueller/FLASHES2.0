import logging
import requests
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
import gzip
import shutil
from astropy.time import Time
from astropy.table import Table
import os
from upload import write_to_influx

def update(sources_collection, write_api, temp_dir = "./_temp") -> None:
    
    logging.info("Starting Update Process.")
    logging.debug(f"Directory for temporary files: {temp_dir}")
    os.makedirs(temp_dir, exist_ok=True)
    errors = []
    for source in sources_collection.find({}):
        logging.info(f"Processing source: {source['integral_name']}")
        try:
            if source.get("swift"):
                logging.info(f"Downloading Swift/BAT data for {source['integral_name']}...")
                swift_df = download_all_data_swift_maxi(source['swift']['data_url'], "swift")
                if swift_df is None:
                    errors.append((source['integral_name'], "swift"))
                    continue
                logging.info(f"Downloaded {len(swift_df)} rows of Swift/BAT data for {source['integral_name']}")
                swift_df = filter_times(swift_df, source, "swift")
                write_to_influx(swift_df, write_api, source, "swift", sources_collection)
            if source.get("maxi"):
                logging.info(f"Downloading MAXI data for {source['integral_name']}...")
                maxi_df = download_all_data_swift_maxi(source['maxi']['data_url'], "maxi")
                if maxi_df is None:
                    errors.append((source['integral_name'], "maxi"))
                    continue
                logging.info(f"Downloaded {len(maxi_df)} rows of MAXI data for {source['integral_name']}")
                maxi_df = filter_times(maxi_df, source, "maxi")
                write_to_influx(maxi_df, write_api, source, "maxi", sources_collection)
            if source.get("fermi"):
                logging.info(f"Downloading Fermi/GBM data for {source['integral_name']}...")
                fermi_df = download_all_data_fermi(source['fermi']['data_url'], temp_dir)
                if fermi_df is None:
                    errors.append((source['integral_name'], "fermi"))
                    continue
                logging.info(f"Downloaded {len(fermi_df)} rows of Fermi/GBM data for {source['integral_name']}")
                fermi_df = filter_times(fermi_df, source, "fermi")
                write_to_influx(fermi_df, write_api, source, "fermi", sources_collection)

        except Exception as e:
                  logging.error(f"Error processing source {source['integral_name']}: {e}")
    shutil.rmtree(temp_dir)
    logging.info("Update Process Complete. The following errors occured:")
    for error in errors:
         logging.error(f"Source: {error[0]}, Telescope: {error[1]}")


def download_all_data_swift_maxi(url:str, telescope:str) -> pd.DataFrame | None:
    """
    Downloads and processes data for Swift/BAT and MAXI. The complete dataset is returned as a pandas DataFrame.
    For Fermi/GBM data, use download_all_data_fermi.
    :param url: URL to download the data from (from the mongoDB)
    :param telescope: "swift" or "maxi"
    :return: DataFrame with the complete dataset or None if download failed.
    """
    logging.debug(f"Downloading data from {url} for {telescope}...")
    if telescope not in ["swift", "maxi", "fermi"]:
        logging.error(f"Unsupported telescope: {telescope}. Skipping downlaod")
        return None
    response = requests.get(url)
    if response.status_code == 200: # response ok
            data = StringIO(response.text)
            df = pd.read_csv(data, sep=r"\s+", comment="#", header=None)
            if telescope == "swift":
                df.columns = [
                "TIME", "FLUX 15-150", "ERROR 15-150", "YEAR", "DAY", "STAT_ERR", "SYS_ERR", 
                "DATA_FLAG", "TIMEDEL_EXPO", "TIMEDEL_CODED", "TIMEDEL_DITHERED"
                ]
            if telescope == "maxi":
                df.columns = [
                "TIME", "FLUX 2-20", "ERROR 2-20", "FLUX 2-4", "ERROR 2-4", "FLUX 4-10", "ERROR 4-10", "FLUX 10-20", "ERROR 10-20"
                ]
            df['UTC TIME'] = get_utc_time(df['TIME'])
            logging.debug(f"Download complete from {url} for {telescope}")
            return df       
    else:
            logging.error(f"Failed to download data from {url}. Status code: {response.status_code}. Skipping download.")
            return None

def download_all_data_fermi(url:str, temp_dir) -> pd.DataFrame | None:
    """
    Downloads and processes data for Fermi/GBM. The complete dataset is returned as a pandas DataFrame.
    For Swift/BAT and MAXI data, use download_all_data_swift_maxi.
    :param url: URL to download the data from (from the mongoDB)
    :param temp_dir: Directory to store temporary files.
    :return: DataFrame with the complete dataset or None if download failed.
    """
    filename = url.split("/")[-1]  
    compressed_path = os.path.join(temp_dir, filename)
    extracted_path = compressed_path.replace(".gz", "")
    response = requests.get(url)
    if response.status_code == 200:
        with open(compressed_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logging.debug(f"Downloaded compressed file to {compressed_path}")
        with gzip.open(compressed_path, "rb") as f_in:
            with open(extracted_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        logging.debug(f"Extracted file to {extracted_path}")
        logging.debug(f"Reading data from {extracted_path}")
        os.remove(compressed_path)
        dat = Table.read(extracted_path, format='fits', hdu=2)
        names = [name for name in dat.colnames if len(dat[name].shape) <= 1]
        df = dat[names].to_pandas()
        df = df[['PSRTIME', 'AMPLITUDE', 'AMPLITUDE_ERR']]
        df['UTC TIME'] = get_utc_time(df['PSRTIME'])
        df.columns = ['TIME', 'FLUX 12-50', 'ERROR 12-50', 'UTC TIME']
        logging.debug(f"Download complete from {url} for fermi")
        return df
    else:
        logging.error(f"Failed to download data from {url}. Status code: {response.status_code}. Skipping download.")
        return None

def get_utc_time(mjd: pd.Series) -> pd.Series:
    """
    Converts MJD to UTC time at midnight.
    :param mjd: Series of MJD times.
    :return: Series of UTC times at midnight.
    """
    dt_series = pd.to_datetime(Time(mjd, format="mjd").to_datetime())
    dt_series = pd.Series(dt_series).dt.floor("D")  # set to midnight
    return dt_series


def filter_times(df: pd.DataFrame, source, telescope) -> pd.DataFrame | None:
    """
    Recalls the last timestamp from the mongoDB and filters the dataframe to only include data after that timestamp.
    :param df: DataFrame to be filtered.
    :param source: Source dictionary from MongoDB.
    :return: Filtered DataFrame or None if telescope is unsupported.
    """
    if telescope not in ["swift", "maxi", "fermi", "hardness", "combined"]:
        logging.error(f"Unsupported telescope: {telescope}. Skipping time filtering")
        return None
    last_timestamp = source[telescope]['last_timestamp']
    logging.debug(f"Filtering data for {source['integral_name']} from {telescope}. Last timestamp is {last_timestamp}...")
    return df[df['TIME'] > last_timestamp]
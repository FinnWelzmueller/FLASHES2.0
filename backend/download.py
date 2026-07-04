import logging
import requests
from io import StringIO
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import gzip
import shutil
from datetime import datetime, timedelta
from astropy.time import Time
from astropy.table import Table
import os
from upload import write_to_influx
from utils import get_utc_time

def update(sources_collection, write_api, temp_dir = "./_temp") -> None:
    """
    Main update function. Downloads data for all sources in the mongoDB, processes it and uploads it to InfluxDB. Calculates hardness ratio and combined flux if both Swift and MAXI data is available.
    :param sources_collection: MongoDB collection object for sources.
    :param write_api: InfluxDB write API object.
    :param temp_dir: Directory to store temporary files.
    """
    logging.info("Starting Update Process.")
    logging.debug(f"Directory for temporary files: {temp_dir}")
    os.makedirs(temp_dir, exist_ok=True)
    errors = []
    i = 1
    all = sources_collection.count_documents({})
    for source in sources_collection.find({}):
        logging.info(f"Processing source: {source['integral_name']} ({i}/{all})")
        i+=1
        try:
            if source.get("swift"):
                logging.info(f"Downloading Swift/BAT data for {source['integral_name']}...")
                swift_df = download_all_data_swift_maxi(source['swift']['data_url'], "swift")
                if swift_df is None:
                    errors.append((source['integral_name'], "swift"))
                    continue
                logging.info(f"Downloaded {len(swift_df)} rows of Swift/BAT data for {source['integral_name']}")
                swift_df_new= filter_times(swift_df, source, "swift")
                write_to_influx(swift_df_new, write_api, source, "swift", sources_collection)
            if source.get("maxi"):
                logging.info(f"Downloading MAXI data for {source['integral_name']}...")
                maxi_df = download_all_data_swift_maxi(source['maxi']['data_url'], "maxi")
                if maxi_df is None:
                    errors.append((source['integral_name'], "maxi"))
                    continue
                logging.info(f"Downloaded {len(maxi_df)} rows of MAXI data for {source['integral_name']}")
                maxi_df_new = filter_times(maxi_df, source, "maxi")
                write_to_influx(maxi_df_new, write_api, source, "maxi", sources_collection)
            if source.get("fermi"):
                logging.info(f"Downloading Fermi/GBM data for {source['integral_name']}...")
                fermi_df = download_all_data_fermi(source['fermi']['data_url'], temp_dir)
                if fermi_df is None:
                    errors.append((source['integral_name'], "fermi"))
                    continue
                logging.info(f"Downloaded {len(fermi_df)} rows of Fermi/GBM data for {source['integral_name']}")
                fermi_df_new = filter_times(fermi_df, source, "fermi")
                write_to_influx(fermi_df_new, write_api, source, "fermi", sources_collection)
            if source.get("swift") and source.get("maxi"):
                logging.info(f"Found Swift and MAXI data for {source['integral_name']}, calculating hardness ratio and combined flux...")
                hardness_combined_df = calculate_hardness_and_combined_flux(swift_df, maxi_df)
                write_to_influx(hardness_combined_df[['UTC TIME', 'HARDNESS RATIO', 'HARDNESS ERROR', 'TIME']], write_api, source, "hardness_ratio", sources_collection)
                write_to_influx(hardness_combined_df[['UTC TIME', 'COMBINED FLUX', 'COMBINED ERROR', 'TIME']], write_api, source, "combined", sources_collection)
        except Exception as e:
                  logging.error(f"Error processing source {source['integral_name']}: {e}")
    shutil.rmtree(temp_dir)
    logging.info("----- Update Process Complete. The following errors occured -----")
    for error in errors:
         logging.info(f" - Source: {error[0]}, Telescope: {error[1]}")

def calculate_hardness_and_combined_flux(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the hardness ratio and combined flux from Swift/BAT and MAXI data.
    The hardness ratio is defined as the ratio of the Swift/BAT flux (15-50 keV) to the MAXI flux (2-20 keV).
    The combined flux is defined as the sum of the Swift/BAT flux (15-50 keV) and the MAXI flux (2-20 keV).
    The errors are calculated using standard error propagation.
    :param df1: DataFrame containing Swift/BAT data. Must contain columns 'UTC TIME', 'FLUX 15-50', 'ERROR 15-50'.
    :param df2: DataFrame containing MAXI data. Must contain columns 'UTC TIME', 'FLUX 2-20', 'ERROR 2-20'.
    :return: DataFrame containing the hardness ratio and combined flux with columns 'UTC TIME', 'HARDNESS RATIO', 'HARDNESS ERROR', 'COMBINED FLUX', 'COMBINED ERROR'.
    """
    df_out = pd.DataFrame(columns=['UTC TIME', 'HARDNESS RATIO', 'HARDNESS ERROR', 'COMBINED FLUX', 'COMBINED ERROR'])
    merged = pd.merge(df1, df2, left_on="UTC TIME",right_on="UTC TIME", how='outer')
    for idx, row in merged.iterrows():
        if not (pd.isna(row['FLUX 2-20']) or pd.isna(row['FLUX 15-50'])):
            df_out.loc[len(df_out)] = {
                'UTC TIME': row['UTC TIME'],
                'HARDNESS RATIO': row['FLUX 15-50'] / row['FLUX 2-20'],
                'HARDNESS ERROR': (row['FLUX 15-50'] / row['FLUX 2-20']) * ((row['ERROR 15-50'] / row['FLUX 15-50'])**2 + (row['ERROR 2-20'] / row['FLUX 2-20'])**2)**0.5,
                'COMBINED FLUX': row['FLUX 15-50'] + row['FLUX 2-20'],
                'COMBINED ERROR': (row['ERROR 15-50']**2 + row['ERROR 2-20']**2)**0.5
            }
    df_out['TIME'] = df_out['UTC TIME'].apply(lambda x: int(Time(x).mjd))
    return df_out 


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
                "TIME", "FLUX 15-50", "ERROR 15-50", "YEAR", "DAY", "STAT_ERR", "SYS_ERR", 
                "DATA_FLAG", "TIMEDEL_EXPO", "TIMEDEL_CODED", "TIMEDEL_DITHERED"
                ]
            if telescope == "maxi":
                df.columns = [
                "TIME", "FLUX 2-20", "ERROR 2-20", "FLUX 2-4", "ERROR 2-4", "FLUX 4-10", "ERROR 4-10", "FLUX 10-20", "ERROR 10-20"
                ]
            df['UTC TIME'] = get_utc_time(df['TIME'])
            df["TIME"] = df["TIME"].astype(int)
            logging.debug(f"Maximum Timestamp in MJD:{df['TIME'].max()}")
            logging.debug(f"Today's Timestamp in MJD: {int(Time.now().mjd)}")
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
        df["TIME"] = df["TIME"].astype(int)
        logging.debug(f"Maximum Timestamp in MJD: {df['TIME'].max()}")
        logging.debug(f"Today's Timestamp in MJD: {int(Time.now().mjd)}")
        logging.debug(f"Download complete from {url} for fermi")
        return df
    else:
        logging.error(f"Failed to download data from {url}. Status code: {response.status_code}. Skipping download.")
        return None




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
    safety_window_days = 1
    cutoff = last_timestamp - safety_window_days
    logging.debug(
        f"Filtering data for {source['integral_name']} from {telescope}. "
        f"Last timestamp is {last_timestamp}, safety window is {safety_window_days} days..."
    )
    return df[df['TIME'] > cutoff]


def calculate_trend_relevance(query_api, influx_key: str, scale: float = 3, plot: bool = False, window_size: int = 7, limit: int = 5) -> float:
    """
    Calculates the trend relevance for a certain timeseries. The fluxes of the timeseries are converted into a flux gradient, from which a histogram is calculated. Then, a Gaussian is fitted to the histogram and the mean and standard deviation are calculated. The trend relevance is then calculated as the number of standard deviations the mean flux gradient is above the mean of the fitted Gaussian. The result is clipped to a maximum of 1 and a minimum of 0. To account for certain unphysical changes, the current flux gradient is calculated as a mean over the last days. The window size for this mean can me adjusted by changing the parameter window_size. To account for large gaps in the data, a minimum number of data points can be set with the parameter limit. If the number of data points in the last window_size days is below this limit, None is returned.
    """
    def gauss(x, A, mu, sigma):
        return A * np.exp(-(x - mu)**2 / (2 * sigma**2))

    if "swift" in influx_key:
        channel = "flux (15-50 keV)"
    elif "fermi" in influx_key:
        channel = "flux (12-50 keV)"
    elif "maxi" in influx_key:
        channel = "flux (2-20 keV)"
    else:
        raise ValueError("Invalid influx_key. No adequate timeseries found.")

    query_flux = f"""
            from(bucket: "flashes_data")
                |> range(start: 0)
                |> filter(fn: (r) => r._measurement == "flux data")
                |> filter(fn: (r) => r.source == "{influx_key}")
            |> keep(columns: ["_time", "_field", "_value"])
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
            |> drop(columns: ["_start","_stop"])
            """
    data = query_api.query_data_frame(org="flashes", query=query_flux)
    time_deltas = data["_time"].diff().dt.total_seconds() / 86400
    data["flux gradient"] = data[channel].diff() / time_deltas
    data["_time"] = data["_time"].dt.tz_convert(None)
    recent_data = data[data["_time"] > (datetime.now() - timedelta(days=window_size))]
    if len(recent_data) >= limit:
        valid_gradient = data["flux gradient"].replace([np.inf, -np.inf], np.nan).dropna()
        counts, bin_edges = np.histogram(valid_gradient, bins=np.linspace(data["flux gradient"].min(), data["flux gradient"].max(), 100))
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        recent_data_mean = recent_data["flux gradient"].dropna().mean()
        popt, pcov = curve_fit(gauss, bin_centers, counts)
        mu, sigma = popt[1], popt[2]
        sigma = np.abs(sigma)
        if plot:
            plt.plot(bin_centers, counts, 'bo')
            plt.plot(bin_centers, gauss(bin_centers, *popt), 'r-')
            plt.xlabel(r'$\Delta\Phi$ / day')
            plt.ylabel('Counts')
            plt.title(f"mu={mu:.2f}, sigma={sigma:.6f}")
            plt.show()
        return np.clip(1/scale * (recent_data_mean - mu) / sigma, 0, 1)
    else:
        return None
    
def get_flux_relevance(query_api, current_snr:float, influx_key: str, scale: float = 3, plot: bool = False) -> float:
    """
        Calculates the Flux Relevance for a certain timeseries. The fluxes and errors of the timeseries are converted into a SNR, from which a histogram is calculated. Then, the mean and standard deviation of a Gaussian is fitted to the histogram and the flux relevance is calculated as the number of standard deviations the flux is above the mean. The result is clipped to a maximum of 3 and a minimum of 0 and scaled to be between 0 and 1.
        :param query_api: The InfluxDB query API.
        :param current_snr: The current SNR value for which the relevance should be calculated.
        :param influx_key: The key of the timeseries in the InfluxDB.
        :param scale: The maximum relevance value. Default is 3, which corresponds to 3 standard deviations above the mean, giving a flux relevance of 1.
        :param plot: Whether to plot the histogram and the fitted Gaussian. Default is False.
        :return: The flux relevance, a value between 0 and 1.
    """
    def gauss(x, A, mu, sigma):
        return A * np.exp(-(x - mu)**2 / (2 * sigma**2))
    
    if "swift" in influx_key:
        channel = "flux (15-50 keV)"
    elif "fermi" in influx_key:
        channel = "flux (12-50 keV)"
    elif "maxi" in influx_key:
        channel = "flux (2-20 keV)"
    else:
        raise ValueError("Invalid influx_key. No adequate timeseries found.")
    
    query_flux = f"""
        from(bucket: "flashes_data")
            |> range(start: 0)
            |> filter(fn: (r) => r._measurement == "flux data")
            |> filter(fn: (r) => r.source == "{influx_key}")
        |> keep(columns: ["_time", "_field", "_value"])
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"])
        |> drop(columns: ["_start","_stop"])
        """
    data = query_api.query_data_frame(org="flashes", query=query_flux)
    snr = np.divide(data[channel], data[f"{channel.replace('flux', 'error')}"])
    counts, bin_edges = np.histogram(snr, bins=np.linspace(snr.min(), snr.max(), 100))
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    popt, pcov = curve_fit(gauss, bin_centers, counts)
    mu, sigma = popt[1], popt[2]

    sigma = np.abs(sigma)  # Ensure sigma is positive
    print(f"mu: {mu}, sigma: {sigma}")
    if plot:
        plt.plot(bin_centers, counts, 'bo')
        plt.plot(bin_centers, gauss(bin_centers, *popt), 'r-')
        plt.xlabel('SNR')
        plt.ylabel('Counts')
        plt.title(f"mu={mu:.2f}, sigma={sigma:.2f}")
        plt.show()
    return np.clip(1/scale * (current_snr - mu) / sigma, 0, 1)

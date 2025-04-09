import requests
from io import StringIO
import pandas as pd
from backend.utils import get_utc_time
import os
import gzip
import shutil
from astropy.table import Table

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

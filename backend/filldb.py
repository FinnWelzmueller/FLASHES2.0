import pandas as pd
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

maxi_url = "http://maxi.riken.jp/star_data/"
swift_url = "https://swift.gsfc.nasa.gov/results/transients/"
fermi_url = "https://gammaray.nsstc.nasa.gov/gbm/science/pulsars/lightcurves/"

client = MongoClient(f"mongodb://admin:{os.getenv("MONGO_INITDB_ROOT_PASSWORD")}@localhost:27017/")    # MongoDB connection setup
db = client['flashes']
sources_collection = db['sources']

df = pd.read_csv(os.path.join(os.getcwd() ,"master_table.txt"))
df = df[~((df['Swift Name'] == 'noSwift') & (df['Maxi Name'] == 'noMaxi') & (df['Fermi Name'] == 'noFermi'))] # drop empty elements

for _, row in df.iterrows():
    source_data = { # Base Information
        "_id": row['Integral Name'].replace(" ", "").lower(), # -> _id from Name? -> I don't want to have twice the same source in the db anyway
        "integral_name": row['Integral Name'],  
        "coord_ra": row['Ra Obj'],
        "coord_dec": row['Dec Obj'],
        "labels_constant" : [],
        "labels_dynamic": []
    }

    
    if row['Maxi Name'] != "noMaxi": # If Maxi Data is available -> .dat file available
        source_data['maxi'] = {
            "data_url": maxi_url + row['Maxi ID'] + "/" + row['Maxi ID'] + "_g_lc_1day_all.dat",
            "influx_key": "maxi_" + row['Integral Name'].replace(" " ,"").lower(),
            "last_timestamp": 0}
    else:
        source_data['maxi'] = None

    if row['Swift Name'] != "noSwift":  # if Swift Data is available -> .txt file available
        str = swift_url
        if row['Swift Weak'] == "yes":
            str += "weak/"
        source_data['swift'] = {
            "data_url": str + row['Swift ID'] + ".lc.txt",
            "influx_key": "swift_" + row['Integral Name'].replace(" " ,"").lower(),
            "last_timestamp": 0}
    else:
        source_data['swift'] = None
    
    if row['Fermi Name'] != "noFermi":  # if Fermi Data is available -> .fits file available
        source_data['fermi'] = {
            "data_url": fermi_url + row['Fermi ID'] + "_old.fits.gz", 
            "influx_key": "fermi_" + row['Integral Name'].replace(" " ,"").lower(),
            "last_timestamp": 0}
    else:
        source_data['fermi'] = None

    # preparer hardness if maxi and swift is not None -> need combined flux and hardness ratio

    if source_data['maxi'] is not None and source_data['swift'] is not None:
        source_data['hardness_ratio'] = {
            "influx_key": "hardness_" + row['Integral Name'].replace(" " ,"").lower(),
            "last_timestamp": 0
        }
        source_data['combined'] = {
            "influx_key": "combined_" + row['Integral Name'].replace(" " ,"").lower(),
            "last_timestamp": 0
        }
    else:
        source_data['hardness_ratio'] = None
        source_data['combined'] = None
    sources_collection.insert_one(source_data)

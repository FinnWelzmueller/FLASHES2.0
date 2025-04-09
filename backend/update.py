from backend.config import sources_collection
from backend.download import download_fermi, download_to_dataframe
from backend.utils import find_last_timestamp, filter_new_data
from backend.influx import write_flux_to_influx, calculate_hardness
from pandas import DataFrame

def update():
    for source in sources_collection.find({}):
        new_swift_df = DataFrame()
        new_maxi_df = DataFrame()

        if source.get("swift"):
            print(f"Updating {source["integral_name"]} (Swift data)...")
            swift_df = download_to_dataframe(source["swift"]["data_url"], "swift")
            if swift_df is not None:
                last_ts_swift = find_last_timestamp(source["integral_name"], source["swift"]["influx_key"])
                new_swift_df = filter_new_data(swift_df, last_ts_swift) if last_ts_swift else swift_df
                write_flux_to_influx(source, "swift", new_swift_df)
                print("\t verifying new timestamp from influx...")
                new_last_ts_swift = find_last_timestamp(source["integral_name"], source["swift"]["influx_key"])
            else:
                print(f"Skipping {source['integral_name']} (Swift): download failed.")
            print(f"Updating {source["integral_name"]} (Swift data) done.")
            

        if source.get("maxi"):
            print(f"Updating {source["integral_name"]} (MAXI data)...")
            maxi_df = download_to_dataframe(source["maxi"]["data_url"], "maxi")
            if maxi_df is not None:
                last_ts_maxi = find_last_timestamp(source["integral_name"], source["maxi"]["influx_key"])
                new_maxi_df = filter_new_data(maxi_df, last_ts_maxi) if last_ts_maxi else maxi_df
                write_flux_to_influx(source, "maxi", new_maxi_df)
                print("\t verifying new timestamp from influx...")
                new_last_ts_maxi = find_last_timestamp(source["integral_name"], source["maxi"]["influx_key"])
            else:
                print(f"Skipping {source['integral_name']} (MAXI): download failed.")
            print(f"Updating {source["integral_name"]} (MAXI data) done.")
            
        if source.get("fermi"):
            print(f"Updating {source["integral_name"]} (Fermi data)...")
            fermi_df = download_fermi(source["fermi"]["data_url"], temp_dir="./_temp")
            if fermi_df is not None:
                last_ts_fermi = find_last_timestamp(source["integral_name"], source["fermi"]["influx_key"])
                new_fermi_df = filter_new_data(fermi_df, last_ts_fermi) if last_ts_fermi else fermi_df
                write_flux_to_influx(source, "fermi", new_fermi_df)
                print("\t verifying new timestamp from influx...")
                new_last_ts_fermi = find_last_timestamp(source["integral_name"], source["fermi"]["influx_key"])
            else:
                print(f"Skipping {source['integral_name']} (Fermi data): no data available.")
           
            print(f"Updating {source["integral_name"]} (Fermi data) done.")
        
        #hardness calculation
        if (source.get("swift") and source.get("maxi") and not new_swift_df.empty and not new_maxi_df.empty):
            print(f"Updating {source["integral_name"]} (Hardness data)...")
            calculate_hardness(source)
            print(f"Updating {source["integral_name"]} (Hardness data) done.")

def initialize():
    # iterate over sources
    for source in sources_collection.find({}):
        if source.get("swift"):
            swift_df = download_to_dataframe(source["swift"]["data_url"], "swift")
            write_flux_to_influx(source, "swift", swift_df)
        if source.get("maxi"):
            maxi_df = download_to_dataframe(source["maxi"]["data_url"], "maxi")
            write_flux_to_influx(source, "maxi", maxi_df)
        if source.get("fermi"):
            fermi_df = download_fermi(source["fermi"]["data_url"], temp_dir="./_temp")
            write_flux_to_influx(source, "fermi", fermi_df)
    for source in sources_collection.find({"swift": {"$ne": None},"maxi": {"$ne": None}}):
        calculate_hardness(source)

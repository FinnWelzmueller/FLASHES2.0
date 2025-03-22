from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv
import os
import logging


def main() -> None:
    dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(dotenv_path)

    mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
    client = MongoClient(f"mongodb://admin:{mongo_password}@localhost:27017/")
    db = client["flashes"]
    sources_collection = db["sources"]

    for ele in sources_collection.find():
        print(ele)


if __name__ == "__main__":
    main()
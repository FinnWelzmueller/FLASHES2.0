from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv("./.env")
mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
client = MongoClient(f"mongodb://admin:{mongo_password}@172.18.0.1:27017/")
db = client["flashes"]
sources_collection = db["sources"]


app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "backend is running!"}

@app.get("/sources/{name}")
def get_source(name: str):
    source = sources_collection.find_one({"_id": name})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source
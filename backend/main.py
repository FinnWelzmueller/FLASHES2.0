from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from backend.update import update
import os
from datetime import datetime
from contextlib import asynccontextmanager

# init
load_dotenv("./.env")
mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
client = MongoClient(f"mongodb://admin:{mongo_password}@172.18.0.1:27017/")
db = client["flashes"]
sources_collection = db["sources"]
scheduler = BackgroundScheduler()


scheduler.start()

def scheduled_update():
    print(f"[{datetime.datetime.now()}] Scheduled update triggered.")
    update()

scheduler.add_job(update, CronTrigger(hour=2, minute=0))  # triggers at 2:00am

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting scheduler...")
    scheduler.start()
    yield
    print("Shutting down scheduler...")
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)


# endpoints
@app.get("/")
def read_root():
    return {"message": "backend is running!"}

@app.get("/sources")
def get_all_sources():
    sources = list(sources_collection.find({}))
    if not sources:
        raise HTTPException(status_code=404, detail="No sources found")

@app.get("/sources/{name}")
def get_source_by_name(name: str):
    source = sources_collection.find_one({"_id": name})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source

@app.get("/sources/{name}/{telescope}")
def get_flux_by_name_and_telescope(name: str, telescope: str):
    source = sources_collection.find_one({"_id": name})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    if not source.get(telescope):
        raise HTTPException(status_code=404, detail="Unsupported telescope for source" )
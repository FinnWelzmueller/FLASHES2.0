from fastapi import FastAPI
from pymongo import MongoClient
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from update import update
import os
from datetime import datetime
from contextlib import asynccontextmanager
from routes import sources

# init
load_dotenv("./.env")
mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
client = MongoClient(f"mongodb://admin:{mongo_password}@172.18.0.1:27017/")
db = client['flashes']
sources_collection = db['sources']
scheduler = BackgroundScheduler()

def scheduled_update():
    print(f"[{datetime.now()}] Scheduled update triggered.")
    update()

scheduler.add_job(scheduled_update, CronTrigger(hour=2, minute=0))  # triggers at 2:00am

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting scheduler...")
    scheduler.start()
    yield
    print("Shutting down scheduler...")
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)
app.include_router(sources.router)

# endpoints for overview
@app.get("/")
def read_root():
    return {"message": "backend is running!"}


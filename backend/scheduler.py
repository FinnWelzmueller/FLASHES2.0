from apscheduler.schedulers.background import BackgroundScheduler
from download import update
import logging

def start_scheduler(args):
    """
    Starts the APScheduler to run the update function daily at 02:00 AM.
    """
    logging.info("Starting scheduler...")
    scheduler = BackgroundScheduler()
    logging.info("Scheduler started. Adding update job to run daily at 02:00 AM...")
    scheduler.add_job(update, 'cron', hour=2, minute=0, second=0, args=args)

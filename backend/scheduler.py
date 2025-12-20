from apscheduler.schedulers.background import BackgroundScheduler
from download import update
import logging

def start_scheduler(args):
    """
    Starts the APScheduler to run the update function daily at 02:00 AM.
    """
    logging.info("Starting scheduler...")

    scheduler = BackgroundScheduler(timezone="UTC")  
    scheduler.start()

    def update_job():
        try:
            logging.info("Scheduler job 'update' started.")
            update(*args)
            logging.info("Scheduler job 'update' finished.")
        except Exception:
            logging.exception("Scheduler job 'update' crashed.")

    logging.info("Scheduler started. Adding update job to run daily at 02:00 AM...")

    scheduler.add_job(
        update_job,
        trigger="cron",
        hour=2,
        minute=0,
        second=0,
        id="update",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=6 * 60 * 60,  # 6h
    )

# main.py
from app import create_app
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app.storage import scheduled_store

app = create_app()

scheduler = BackgroundScheduler()
trigger = CronTrigger(minute='0,15,30,45')
scheduler.add_job(scheduled_store, trigger=trigger)
scheduler.start()

## THIS SHOULD NOT BE CHANGED - THIS IS WHAT DOCKER RUNS!
from apscheduler.schedulers.blocking import BlockingScheduler
from scraper_schedule import run_scraper_schedule
run_scraper_schedule(scheduler_class=BlockingScheduler)

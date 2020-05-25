from classes.news import *
from classes.appledaily import *
from classes.likes import *
from classes.individual_news import *
import datetime 
import warnings
import os
import requests
import urllib
import traceback


def send_to_telegram(status):
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    JOB_NAME = os.getenv("JOB_NAME")
    text = "Job %s is %s" % (JOB_NAME, status)
    qs = urllib.parse.urlencode({'chat_id': os.getenv("TELEGRAM_CHANNEL_ID"), 'text': text})
    url = "https://api.telegram.org/bot%s/sendMessage?%s" % (os.getenv("TELEGRAM_TOKEN"), qs)   
    print(url)
    print(requests.post(url).json())




def fetch_apple_daily(rundate):
    appledaily_articles = fetch_news_from_appledaily(rundate.replace("-", ""))
    print("Fetched %d articles" % (len(appledaily_articles)))
    print(upsert_news(appledaily_articles))
    print("Finding related articles")
    print(update_individual_news(appledaily_articles))
    before = datetime.datetime.strptime(rundate,"%Y-%m-%d") - datetime.timedelta(weeks = 2)
    before = before.strftime("%Y-%m-%d")
    print("Updating like counts from %s" % (before))
    update_news_like_count(before)   


warnings.filterwarnings("ignore")
completed = False
try:
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    print(today)
    fetch_apple_daily(today)
    completed = True
except Exception as e:
    traceback.print_exc()
send_to_telegram("completed" if completed else "error")


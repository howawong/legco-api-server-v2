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
from datetime import timedelta, date


def get_memory():
    """ Look up the memory usage, return in MB. """
    proc_file = '/proc/{}/status'.format(os.getpid())
    scales = {'KB': 1024.0, 'MB': 1024.0 * 1024.0}
    with open(proc_file, 'rU') as f:
        for line in f:
            if 'VmHWM:' in line:
                fields = line.split()
                size = int(fields[1])
                scale = fields[2].upper()
                return size*scales[scale]/scales['MB']
    return 0.0

def print_memory():
    print("Peak: %f MB" % (get_memory()))



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
    result = update_individual_news(appledaily_articles)
    print(result)
    before = datetime.datetime.strptime(rundate,"%Y-%m-%d") - datetime.timedelta(weeks = 2)
    before = before.strftime("%Y-%m-%d")
    print("Updating like counts from %s" % (before))
    update_news_like_count(before)   


warnings.filterwarnings("ignore")
completed = False
message = ""


def daterange(start_date, end_date):
    if start_date is None:
        yield end_date
    else:
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)

backfill = False

start_date = None
end_date = datetime.datetime.now().strftime("%Y-%m-%d")

if backfill:
    start_date = date(2020, 6, 1)
    end_date = date(2020, 6, 18)

for single_date in daterange(start_date, end_date):
    print(single_date)
    try:
        today = single_date
        print(today)
        print_memory()
        fetch_apple_daily(today)
        print_memory()
        completed = True
    except Exception as e:
        message = str(e)
        traceback.print_exc()
    send_to_telegram("completed" if completed else "error %s" % (message))


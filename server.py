from flask import Flask, jsonify
import requests
import click
import datetime
from .jobs import *
import os
from dotenv import find_dotenv, load_dotenv


dotenv_path = os.getenv('ENV_FILE', os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv(dotenv_path)

app = Flask(__name__)


@app.cli.command("fetch-appledaily")
@click.argument("rundate")
def fetch_apple_daily(rundate):
    appledaily_articles = fetch_news_from_appledaily(rundate.replace("-", ""))
    print("Fetched %d articles" % (len(appledaily_articles)))
    upsert_news(appledaily_articles)
    print("Finding related articles")
    update_individual_news(appledaily_articles)
    before = datetime.datetime.strptime(rundate,"%Y-%m-%d") - datetime.timedelta(weeks = 2)
    before = before.strftime("%Y-%m-%d")
    print("Updating like counts from %s" % (before))
    update_news_like_count(before)   


@app.route("/budget/meeting/<int:year>/")
def budget_meeting(year):
    query = \
"""
query MyQuery {
  legco_BudgetQMeeting(where: {year: {_eq: %d}}) {
    year
    bureau: bureau_name {
      bureau
      name_ch
      name_en
      id
    }
    id
  }
}
""" % (year)
    r = run_query(query)
    return jsonify(r)


@app.route("/legco/hot_news/")
def news():
    query = \
"""
query MyQuery {
  legco_IndividualNews(where: {News: {date: {_gte: "2019-12-01"}}}, order_by: {news: asc}) {
    News {
      date
      image
      link
      title
      key
    }
    engagement {
      engagement
    }
    Individual {
      id
      name_ch
      image
    }
  }
}
"""
    r = run_query(query)
    news = {}
    engagement = {}
    for j in r:
        orig_news = j["News"]
        key = orig_news["key"]
        engagement = j["engagement"]
        if key not in news:
            news[key] = {}
            news[key]["individuals"] = []
        news[key].update(engagement)   
        news[key].update(orig_news)   
    
    for j in r:
        orig_news = j["News"]
        key = orig_news["key"]
        individual = j["Individual"]
        news[key]["individuals"].append(individual)       
    output = sorted([v for k, v in news.items()], key=lambda x: x["engagement"], reverse=True)[0:10]
    return jsonify(output)


@app.route("/legco/member_news/<int:member>/")
def member_news(member):
    query = \
"""
query MyQuery {
  legco_IndividualNews(where: {Individual: {id: {_eq: %d}}}, order_by: {News: {date: desc}}, limit: 20) {
    News {
      date
      image
      link
      title
      key
    }
  }
}
""" % (member)
    r = run_query(query)
    return jsonify(r)


@app.route("/legco/member_news_by_name/<string:name_ch>/")
def member_news_by_name(name_ch):
    query = \
"""
query MyQuery {
  legco_IndividualNews(where: {Individual: {name_ch: {_eq: "%s"}}}, order_by: {News: {date: desc}}, limit: 20) {
    News {
      date
      image
      link
      title
      key
    }
  }
}
""" % (name_ch)
    r = run_query(query)
    return jsonify(r)

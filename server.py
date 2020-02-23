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
    print(upsert_news(appledaily_articles))
    print("Finding related articles")
    print(update_individual_news(appledaily_articles))
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
    r = run_query(query)["data"]["legco_BudgetQMeeting"]
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
    member: Individual {
      id
      name_ch
      image
    }
  }
}
"""
    r = run_query(query)["data"]["legco_IndividualNews"]
    news = {}
    engagement = {}
    for j in r:
        print(j)
        orig_news = j["News"]
        key = orig_news["key"]
        engagement = j["engagement"]
        if key not in news:
            news[key] = {}
            news[key]["members"] = []
        news[key].update(engagement)   
        news[key].update(orig_news)   
    
    for j in r:
        orig_news = j["News"]
        key = orig_news["key"]
        individual = j["member"]
        news[key]["members"].append(individual)       
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
    r = [r["News"] for r in run_query(query)["data"]["legco_IndividualNews"]]
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
    r = [r["News"] for r in run_query(query)["data"]["legco_IndividualNews"]]
    return jsonify(r)

@app.route("/legco/member/<int:member_id>/")
def member_statistics(member_id):
    year = 2016
    start_date = "2018-05-01"
    query = \
"""
query MyQuery {
  legco_IndividualVote(where: {individual: {_eq: %d}, Meeting: {date: {_gte: "%s"}}}, order_by: {Meeting: {date: asc}}) {
    Meeting {
      id
      date
      meeting_type
    }
    vote_number
    result
  }
  legco_Individual(where: {id: {_eq: %d}}) {
    image
    name_ch
    name_en
    Party {
      name_ch
      name_en
      name_short_ch
      name_short_en
      id
      image
    }
  }
  legco_CouncilMembers(where: {Individual: {id: {_eq: %d}}, Council: {start_year: {_eq: %d}}}) {
    disqualified
    id
    member
    membership_type
    Council {
      start_year
    }
    CouncilMembershipType {
      category
      id
      sub_category
    }
  }
}
""" % (member_id, start_date, member_id, member_id, year)
    data = run_query(query)["data"]
    votes = data["legco_IndividualVote"]
    votes = [{
        "date": vote["Meeting"]["date"][0:-3] + "-01 00:00:00",
        "result": vote["result"],
        "vote_number": vote["vote_number"],
        "meeting": vote["Meeting"]["id"]

    } for vote in votes]
    summary = {}
    for vote in votes:
        d = vote["date"]
        result = vote["result"]
        if d not in summary:
            summary[d] = {}
        summary[d][result] = summary[d].get(result, 0) + 1
    vote_rate = [
        {d:{
            'vote_count': stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0),
            'no_vote_count': stats.get('ABSTAIN', 0) + stats.get('ABSENT', 0)
        }}
     for d, stats in summary.items()]

    vote_rate = [
        {d:{
            'vote_count': stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0),
            'no_vote_count': stats.get('ABSTAIN', 0) + stats.get('ABSENT', 0)
        }}
     for d, stats in summary.items()]

    attendance_rate = [
        {d:{
            'present_count': stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0) + stats.get('ABSTAIN', 0),
            'absent_count': stats.get('ABSENT', 0)
        }}
     for d, stats in summary.items()]


    individual = data["legco_Individual"][0]
    council_member = data["legco_CouncilMembers"][0]
    votes_by_month = {}
    output = {}
    output = {}
    output["id"] = member_id
    output["name_zh"] = individual["name_ch"]
    output["name_en"] = individual["name_en"]
    output["avatar"] = individual["image"]
    output["constituency_type"] = council_member["CouncilMembershipType"]["category"]
    output["constituency_district"] = council_member["CouncilMembershipType"]["sub_category"]
    output["political_affiliation"] = individual["Party"]["name_short_ch"]
    output["attendance_rate"] = attendance_rate
    output["vote_rate"] = vote_rate
    return jsonify(output)

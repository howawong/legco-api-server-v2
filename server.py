from flask import Flask, jsonify
import requests
import click
import datetime
import os
from .jobs.news.classes.graphql import *
from dotenv import find_dotenv, load_dotenv
from flask_cors import CORS
import datetime 
from datetime import timedelta, date


dotenv_path = os.getenv('ENV_FILE', os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv(dotenv_path)

app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
app.url_map.strict_slashes = False
CORS(app)

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
      source
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


@app.route("/legco/member_news/<int:member>/<int:page_size>/<int:page>/")
def member_news_paginated(member, page_size, page):
    past_3_months = datetime.datetime.now() - timedelta(days=180)
    past_3_months = past_3_months.strftime("%Y-%m-%d")
    if page < 1:
        page = 1
    query = \
"""
query MyQuery {
  legco_IndividualNews(where: {Individual: {id: {_eq: %d}}, News: {date: {_gte: "%s"}}}, order_by: {News: {date: desc}}, limit: %d, offset: %d) {
    News {
      date
      source
      image
      link
      title
      key
    }
  }
  legco_IndividualNews_aggregate(where: {Individual: {id: {_eq: %d}}, News: {date: {_gte: "%s"}}}, order_by: {News: {date: desc}}) {
    aggregate {
      count
    }
  }
}
""" % (member, past_3_months, page_size, (page - 1) * page_size, member, past_3_months)
    result =  run_query(query)
    news = result["data"]["legco_IndividualNews"]
    total = result["data"]["legco_IndividualNews_aggregate"]["aggregate"]["count"]
    r = [r["News"] for r in news]
    return jsonify({"news": r, "pagination": {"total": total, "page_size": page_size, "page": page }})



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

@app.route("/legco/members/", defaults={'sortkey': 'id', 'sortorder': 'asc'})
@app.route("/legco/members/<string:sortkey>/", defaults={'sortorder': 'asc'})
@app.route("/legco/members/<string:sortkey>/<string:sortorder>/")
def all_members_statistics(sortkey, sortorder):
    search_functions = {
        'id': lambda member: member['id'],
        'name_zh': lambda member: member['name_zh'],
        'vote_rate': lambda member: list(member['vote_rate'][0].values())[0]['vote_count'],
        'attendance_rate': lambda member: list(member['attendance_rate'][0].values())[0]['present_count'],
    }
    if sortkey not in search_functions:
        return jsonify({})
    elif sortorder not in ['asc', 'desc']:
        return jsonify({})
    year = 2016
    start_date = "2019-10-01"
    query = \
"""
query MyQuery {
  legco_IndividualVote(where: {Meeting: {date: {_gte: "%s"}}}, order_by: {Meeting: {date: asc}}) {
    Meeting {
      id
      date
      meeting_type
    }
    individual
    vote_number
    result
  }
  legco_Individual {
    id
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
  legco_CouncilMembers(where: {Council: {start_year: {_eq: %d}}}) {
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
""" % (start_date, year)
    data = run_query(query)["data"]
    votes = data["legco_IndividualVote"]
    individuals = data["legco_Individual"]
    council_members = data["legco_CouncilMembers"]
    members_statistics = {}

    # Fill in data from legco_CouncilMembers
    for council_member in council_members:
        members_statistics[council_member["member"]] = {
            "id": council_member["member"],
            "constituency_type": council_member["CouncilMembershipType"]["category"],
            "constituency_district": council_member["CouncilMembershipType"]["sub_category"],
        }

    # Fill in data from legco_Individual
    for individual in individuals:
        if individual['id'] in members_statistics:
            members_statistics[individual["id"]].update({
                "id": individual["id"],
                "name_zh": individual["name_ch"],
                "name_en": individual["name_en"],
                "avatar": individual["image"],
                "political_affiliation": individual["Party"]["name_short_ch"] if individual["Party"] else None,
            })

    # Generate vote_summary from legco_IndividualVote
    vote_summary = {}
    for vote in votes:
        d = vote["Meeting"]["date"][0:-3] + "-01"
        # meeting = vote["Meeting"]["id"]
        result = vote["result"]
        # vote_number = vote["vote_number"]
        individual = vote["individual"]
        if individual not in vote_summary:
            vote_summary[individual] = {}
        if d not in vote_summary[individual]:
            vote_summary[individual][d] = {}
        vote_summary[individual][d][result] = vote_summary[individual][d].get(result, 0) + 1

    # Fill in data from vote_summary
    for i in members_statistics:
        if not vote_summary.get(i, {}):
            members_statistics[i]['stats'] = []
        else:
            d = max(vote_summary[i])
            stats = vote_summary[i][d]
            members_statistics[i]['stats'] = [
                {   'date': d,
                    'vote_count': stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0),
                    'yes_count': stats.get('YES', 0),
                    'no_count': stats.get('NO', 0),
                    'present_count': stats.get('PRESENT', 0),
                    'abstain_count': stats.get('ABSTAIN', 0),
                    'absent_count': stats.get('ABSENT', 0),
                    'no_vote_count': stats.get('ABSTAIN', 0) + stats.get('ABSENT', 0),
                    'present_count': stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0) + stats.get('ABSTAIN', 0),
                }
            ]
    output = sorted(members_statistics.values(), key = search_functions.get(sortkey), reverse = sortorder == 'desc')
    return jsonify(output)

@app.route("/legco/member/<int:member_id>/")
def member_statistics(member_id):
    year = 2016
    start_date = "2019-10-01"
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
        d = vote["date"].split(' ')[0]
        result = vote["result"]
        if d not in summary:
            summary[d] = {}
        summary[d][result] = summary[d].get(result, 0) + 1
    stats = [
        {'date': d,
        'vote_count': stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0),
        'yes_count': stats.get('YES', 0),
        'no_count': stats.get('NO', 0),
        'present_count': stats.get('PRESENT', 0),
        'abstain_count': stats.get('ABSTAIN', 0),
        'absent_count': stats.get('ABSENT', 0),
        'no_vote_count': stats.get('ABSTAIN', 0) + stats.get('ABSENT', 0),
        'attendance_rate': 1.0 - stats.get('ABSENT', 0) /(stats.get('ABSTAIN', 0) + stats.get('ABSENT', 0)+stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0)),
        'vote_rate': (stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0))/ (stats.get('ABSTAIN', 0) + stats.get('ABSENT', 0)+stats.get('YES', 0) + stats.get('NO', 0) + stats.get('PRESENT', 0)),
        }
        for d, stats in summary.items()
    ]
    individual = data["legco_Individual"][0]
    council_member = data["legco_CouncilMembers"][0]
    output = {}
    output["id"] = member_id
    output["name_zh"] = individual["name_ch"]
    output["name_en"] = individual["name_en"]
    output["avatar"] = individual["image"]
    output["constituency_type"] = council_member["CouncilMembershipType"]["category"]
    output["constituency_district"] = council_member["CouncilMembershipType"]["sub_category"]
    output["political_affiliation"] = individual["Party"]["name_short_ch"]
    output["stats"] = stats
    return jsonify(output)

@app.route("/legco/bill_categories/")
def bill_categories():
    data = [
        #(1, '司法及法律', 'Administration of Justice and Legal Services', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(2, '工商', 'Commerce and Industry', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(3, '政制', 'Constitutional Affairs', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(4, '發展', 'Development', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(5, '經濟發展', 'Economic Development', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(6, '教育', 'Education', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(7, '環境', 'Environmental Affairs', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(8, '財經', 'Financial Affairs', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(9, '食物安全及環境衞生', 'Food Safety and Environmental Hygiene', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(10, '衞生', 'Health Services', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(11, '民政', 'Home Affairs', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(12, '房屋', 'Housing', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(13, '資訊科技及廣播', 'Information Technology and Broadcasting', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(14, '人力', 'Manpower', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(15, '公務員及資助機構員工', 'Public Service', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(16, '保安', 'Security', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(17, '交通', 'Transport', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png'),
        #(18, '福利', 'Welfare Services', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png')
    ]
    output = [{'id': d[0], 'title_zh': d[1], 'title_en': d[2], 'avatar': d[3]} for d in data]
    return jsonify(output)


@app.route("/legco/bills/")
def bills():
    query = """
query MyQuery {
  legco_Bill {
    bill_gazette_date
    bill_title_chi
    bill_title_eng
    internal_key
    tag {
      categories
      keywords
    }
    ordinance_gazette_date
    ordinance_gazette_content_url_chi
    meeting {
      date
    }
    reading {
      first_reading_date
      second_reading_date
      third_reading_date
    }
  }
}
"""
    r = run_query(query)["data"]["legco_Bill"]
    output = [ {
        "bill_gazette_date": b["bill_gazette_date"],
        "bill_title_chi": b["bill_title_chi"],
        "bill_title_eng": b["bill_title_eng"],
        "internal_key": b["internal_key"],
        "categories": b["tag"]["categories"],
        "internal_key": b["internal_key"],
        "meeting": [z["date"] for z in b["meeting"]],
        "first_reading": b["reading"]["first_reading_date"],
        "second_reading": b["reading"]["second_reading_date"],
        "third_reading": b["reading"]["third_reading_date"]
    }
    for b in r]
    return jsonify(r)

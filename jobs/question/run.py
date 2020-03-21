import requests
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup
import os
import json
from time import sleep
import sys
from lxml import etree
import hashlib
import re
from lxml.html.clean import Cleaner


def check_existence(key):
    query = """
    query MyQuery {
      __typename
      legco_Question(where: {key: {_eq: "%s"}}, limit: 1) {
        key
        date
      }
    }

    """ % key
    result = run_query(query)
    data = result["data"]["legco_Question"]
    existed = len(data) > 0
    d = None
    if existed:
        d = data[0]["date"]
    return existed, d



def parse_date(s):
    for fmt in ["%d/%m/%Y","%d-%m-%Y"]:
        try:
            return datetime.strptime(s, fmt).date().strftime("%Y-%m-%d")
        except:
            pass
    raise Exception("failed to parse %s." % (s))


def get_individuals():
    individual_query = """
    query MyQuery {
      result: legco_Individual {
        id
        name_ch
      }
    }
    """
    pairs = []
    mapping = {}
    individuals = run_query(individual_query)['data']['result']
    for individual in individuals:
        mapping[individual['name_ch']] = individual['id']
    return mapping
    

def get_records_from_url(url):
    r = requests.get(url)
    s = r.content
    doc = etree.XML(s)
    meetings = []
    for meeting_node in doc.xpath("//meeting"):
        meeting = {}
        meeting["meeting_type"] = url.split("/")[-1].split("_")[0]
        meeting["date"] = parse_date(meeting_node.attrib["start-date"])
        meeting["source_url"] = url
        motions = []
        votes = []
        summaries = []
        for vote_node in meeting_node.xpath("./vote"):
            vote = {}
            vote["date"] = parse_date(vote_node.xpath("vote-date")[0].text)
            vote["time"] = vote_node.xpath("vote-time")[0].text
            vote["vote_number"] = int(vote_node.attrib["number"])
            vote["separate_mechanism"] = vote_node.xpath("vote-separate-mechanism")[0].text
            vote["meeting"] = -1
            motion = {}
            motion["name_en"] = vote_node.xpath("motion-en")[0].text or ""
            motion["name_ch"] = vote_node.xpath("motion-ch")[0].text
            if len(vote_node.xpath("mover-en")) > 0:
                motion["mover_en"] = vote_node.xpath("mover-en")[0].text
                motion["mover_ch"] = vote_node.xpath("mover-ch")[0].text
                motion["mover_type"] = vote_node.xpath("mover-type")[0].text
            else:
                motion["mover_en"] = ""
                motion["mover_ch"] = ""
                motion["mover_type"] = ""
            motion["vote_number"] = vote["vote_number"]
            for summary_node in vote_node.xpath("vote-summary")[0].xpath("*"):
                summary = {}
                summary["vote_number"] = vote["vote_number"]
                summary["summary_type"] = summary_node.tag
                summary["present_count"] = int(summary_node.xpath("present-count")[0].text or 0)
                summary["vote_count"] = int(summary_node.xpath("vote-count")[0].text or 0)        
                summary["yes_count"] = int(summary_node.xpath("yes-count")[0].text or 0)
                summary["no_count"] =  int(summary_node.xpath("no-count")[0].text or 0)
                summary["abstain_count"] = int(summary_node.xpath("abstain-count")[0].text or 0)
                summary["result"] = summary_node.xpath("result")[0].text
                summary["meeting"] = 0        
                summaries.append(summary)
            
            individual_votes = []
            for individual_vote_node in vote_node.xpath("./individual-votes/member"):
                individual_vote = {}
                individual_vote["individual"] = -1
                individual_vote["vote_number"] = vote["vote_number"]
                individual_vote["meeting"] = 0
                individual_vote["name_ch"] = individual_vote_node.attrib["name-ch"]
                individual_vote["constituency"] = individual_vote_node.attrib["constituency"]
                individual_vote["name_en"] = individual_vote_node.attrib["name-en"]
                individual_vote["result"] = individual_vote_node.xpath("vote")[0].text.upper()
                individual_votes.append(individual_vote)
            vote["individual_votes"] = individual_votes
            votes.append(vote)
            motions.append(motion)
        meeting["motions"] = motions
        meeting["votes"] = votes
        meeting["vote_summaries"] = summaries
        meetings.append(meeting)
    return meetings


def run_query(query):
    ADMIN_SECRET = os.getenv('ADMIN_SECRET')
    ENDPOINT = os.getenv('ENDPOINT')
    HEADERS = {
        'Content-Type': 'application/json',
        'X-Hasura-Admin-Secret': ADMIN_SECRET,
    }
    j = {"query": query, "operationName": "MyQuery"}
    resp = requests.post(ENDPOINT, data=json.dumps(j), headers=HEADERS)
    return resp.json()


def notify_via_slack(messaga):
    TOKEN = os.getenv('SLACK_TOKEN')
    CHANNEL = os.getenv('SLACK_CHANNEL')
    if TOKEN is None or CHANNEL is None:
        print("No credential, skipping slack")
        return
    slack = SlackClient(TOKEN)
    slack.api_call("chat.postMessage",
                   channel=CHANNEL,
                   text=text)
    print("Sending [%s] to slack" % (text))


def upsert_records(schema, table, records, returning_keys=[], update_columns=[]):
    if len(records) == 0:
        return
    first_record = records[0]
    template = "{{\n"
    for key, value in first_record.items():
        if type(value) in [list, dict]:
            continue
        line = "            {0}:{1}{{{0}}}{1}".format(key, "\"" if type(value) == str else "")
        template += line + ",\n"
    template += "           }}"
    print(template)
    records = ",\n".join([template.format(**r) for r in records])
    records = "[%s]" % records
    returning = "\n".join(returning_keys)
    update = "\n".join(update_columns)
    query = """
mutation MyQuery {
  insert_%s_%s(
     objects: %s,
     on_conflict: {constraint: %s_pkey,update_columns: [%s]}
  ){
     affected_rows
     returning {
       %s
     }
  }
}
""" % (schema, table, records, table, update, returning)
    print(query)
    return run_query(query)


def all_text(node):
    return "".join([x for x in node.itertext()])

def crawl(year=0):
    if year == 0:
        today = date.today()
        year = today.year
        if today.month < 10:
            year = year - 1
    current_year = year - 2000
    print(current_year)
    mapping = get_individuals()
    year_start = (current_year // 4) * 4
    year_end = year_start + 4
    url = "https://www.legco.gov.hk/yr%.2d-%.2d/chinese/counmtg/question/ques%.2d%.2d.htm" % (current_year, current_year + 1, current_year, current_year + 1)
    print(url)
    r = requests.get(url)
    r.encoding = "utf-8"
    root = etree.HTML(r.text)
    dates = [d.text for d in root.xpath("//h2[@class=\"h3_style\"]/a[contains(@href,\"agenda\")]")]
    tables = root.xpath("//table[@class=\"interlaced\"]")
    if len(dates) != len(tables):
        raise Exception("Dates and Questions Mismatch! %d <> %d" % (len(dates), len(tables)) )    
    questions = []
    for i in range(0, len(dates)):
        question_date = datetime.strptime(dates[i], '%d.%m.%Y').strftime('%Y-%m-%d')
        print(question_date)
        table = tables[i]
        for row in table.xpath(".//tr")[1:]:
            cells = row.xpath("td")
            if all_text(cells[3]).strip() == '-':
                continue
            legislator_name = cells[1].text
            if legislator_name.startswith(u"郭偉强"):
                legislator_name = u"郭偉強"
            title = all_text(cells[2])
            question_type_text = all_text(cells[0])
            link_cells = cells[3].xpath(".//a")
            if len(link_cells) == 0:
                continue
            link = link_cells[0].attrib['href']
            key = str(hashlib.md5(link.encode('utf-8')).hexdigest())
            m = re.match(r"(.*[0-9]+|UQ)[\(]{0,1}(.*)\)", question_type_text)
            if m is None:
                raise Exception("Undefined Question Type", link, question_type_text)
            question_type = m.group(2)
            detail_r = requests.get(link)
            detail_r.encoding = "big5"
            output = detail_r.text
            cleaner = Cleaner(comments=False)
            output = cleaner.clean_html(output)
            detail_root = etree.HTML(output)
            try:
                press_release = all_text(detail_root.xpath("//div[@id=\"pressrelease\"]")[0])
            except IndexError:
                print(link)
                detail_r = requests.get(link)
                detail_r.encoding = "utf-8"
                if detail_r.status_code > 299:
                    print('Not available')
                    continue
                output = detail_r.text
                output = cleaner.clean_html(output)
                detail_root = etree.HTML(output)
                press_release = all_text(detail_root.xpath("//span[@id=\"pressrelease\"]")[0])
            question_start = press_release.find(u'以下')
            reply_start = press_release.rfind(u'答覆：')
            question_text = press_release[question_start:reply_start]
            answer_text = press_release[reply_start + 3:]   
            individual =  mapping.get(legislator_name.replace("議員", ""), -1)
            question_dict = {
                'key': key,
                'individual': individual,
                'date': question_date,
                'question_type': question_type,
                'question': question_text,
                'answer': answer_text,
                'title': title,
                'link': link,
                'title_ch': title,
            }
            questions.append(question_dict)
    for q in questions:
        key = q['key']
        existed, _ = check_existence(key)
        if not existed:
            for c in ["answer", "question", "title", "title_ch"]:
                q[c] = json.dumps(q[c])[1:-1]
            text = "New question is available at %s." % (q['link'])
            upsert_result = upsert_records("legco", "Question", [q], ["question", "answer"])
            if "errors" in upsert_result:
                raise Exception(upsert_result["errors"][0]["message"])
            notify_via_slack(text)
        else:
            print('Already uploaded')

now = datetime.now()
year = now.year
month = now.month
if month < 10:
    year = year - 1
crawl(year)

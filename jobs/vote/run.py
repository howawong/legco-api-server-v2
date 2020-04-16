import requests
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup
import os
import json
from time import sleep
import sys
from lxml import etree

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
    #print(template)
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
    #print(query)
    return run_query(query)


schema = "legco"
meeting_table = "Meeting"
motion_table = "Motion"
vote_table = "Vote"
individual_vote_table = "IndividualVote"
vote_summary_table = "VoteSummary"


def get_id_from_result(summary, key="id"):
    returning = summary.get("returning", [])
    if len(returning) == 0:
        return None
    return returning[0][key]

def crawl(year=0):
    if year == 0:
        today = date.today()
        year = today.year
        if today.month < 10:
            year = year - 1
    current_year = year - 2000
    year_range = "%d-%d" % (current_year, current_year + 1) 
    meeting_types = ["cm", "esc", "pwsc", "hc", "fc"]
    url_format = {
        "cm": "http://www.legco.gov.hk/yr%s/chinese/counmtg/voting/cm_vote_",
        "esc": "http://www.legco.gov.hk/yr%s/chinese/fc/esc/results/esc_vote_",
        "pwsc": "http://www.legco.gov.hk/yr%s/chinese/fc/pwsc/results/pwsc_vote_",
        "hc": "http://www.legco.gov.hk/yr%s/chinese/hc/voting/hc_vote_",
        "fc": "http://www.legco.gov.hk/yr%s/chinese/fc/fc/results/fc_vote_"
    }
    detect_url_format = \
        "http://www.legco.gov.hk/php/detect-votes.php?term=yr%s&meeting=%s"
    output = []
    for yr in [year_range]:
        for mc in meeting_types:
            detect_url = detect_url_format % (yr, mc)
            r = requests.get(detect_url)
            #print(detect_url)
            xml_files = [f for f in r.text.split(",") if f.endswith(".xml")]
            for xml_file in xml_files:
                output.append(url_format[mc] % (yr) + xml_file)
    return output    


def upsert_meetings(meetings, mapping):
    for meeting in meetings:
        result = upsert_records(schema, meeting_table, [meeting], ["id"], ["date"])
        result_data = result.get("data", None)
        if result_data is not None:
            meeting_key = 'insert_%s_%s' % (schema, meeting_table) 
            summary = result_data[meeting_key]
            affected_rows = summary["affected_rows"]
            #print(summary)
            if affected_rows == 0:
                print("%s Already existed." % (meeting["source_url"]))
            meeting_id = get_id_from_result(summary)
            print("Meeting ID: %d" % meeting_id)
            for vote, vote_summary, motion in zip(meeting["votes"], meeting["vote_summaries"], meeting["motions"]):
                motion["meeting"] = meeting_id
                motion["vote_number"] = vote["vote_number"]
                upsert_records(schema, motion_table, [motion], ["meeting", "vote_number"])
                vote["meeting"] = meeting_id
                #print(vote)
                vote_result = upsert_records(schema, vote_table, [vote], ["meeting", "vote_number"])
                #print(vote_result)
                vote_result_data = vote_result.get("data", None)
                vote_key = 'insert_%s_%s' % (schema, vote_table) 
                vote_summary["vote_number"] = vote["vote_number"]
                vote_summary["meeting"] = meeting_id 
                vote_summary_result = upsert_records(schema, vote_summary_table, [vote_summary], ["vote_number", "meeting"])
                individual_votes = vote["individual_votes"]
                for individual_vote in individual_votes:
                    individual_vote["vote_number"] = vote["vote_number"]
                    individual_vote["meeting"] = vote["meeting"]
                    individual_vote["individual"] = mapping.get(individual_vote["name_ch"], -1) 
                print(upsert_records(schema, individual_vote_table, individual_votes, ["vote_number", "individual", "meeting"], ["individual"]))
                #print(vote_summary_result)
            print("Meeting ID: %d" % meeting_id)
        else:
            print(result)

mapping = get_individuals()
urls = crawl(2017) + crawl(2016)   
for url in urls:
    meetings = get_records_from_url(url)
    #print(meetings)
    upsert_meetings(meetings, mapping)

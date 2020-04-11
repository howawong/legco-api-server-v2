import requests
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup, SoupStrainer
import os
import json
from time import sleep
import sys
from io import BytesIO
import re
from pdfminer.layout import LAParams, LTTextBox
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator



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
        if type(value) in [list, dict] or value is None:
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


def delete_committee_member_by_internal_key(internal_key):
    query = """
mutation MyQuery {
  delete_legco_BillCommitteeMember(where: {internal_key: {_eq: "%s"}}) {
    affected_rows
  }
}
""" % (internal_key)
    #print(query)
    return run_query(query)

def strip_value(d):
    for k in d.keys():
        if d[k] is not None:
            d[k] = d[k].strip()
    return d


def get_bill_committee_members(bill_committee):
    members = []
    url = bill_committee["bills_committee_url_chi"]
    url = url.replace(".htm", "_mem.htm")
    if url != "":
        r = requests.get(url)
        if r.ok:
            print("Bill Committee URL: %s" % url)
            soup = BeautifulSoup(r.content, 'html.parser', parse_only=SoupStrainer("table"))
            table = soup.find("table")
            for row in table.find_all("tr"):
                cell = row.find_all("td")[-1]
                for title in cell.text.strip().split("\n"):
                    if "議員" in title and "合共" not in title and "總數" not in title:
                        title = title.replace("議員", "")
                        members.append(title.split(",")[0].strip())
        else:
            print("Something wrong at %s:%d" % (url, r.status_code))
    return members


def get_bill_content(bill):
    url = bill["bill_content_url_chi"]
    print("Bill PDF URL: %s" % url)
    r = requests.get(url)
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    pages = PDFPage.get_pages(BytesIO(r.content), check_extractable=False)
    started = True
    end = False
    desc = ""
    for page in pages:
        if end:
            break
        interpreter.process_page(page)
        layout = device.get_result()
        texts = []
        for lobj in layout:
            if end:
                break
            if isinstance(lobj, LTTextBox):
                x, y, text = lobj.bbox[0], lobj.bbox[3], lobj.get_text()
                if  x >= 400:
                    continue
                texts.append((x, y, text))
        texts = sorted(texts, key = lambda x: (-x[1], x[0]))
        page_text = "".join([x[2].strip() for x in texts])
        m = re.match(r'(.*)旨在(.*)(由立法會制(定|訂)。|弁言)', page_text)
        if m is not None:
            desc = m.group(2)
    return desc


def get_bills(mapping, year):
    r = requests.get("https://app.legco.gov.hk/BillsDB/odata/Vbills?$filter=year(bill_gazette_date)%20eq%20" + str(year))
    raw_bills = r.json()["value"]
    bills = []
    readings = []
    committees = []
    committee_members = []
    descriptions = []
    for raw_bill in raw_bills:
        bill = {k: raw_bill[k] for k in raw_bill.keys() if k in ["internal_key","ordinance_title_eng","ordinance_title_chi","ordinance_content_url_eng","ordinance_content_url_chi","bill_title_eng","bill_title_chi","proposed_by_eng","proposed_by_chi","bill_gazette_date","bill_content_url_eng","bill_content_url_chi","bill_gazette_date_2","bill_content_url_2_eng","bill_content_url_2_chi","bill_gazette_date_3","bill_content_url_3_eng","bill_content_url_3_chi","ordinance_gazette_date","ordinance_year_number_eng","ordinance_year_number_chi","ordinace_gazette_content_url_eng","ordinance_gazette_content_url_chi","legco_brief_file_reference","legco_brief_url_eng","legco_brief_url_chi","additional_information_eng","additional_information_chi","remarks_eng","remarks_chi"]}
        bill = strip_value(bill)
        bill_committee =  {k: raw_bill[k] for k in raw_bill.keys() if k in ["internal_key","bills_committee_title_eng","bills_committee_title_chi","bills_committee_url_eng","bills_committee_url_chi","bills_committee_formation_date","bills_committee_report_url_eng","bills_committee_report_url_chi"]} 
        bill_committee = strip_value(bill_committee)
        bill_reading =  {k: raw_bill[k] for k in raw_bill.keys() if k in ["internal_key","first_reading_date","first_reading_date_hansard_url_eng","first_reading_date_hansard_url_chi","first_reading_date_2","first_reading_date_2_hansard_url_eng","first_reading_date_2_hansard_url_chi","second_reading_date","second_reading_date_hansard_url_eng","second_reading_date_hansard_url_chi","second_reading_date_2","second_reading_date_2_hansard_url_eng","second_reading_date_2_hansard_url_chi","second_reading_date_3","second_reading_date_3_hansard_url_eng","second_reading_date_3_hansard_url_chi","second_reading_date_4","second_reading_date_4_hansard_url_eng","second_reading_date_4_hansard_url_chi","second_reading_date_5","second_reading_date_5_hansard_url_eng","second_reading_date_5_hansard_url_chi","third_reading_date","third_reading_date_hansard_url_eng","third_reading_date_hansard_url_chi"]}
        bill_reading = strip_value(bill_reading)
        member_names = get_bill_committee_members(bill_committee)
        description = get_bill_content(bill)
        
        bills.append(bill)
        readings.append(bill_reading)
        committee_members += [{'internal_key': bill['internal_key'], 'member_name': k, 'individual': mapping.get(k, -1), 'pos': i} for i, k in enumerate(member_names)]
        committees.append(bill_committee)
        descriptions.append({'internal_key': bill['internal_key'], 'description': description})
        sleep(0.5)
    return bills, readings, descriptions, committees, committee_members

mapping = get_individuals()

for year in range(2016, 2021):
    print("Year: %d" % year)
    bills, readings, descriptions, committees, committee_members = get_bills(mapping, year)
    upsert_records('legco', 'Bill', bills, returning_keys=['internal_key'])
    upsert_records('legco', 'BillReading', readings, returning_keys=['internal_key'])
    upsert_records('legco', 'BillDescription', descriptions, returning_keys=['internal_key'])
    upsert_records('legco', 'BillCommittee', committees, returning_keys=['internal_key'])
    for bill in bills:
        delete_committee_member_by_internal_key(bill["internal_key"])
    upsert_records('legco', 'BillCommitteeMember', committee_members, returning_keys=['internal_key'])
    print_memory()

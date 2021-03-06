import requests
from bs4 import BeautifulSoup,  SoupStrainer
import json
import hashlib
import os
import urllib
import multiprocessing
from time import sleep
import sys
from datetime import datetime
from dateutil.tz import tzoffset
from datetime import datetime, timedelta
from apiclient import discovery
from google.oauth2 import service_account
import base64
import traceback


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
    for r in records:
        for k in r.keys():
            v = r[k]
            if type(v) == bool:
                v = str(v).lower()
                r[k] = v
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
    return run_query(query)




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


def run_query(query):
    ADMIN_SECRET = os.getenv("ADMIN_SECRET")
    ENDPOINT = os.getenv("ENDPOINT")
    HEADERS = {
        "Content-Type": "application/json",
        "X-Hasura-Admin-Secret": ADMIN_SECRET,
    }
    j = {"query": query, "operationName": "MyQuery"}
    resp = requests.post(ENDPOINT, data=json.dumps(j), headers=HEADERS)
    j = resp.json()
    if "data" not in j:
        print(query)
    return j


# https://stackoverflow.com/a/23862195/278528

def colnum_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


def upload_to_google_sheet(bills):
    output_rows = [[bill["internal_key"]
                    , bill["bill_title_chi"]
                    , "=VLOOKUP($A%d,Category!$A$2:$E, 2, FALSE)" % (idx + 2)
                    , "=VLOOKUP($A%d,Category!$A$2:$E, 3, FALSE)" % (idx + 2)
                    ] + ["=VLOOKUP($A%d & %s$1, Member!$A$2:$D,4,FALSE)" % (idx + 2, colnum_string(j + 4) )  for j in range(1, 67) ]  for idx, bill in enumerate(bills)]

    base64_credentials = os.getenv("GOOGLE_CRED", "")
    decoded_credentials = base64.b64decode(base64_credentials)
    info = json.loads(decoded_credentials)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    service = discovery.build("sheets", "v4", credentials=credentials)

    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    sheet_name = "Master"
    range_name = "%s!A2:BR%d" % (sheet_name, len(output_rows) + 1)
    values = output_rows
    data = {
        "values" : values
    }

    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range="%s!A2:BR"% (sheet_name)).execute()
    service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption="USER_ENTERED").execute()


def get_bill_tags():
    base64_credentials = os.getenv("GOOGLE_CRED", "")
    decoded_credentials = base64.b64decode(base64_credentials)
    info = json.loads(decoded_credentials)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    service = discovery.build("sheets", "v4", credentials=credentials)

    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    sheet_name = "Master"
    sheet = service.spreadsheets()
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    range_name = "%s!A2:D" % (sheet_name)
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    tags = [{"internal_key": v[0], "categories": v[1], "keywords": v[2]} for v in values]
    return tags

   


def get_bills():
    individual_query = """
    query MyQuery {
      result: legco_Bill {
        internal_key
        bill_title_chi
      }
    }
    """
    bills = run_query(individual_query)["data"]["result"]
    return bills
 

completed = False
try:
    print_memory()
    bills = get_bills()
    #upload_to_google_sheet(bills)
    tags = get_bill_tags()
    upsert_records('legco', 'BillTag', tags, returning_keys=['internal_key'], update_columns=["categories", "keywords"])
    print_memory()
    completed = True
except Exception as e:
    traceback.print_exc()
send_to_telegram("completed" if completed else "error")


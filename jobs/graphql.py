import requests
import json
import os


ADMIN_SECRET = os.getenv('ADMIN_SECRET')
ENDPOINT = os.getenv('ENDPOINT')

def run_query(query):

    HEADERS = {
        'Content-Type': 'application/json',
        'X-Hasura-Admin-Secret': ADMIN_SECRET,
    }
    j = {"query": query, "operationName": "MyQuery"}
    resp = requests.post(ENDPOINT, data=json.dumps(j), headers=HEADERS)
    return resp.json()



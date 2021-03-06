import requests
import json
import os


def run_query(query):
    ADMIN_SECRET = os.getenv('ADMIN_SECRET')
    ENDPOINT = os.getenv('ENDPOINT')
    HEADERS = {
        'Content-Type': 'application/json',
        'X-Hasura-Admin-Secret': ADMIN_SECRET,
    }
    j = {"query": query, "operationName": "MyQuery"}
    resp = requests.post(ENDPOINT, data=json.dumps(j), headers=HEADERS)
    result = resp.json()
    errors = result.get("errors", None)
    if errors is not None:
        error  = errors[0]
        raise Exception(error["message"])
    return result



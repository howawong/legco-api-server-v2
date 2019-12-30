import  json
import requests
import re
import sys
from .graphql import run_query



def update_individual_news(news):
    individual_query = """
    query MyQuery {
      result: legco_Individual {
        id
        name_ch
      }
    }
    """
    pairs = []
    individuals = run_query(individual_query)['data']['result']
    for article in news:
        for member in individuals:
            if member['name_ch'] in article['text'] or member['name_ch'] in article['title']:
                print(member['name_ch'], article['link'], article['key'], member['id'])
                pairs.append((article['key'], member['id']))

    news_individual = '[' +  ','.join(['{news: "%s", individual: %d}' % (p[0], p[1]) for p in pairs ]) + ']'
    news_individual_query = """
      mutation MyQuery {
        insert_legco_IndividualNews(
          objects: %s,
          on_conflict: {constraint: IndividualNews_pkey,update_columns: []}
        ){
          affected_rows
        }
      }
    """ % (news_individual)

    return run_query(news_individual_query)


from .graphql import run_query
from time import sleep
import requests
import re


def update_news_like_count(d):
    def get_engagement(link):
        url = "https://www.facebook.com/v2.5/plugins/like.php" 
        params = {'locale': 'en_US', 'href': link}
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        r = requests.get(url, params=params, headers=headers)
        #p = r.text.find('people like this.')
        #print(r.text[p - 20: p + 40])
        m = re.search("<span>(.*) people like this.", r.text)
        if m is None:
            return 0
        g = m.group(1).strip()
        total = 0
        if 'K' in g:
            total  = int(float(g.replace('K', '')) * 1000)
        elif 'M' in g:
            total  = int(float(g.replace('M', '')) * 100000)
        else:
            total = int(g)
        return total


    query = """
    query MyQuery {
      legco_IndividualNews(where: {News: {date: {_gte: "%s"}}}) {
        Individual {
          name_ch
          id
        }
        News {
          link
          key
          date
        }
      }
    }

    """ % (d)

    links = set()

    news = run_query(query)['data']['legco_IndividualNews']
    for article in news:
        links.add(article['News']['link'])


    counts_by_url = {}

    for link in list(links):
        count = get_engagement(link)
        counts_by_url[link] = count

    output = {}

    for article in news:
        link = article['News']['link']
        if link in counts_by_url:
            key = article['News']['key']
            count = counts_by_url[link]
            output[key] = count

    mutation_data = '[' +  ',\n'.join(['{key: "%s", engagement: %d }' % (key, value)   for key, value in output.items()]) + ']'

    counts_query = """
        mutation MyQuery {
           insert_legco_IndividualNewsEngagement(
              objects: %s,
              on_conflict: {constraint: IndividualNewsEngagement_pkey,update_columns: [engagement]}
            ){
              affected_rows
            }
          }
    """ % (mutation_data)
    print(counts_query)
    print(run_query(counts_query))


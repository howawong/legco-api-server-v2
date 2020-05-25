from .graphql import run_query

def upsert_news(news):
    template = """
      {{
        source: "{source}",
        date: "{date}",
        image: "{image}",
        link: "{link}",
        text: "{text}",
        key: "{key}",
        title: "{title}"
      }}
    """
    news_objects = ",\n".join([template.format(**article) for article in news])
    news_objects = "[%s]" % news_objects
    query = """
      mutation MyQuery {
        insert_legco_News(
          objects: %s,
          on_conflict: {constraint: News_pkey,update_columns: []}
        ){
          affected_rows
        }
      }
    """ % (news_objects)


    return run_query(query)

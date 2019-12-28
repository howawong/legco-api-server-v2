import sys
import requests
from slimit import ast
from slimit.parser import Parser as JavascriptParser
from slimit.visitors import nodevisitor
from bs4 import BeautifulSoup
from io import StringIO
import re
import multiprocessing
import json
import hashlib
from .graphql import run_query


def fetch(item):
    msg = ""
    try:
        r = requests.get(item["link"])
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        msg = r.text
        item["title"] = soup.find("meta",  property="og:title")["content"]
        item["image"] = soup.find("meta",  property="og:image")["content"]
        script_text = None
        for s in soup.find_all("script"):
            if s.string is None:
                continue
            if "Fusion.globalContent" in s.string:
                script_text = s.string
        text = ""
        if script_text is not None:
            tree = JavascriptParser().parse(script_text)
            for node in nodevisitor.visit(tree):
                if not isinstance(node, ast.Assign):
                    continue
                left = node.left.to_ecma()
                if "Fusion.globalContent" == left:
                    data = json.loads(node.right.to_ecma()).get("content_elements", [])
                    text = "<br/>".join([x["content"] for x in data if "content" in x])
        item["text"] = text
    except Exception as e:
        print("cannot parse %s" % (item["link"]))
        raise
    item["source"] = "appledaily"
    item["key"] =  hashlib.md5(item["link"].encode()).hexdigest()
    return item


def fetch_news_from_appledaily(d):
    r = requests.get("http://hk.apple.nextmedia.com/archive/index/%s/index/" % d)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.find_all("a"):
        href = a["href"]
        m = re.match(r"(http|https)://hk.([A-z]+).appledaily.com(/[^/]*)/([^/]*)/([^/]*)/([^/]*)/([^/]*)", href)
        if m is not None:
            g = list(m.groups())
            if g[-2] == d and g[-1] != "index" and g[-3] != "index":
                if g[1] == 'news':
                    links.append(href)
                    #print(href)
    items = [{"link": link, "date": "%s-%s-%s" % (d[0:4], d[4:6], d[6:8])} for link in list(set(links))]
    pool = multiprocessing.Pool()
    items = pool.map_async(fetch, items).get()
    return items

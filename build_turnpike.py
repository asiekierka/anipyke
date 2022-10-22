from anipyke.lib import *
from urllib.parse import urljoin
import anipyke.db as db
import copy
import logging
import os
import re
import sqlalchemy
import sys

target_date = datetime.date(1999, 4, 12)
logger.info(f"=== Building Turnpike for date {target_date} ===")
target_path = f"output"

try:
    shutil.rmtree(target_path)
except Exception:
    pass

anipike_files_to_add = ["index.html"]
anipike_files_added = {}

def request_anipike_file(filename):
    global anipike_files_added, anipike_files_to_add
    if filename.startswith("mailto:"):
        return
    if filename.startswith("news:"):
        return
    if filename not in anipike_files_added:
        if filename not in anipike_files_to_add:
            anipike_files_to_add.append(filename)

def to_anipike_url(url, current):
    if "://" not in url:
        # relative url
        if not url.startswith("/"):
            url = urljoin(current, url)
    else:
        a_prefixes = ["http://anime.jyu.fi/~anipike", "http://www.anipike.com"]
        for p in a_prefixes:
            if url.startswith(p):
                url = url[len(p):]
    if url.startswith("/"):
        url = url[1:]
    if ("://" not in url) and ("." not in url):
        if not url.endswith("/"):
            url += "/"
        url += "index.html"
    if ("#" in url):
        url = url[0:url.index("#")]
    return url

def anipike_map_url(url, current):
    orig_url = url
    url = to_anipike_url(url, current)
    if ("://" not in url):
        request_anipike_file(url)
        return orig_url
    else:
        url = normalize_url(url)
        url = db.get_archived_url(url, target_date)
        if url is not None:
            logger.info(f"Found URL: {url}")
            return url
        else:
            return None

def anipike_delete_element(el):
    if el.parent.name in ['li']:
        el.parent.decompose()
    else:
        el.decompose()

with db.new_session() as session:
    while len(anipike_files_to_add) > 0:
        filename = anipike_files_to_add[0]
        anipike_files_added[filename] = True
        anipike_files_to_add = anipike_files_to_add[1:]
        data = db.get_latest_anipike_file(filename, target_date)
        if data is None:
            logger.warning(f"Could not find {filename}")
            continue
        if ext_is_html(filename):
            html = BeautifulSoup(data, 'lxml')
            map_html_urls(html, lambda u: anipike_map_url(u, filename), lambda x: anipike_delete_element(x))
            
            new_meta = html.new_tag("meta")
            new_meta.attrs["charset"] = "utf-8"
            html.head.append(new_meta)
            
            data = str(html)

        if type(data) == str:
            data = data.encode("utf-8")
        target_filename = f"{target_path}/{filename}"
        try:
            os.makedirs(os.path.dirname(target_filename))
        except Exception:
            pass
        with open(target_filename, "wb") as f:
            f.write(data)
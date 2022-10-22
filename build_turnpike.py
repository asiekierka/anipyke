from anipyke.lib import *
from urllib.parse import urljoin
import anipyke.db as db
import collections
import copy
import logging
import os
import re
import sqlalchemy
import sys

target_date = datetime.date(2001, 1, 1)
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

absolute_urls = {}

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
    global absolute_urls
    orig_url = url
    url = to_anipike_url(url, current)
    if ("://" not in url):
        request_anipike_file(url)
        return orig_url
    else:
        url = normalize_url(url)
        url_key = url
        if url_key in absolute_urls:
            return absolute_urls[url_key]
        else:
            url = db.get_archived_url(url, target_date)
            if url is not None:
                absolute_urls[url_key] = url
                logger.info(f"Found URL: {url}")
                return url
            else:
                absolute_urls[url_key] = None
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
            if type(data) != str:
                data = data.decode("windows-1252")
            html = BeautifulSoup(data, 'lxml')
            map_html_urls(html, lambda u: anipike_map_url(u, filename), lambda x: anipike_delete_element(x))
            
            add_html_meta_utf8(html)

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

# fix index.html one last time
with open("output/index.html", "r") as f:
    index_data = f.read()

found_urls = len(list(filter(lambda k: absolute_urls[k] is not None, absolute_urls.keys())))
all_urls = len(absolute_urls)
index_data = index_data.replace("Creation Date: 8/16/95", f"Creation Date: 8/16/95 / Preserved: {found_urls}/{all_urls} URLs<br>Unofficial restoration by <a href=\"http://asie.pl\">asie</a> for archival/education purposes.")

with open("output/index.html", "w") as f:
    f.write(index_data)

# generate new new.html
shutil.move("output/new.html", "output/new.html.origanipyke")

with open("new.html.template", "r") as f:
    new_data = f.read()

changelog_data = ""

with db.new_session() as session:
    # find all URLs added
    new_urls = session.execute(
        sqlalchemy.select(db.UrlLocation)
            .where(db.UrlLocation.add_date != None)
            .order_by(db.UrlLocation.add_date.desc())
    ).scalars()

    changes_by_date = collections.OrderedDict()
    urls_added = {}

    for url in new_urls:
        date_str = url.add_date.strftime("%m/%d/%Y")
        print(f"{url.prefix}")
        for url_found in session.execute(
            sqlalchemy.select(db.AnipikeWebsite)
                .where(db.AnipikeWebsite.link_normalized.startswith(url.url))
        ).scalars():
            print(f"- {url_found.link_normalized}")
            url_href = db.url_location_to_url(url_found.link_normalized, url)
            if url_href is None:
                logger.warn(f"Weird URL location: {url_href}")
                continue
            url_href = normalize_url(url_href)
            if url_href not in urls_added:
                if date_str not in changes_by_date:
                    changes_by_date[date_str] = []
                changes_by_date[date_str].append(f"<a href=\"{url_href}\">{url_found.link_name}</a>")
                urls_added[url_href] = True

    for ch_date, ch in changes_by_date.items():
        changelog_data += f"<h2>{ch_date}</h2><ul>\n"
        ch.reverse()
        for i in ch:
            changelog_data += f"<li>{i}</li>\n"
        changelog_data += f"</ul>\n"

new_data = new_data.replace("[CHANGELOG]", changelog_data)

create_dir_parent("output/new.html")
with open("output/new.html", "w") as f:
    f.write(new_data)

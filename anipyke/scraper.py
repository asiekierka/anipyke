from anipyke.lib import *
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
import copy
import logging
import io
import re
import requests
import shutil
import subprocess
import sys

# [tag] = (attr, follow?)
scrape_tag_attribute_pairs = {}
for i in [
    ("body", "background", False), # oddballs
    ("frame", "src", True),
    ("iframe", "src", True),
    ("a", "href", True), # https://www.w3.org/TR/html401/sgml/dtd.html#URI
    ("area", "href", True),
    ("img", "src", False),
    ("img", "longdesc", True),
    ("img", "usemap", False),
    ("script", "src", False),
    ("link", "href", False)
]:
    if i[0] not in scrape_tag_attribute_pairs:
        scrape_tag_attribute_pairs[i[0]] = []
    scrape_tag_attribute_pairs[i[0]].append((i[1], i[2]))

def has_no_extension(name):
    rname = name.rsplit("/", maxsplit=1)[1]
    return "." not in rname

protocol_in_front = re.compile("^[a-zA-Z]+:")
tripod_members = re.compile(r"members\.tripod\.com/\~?([^/]+)")

class WebScraper(object):
    def __init__(self, urls, target_path):
        self.urls = urls
        self.urls_scraped = {}
        # url, follow?
        self.urls_queue = []
        for u in urls:
            self.urls_queue.append((u, True))
        self.target_path = target_path
        basic_prefix_url = normalize_url(self.urls[0])
        self.prefix = basic_prefix_url.replace("http://", "")
        self.prefix_urls = [basic_prefix_url]
        if tripod_members.search(basic_prefix_url) is not None:
            m = tripod_members.search(basic_prefix_url)
            member_name = m.group(1).lower()
            logger.info(f"Detected Tripod-style members URL {member_name}")
            self.prefix_urls.append(f"http://{member_name}.tripod.com")
            self.prefix_urls.append(f"https://{member_name}.tripod.com")

    def is_targetted_location(self, url):
        for prefix in self.prefix_urls:
            if url.startswith(prefix):
                return True
        return False

    def add_url_to_queue(self, url, follow):
        if url not in self.urls_scraped:
            if url not in map(lambda x: x[0], self.urls_queue):
                self.urls_queue.append((url, follow))

    def find_all_links(self, html):
        for tag, attrs in scrape_tag_attribute_pairs.items():
            for el in html.select(tag):
                if el.attrs is None:
                    continue
                for attr_name, attr_follow in attrs:
                    if attr_name in el.attrs:
                        link = el.attrs[attr_name].strip()
                        if (len(link) > 0) and ((protocol_in_front.match(link) is None) or link.startswith("http")):
                            def modifier(url):
                                logger.info(f"replacing {link} with {url}")
                                if "tppabs" not in el.attrs:
                                    el.attrs["tppabs"] = link
                                el.attrs[attr_name] = url

                            yield (link, attr_follow, modifier)

    def scrape(self):
        warc_path = "warcs/" + datetime.datetime.now().isoformat() + ".warc.gz"
        create_dir_parent(warc_path)
        with open(warc_path, 'wb') as output:
            writer = WARCWriter(output, gzip=True)
            while len(self.urls_queue) > 0:
                url = self.urls_queue[0][0]
                follow_from = self.urls_queue[0][1]
                self.urls_queue = self.urls_queue[1:]
                self.urls_scraped[url] = True

                if url.startswith("http://members.tripod.com/bin/counter/"):
                    continue
                while "//" in url[9:]:
                    url = url[0:9] + url[9:].replace("//", "/")

                target_file_path = self.target_path + "/" + normalize_url(url).replace("http://", "")                    
                raw_cache_path = "scraper_cache/raw/" + normalize_url(url).replace("http://", "")
                html_cache_path = "scraper_cache/html/" + normalize_url(url).replace("http://", "")
                
                logger.info(f"Downloading {url} to {target_file_path}...")

                is_html = False
                data = None

                if os.path.isdir(raw_cache_path):
                    raw_cache_path += "/index.html"
                if os.path.isdir(html_cache_path):
                    html_cache_path += "/index.html"

                if os.path.exists(raw_cache_path):
                    is_html = False
                    with open(raw_cache_path, "rb") as f:
                        data = f.read()
                elif os.path.exists(html_cache_path):
                    is_html = True
                    with open(html_cache_path, "rb") as f:
                        data = f.read()
                else:
                    try:
                        r = requests.get(url, stream=True)
                    except Exception:
                        logger.error("Could not download - requests connection error")
                        continue
                    headers_list = r.raw.headers.items()
                    if r.status_code == 200:
                        r.raw.decode_content = True
                        try:
                            data = r.raw.read()
                        except Exception:
                            logger.error("Could not download - requests error")
                            continue
                        is_html = "html" in r.headers["content-type"]

                        if is_html:
                            if has_no_extension(target_file_path):
                                raw_cache_path += "/index.html"
                                html_cache_path += "/index.html"

                        cache_path = html_cache_path if is_html else raw_cache_path
                        create_dir_parent(cache_path)
                        with open(cache_path, "wb") as f:
                            f.write(data)

                        http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')
                        record = writer.create_warc_record(url, 'response', payload=io.BytesIO(data), http_headers=http_headers)
                    else:
                        logger.info(f"Could not download - code {r.status_code}")
                        continue

                if is_html:
                    if os.path.isdir(target_file_path) or has_no_extension(target_file_path):
                        url += "/"
                        target_file_path += "/index.html"
                    # html file
                    html = BeautifulSoup(data, "html.parser")

                    for follow_to_link, follow_should, followed_link_modify in self.find_all_links(html):
                        if (follow_to_link.lower().startswith("www.") or follow_to_link.lower().startswith("members.tripod.com")) and (("/" in follow_to_link) or (".htm" not in follow_to_link.lower())):
                            follow_to_link = "http://" + follow_to_link
                            followed_link_modify(follow_to_link)
                        if follow_to_link.startswith("/"):
                            follow_to_link = normalize_url(urljoin(url, follow_to_link))
                            followed_link_modify(follow_to_link)

                        follow_to_queue_link = normalize_url(urljoin(url, follow_to_link))
                        print(follow_to_queue_link)
                        # if follow_from AND (we're on the same prefix OR we don't follow further), queue the URL
                        # `-> if follow_should, follow the URL
                        # `-> this also means patching up the element
                        # (not follow_should) or 
                        if follow_from and self.is_targetted_location(follow_to_queue_link):
                            url_follow = follow_should
                            self.add_url_to_queue(follow_to_queue_link, url_follow)

                            if follow_to_link.startswith("http://") or follow_to_link.startswith("https://"):
                                logger.info(target_file_path)
                                print(normalize_url(url).replace("http://", "").replace("https://", ""))
                                steps_to_descend = len(normalize_url(url).replace("http://", "").replace("https://", "").split("/")) - 1
                                follow_to_relative = ("../" * steps_to_descend) + normalize_url(follow_to_link).replace("http://", "")
                                followed_link_modify(follow_to_relative)

                    create_dir_parent(target_file_path)
                    with open(target_file_path, "w") as f:
                        
                        f.write(f"<!-- Archived by AniPyke on {datetime.datetime.now().isoformat()} from {url} -->")
                        f.write(str(html))
                else:
                    # binary file
                    create_dir_parent(target_file_path)
                    with open(target_file_path, "wb") as f:
                        f.write(data)


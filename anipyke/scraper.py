from anipyke.lib import *
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.parse import urljoin
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
import anipyke.db as db
import asyncio
import copy
import logging
import io
import re
import requests
import shutil
import subprocess
import sys
import time

proxy_session = requests.Session()

proxy_session.proxies = {
   'http': 'http://127.0.0.1:23000',
   'https': 'http://127.0.0.1:23000',
}

# [tag] = (attr, follow?)
scrape_tag_attribute_pairs = {}
for i in [
    ("body", "background", False), # oddballs
    ("table", "background", False),
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

def is_root_page(name):
    rname = name.split("://", maxsplit=1)[-1]
    if rname.endswith("/"):
        rname = rname[:-1]
    return "/" not in rname

def has_no_extension(name):
    rname = name.rsplit("/", maxsplit=1)[-1]
    return "." not in rname

protocol_in_front = re.compile("^[a-zA-Z]+:")
css_url = re.compile("url\(([^)]+)\)")
tripod_members = re.compile(r"members\.tripod\.com/\~?([^/]+)")

defunct_websites = [
    "http://www.geocities.com",
    "http://www.fortunecity.com"
]

blocked_websites = [
    "http://members.tripod.com/bin/counter",
    "http://ln.doubleclick.net",
    "http://ad.linkexchange.com",
    "http://banner.linkexchange.com",
    "http://fastcounter.linkexchange.com",
    "http://leader.linkexchange.com",
    "http://hb.lycos.com/header"
]

class webLinkModifierAttr(object):
    def __init__(self, el, attr_name, orig_link):
        self.el = el
        self.attr_name = attr_name
        self.orig_link = orig_link

    def modify(self, url):
        # if self.attr_name == "href" and self.el.name == "area":
        #     logger.info(f"replacing {self.orig_link} in {self.attr_name} with {url}")
        if "tppabs" not in self.el.attrs:
            self.el.attrs["tppabs"] = self.orig_link
        self.el.attrs[self.attr_name] = url

class cssInlineStyleModifierLink(object):
    def __init__(self, el, attr_name, orig_link):
        self.el = el
        self.attr_name = attr_name
        self.orig_link = orig_link

    def modify(self, url):
        logger.info(f"inline css: replacing {self.orig_link} in {self.attr_name} with {url}")
        if "tppabs" not in self.el.attrs:
            self.el.attrs["tppabs"] = self.orig_link
        self.el.attrs[self.attr_name] = self.el.attrs[self.attr_name].replace(self.orig_link, url)
        self.orig_link = url

class cssTextStyleModifierLink(object):
    def __init__(self, orig_link):
        self.orig_link = orig_link

    def modify(self, url):
        logger.info(f"style css: replacing {self.orig_link} with {url}")
        self.orig_link = url

class WebScraper(object):
    def __init__(self, urls, target_path, urls_scraped=None):
        self.urls = urls
        self.urls_scraped = urls_scraped
        if self.urls_scraped is None:
            self.urls_scraped = {}
        # url, follow?
        self.urls_queue = []
        for u in urls:
            self.urls_queue.append((u, True))
        self.target_path = target_path
        basic_prefix_url = normalize_url(self.urls[0])
        self.prefix = basic_prefix_url.replace("https://", "").replace("http://", "")
        self.prefix_urls = [self.prefix.lower()]
        if tripod_members.search(basic_prefix_url) is not None:
            m = tripod_members.search(basic_prefix_url)
            member_name = m.group(1).lower()
            logger.info(f"Detected Tripod-style members URL {member_name}")
            self.prefix_urls.append(f"{member_name}.tripod.com")

    def is_targetted_location(self, url):
        if url.startswith("https://"):
            url = url.replace("https://", "")
        elif url.startswith("http://"):
            url = url.replace("http://", "")
        url = url.lower()
        for prefix in self.prefix_urls:
            if url.startswith(prefix):
                return True
        return False

    def add_url_to_queue(self, url, follow):
        if "#" in url:
            url = url.split("#")[0]
        if (url not in self.urls_scraped) and not (url.endswith("/") and (url[:-1] in self.urls_scraped)) and not (url.endswith("index.html") and (url.rsplit("/", maxsplit=1)[0] in self.urls_scraped)):
            if follow == True:
                was_in_queue = False
                for entry in copy.copy(self.urls_queue):
                    if entry[0] == url:
                        was_in_queue = True
                        self.urls_queue.remove(entry)
                self.urls_queue.append((url, follow))
                return not was_in_queue
            else:
                if url not in map(lambda x: x[0], self.urls_queue):
                    self.urls_queue.append((url, follow))
                    return True
        return False

    def find_all_links(self, html):
        for tag, attrs in scrape_tag_attribute_pairs.items():
            for el in html.select(tag):
                if el.attrs is None:
                    continue
                for attr_name, attr_follow in attrs:
                    if attr_name in el.attrs:
                        link = el.attrs[attr_name].strip()
                        if (len(link) > 0) and ((protocol_in_front.match(link) is None) or link.startswith("http")):
                            yield (link, attr_follow, webLinkModifierAttr(el, attr_name, link))
        # inline CSS (experimental)
        for el in html.select("*"):
            if (el.attrs is not None) and ("style" in el.attrs):
                for m in css_url.finditer(el.attrs["style"]):
                    link = m.group(1).strip()
                    if (len(link) > 0) and ((protocol_in_front.match(link) is None) or link.startswith("http")):
                        yield (link, False, cssInlineStyleModifierLink(el, "style", link))
        for el in html.select("style"):
            for text in el.find_all(text = css_url):
                m = css_url.search(text)
                if m is not None:
                    link = m.group(1)
                    if (len(link) > 0) and ((protocol_in_front.match(link) is None) or link.startswith("http")):
                        modifier = cssTextStyleModifierLink(link)
                        yield (link, False, modifier)
                        new_text = text.replace(link, modifier.orig_link)
                        text.replace_with(new_text)


    def _scrape_immediate(self, url, follow_from, pbar, show_progress, wayback_date):
        if url in self.urls_scraped:
            pbar.update(1)
            return self.urls_scraped[url]
        result = self._scrape_immediate2(url, follow_from, pbar, show_progress, wayback_date)
        pbar.update(1)
        self.urls_scraped[url] = result
        return result

    def _scrape_immediate2(self, url, follow_from, pbar, show_progress, wayback_date):
        if url.startswith("https://"):
            raise Exception("uh")
                        
        if any(url.startswith(x) for x in blocked_websites):
            return False
        if ".tripod.com/adm/redirect" in url:
            return False
        while "//" in url[9:]:
            url = url[0:9] + url[9:].replace("//", "/")

        target_file_path = self.target_path + "/" + normalize_url(url, keep_index=True).replace("http://", "")                    
        raw_cache_path = "scraper_cache/raw/" + normalize_url(url, keep_index=True).replace("http://", "")
        html_cache_path = "scraper_cache/html/" + normalize_url(url, keep_index=True).replace("http://", "")                   
        wayback_cache_path = "scraper_cache/way/" + normalize_url(url, keep_index=True).replace("http://", "")
        manual_cache_path = "scraper_cache/manual/" + normalize_url(url, keep_index=True).replace("http://", "")
        fail_cache_path = "scraper_cache/fail/" + normalize_url(url, keep_index=True).replace("http://", "")
        
        if not show_progress:
            logger.info(f"Downloading {url} to {target_file_path}...")
        pbar.set_description(url)

        is_html = False
        data = None

        if os.path.isdir(raw_cache_path):
            raw_cache_path += "/index.html"
        if os.path.isdir(html_cache_path):
            html_cache_path += "/index.html"
        if os.path.isdir(wayback_cache_path):
            wayback_cache_path += "/index.html"
        if os.path.isdir(manual_cache_path):
            manual_cache_path += "/index.html"
        if os.path.isdir(fail_cache_path):
            fail_cache_path += "/index.html"

        if os.path.exists(manual_cache_path):
            logger.info(f"Using MANUAL file {manual_cache_path}")
            is_html = False
            with open(manual_cache_path, "rb") as f:
                data = f.read()
        elif os.path.exists(raw_cache_path):
            is_html = False
            with open(raw_cache_path, "rb") as f:
                data = f.read()
        elif os.path.exists(html_cache_path):
            is_html = True
            with open(html_cache_path, "rb") as f:
                data = f.read()
        elif os.path.exists(wayback_cache_path):
            is_html = True
            with open(wayback_cache_path, "rb") as f:
                data = f.read()
        elif os.path.exists(fail_cache_path):
            logger.info(f"Could not download {url} (cached)")
            return False
        else:
            success = False
            r = None
            if not any(url.startswith(x) for x in defunct_websites):
                if wayback_date is None:
                    try:
                        r = proxy_session.get(url, stream=True, timeout=10, verify=False)
                    except Exception:
                        logger.error("Could not download - requests connection error")
                        create_dir_parent(fail_cache_path)
                        with open(fail_cache_path, "w") as f:
                            f.write("requests connection error")
                        return False
                    success = r.status_code == 200
                    if not success:
                        logger.info(f"Could not download {url} - code {r.status_code}")
            else:
                logger.info(f"Could not download {url} - defunct website")
                if wayback_date is None:
                    wayback_date = datetime.datetime(1997, 1, 1)
            if success:
                r.raw.decode_content = True
                try:
                    data = r.raw.read()
                except Exception:
                    logger.error("Could not download - requests error")
                    create_dir_parent(fail_cache_path)
                    with open(fail_cache_path, "w") as f:
                        f.write("requests error")
                    return False
                is_html = ("content-type" in r.headers) and ("html" in r.headers["content-type"])

                if is_html:
                    if has_no_extension(target_file_path) or is_root_page(url):
                        if not raw_cache_path.endswith("/index.html"):
                            raw_cache_path += "/index.html"
                        if not html_cache_path.endswith("/index.html"):
                            html_cache_path += "/index.html"
                        if not wayback_cache_path.endswith("/index.html"):
                            wayback_cache_path += "/index.html"
                        if not fail_cache_path.endswith("/index.html"):
                            fail_cache_path += "/index.html"

                cache_path = html_cache_path if is_html else raw_cache_path
                create_dir_parent(cache_path)
                with open(cache_path, "wb") as f:
                    f.write(data)
            else:
                if self.is_targetted_location(url) or (not follow_from):
                    data, is_html = db.get_http_contents_from_wayback(url, wayback_date, ignore_html=(not follow_from))
                    if data is not None:
                        logger.info("Found on Wayback, continuing...")
                        if is_html:
                            if has_no_extension(target_file_path) or is_root_page(url):
                                if not raw_cache_path.endswith("/index.html"):
                                    raw_cache_path += "/index.html"
                                if not html_cache_path.endswith("/index.html"):
                                    html_cache_path += "/index.html"
                                if not wayback_cache_path.endswith("/index.html"):
                                    wayback_cache_path += "/index.html"
                                if not fail_cache_path.endswith("/index.html"):
                                    fail_cache_path += "/index.html"
                        create_dir_parent(wayback_cache_path)
                        with open(wayback_cache_path, "wb") as f:
                            f.write(data)
                    else:
                        create_dir_parent(fail_cache_path)
                        with open(fail_cache_path, "w") as f:
                            if r is None:
                                f.write(f"defunct website + no wayback")
                            else:
                                f.write(f"code {r.status_code} + no wayback")
                        return False
                else:
                    create_dir_parent(fail_cache_path)
                    with open(fail_cache_path, "w") as f:
                        f.write(f"code {r.status_code}")
                    return False

        # Prevents recursive index.html -> usemap -> index.html bug
        self.urls_scraped[url] = True
        if is_html:
            if os.path.isdir(target_file_path) or has_no_extension(target_file_path) or is_root_page(url):
                url += "/index.html"
                target_file_path += "/index.html"
            # html file
            html = BeautifulSoup(data, "html5lib")

            # remove <BASE>
            for el in html.select("base"):
                el.decompose()

            # follow links
            for follow_to_link, follow_should, followed_link_modify in self.find_all_links(html):
                if follow_to_link.startswith("https://"):
                    follow_to_link = follow_to_link.replace("https://", "http://")

                if (follow_to_link.lower().startswith("www.") or follow_to_link.lower().startswith("members.tripod.com")) and (("/" in follow_to_link) or (".htm" not in follow_to_link.lower())):
                    follow_to_link = "http://" + follow_to_link
                    followed_link_modify.modify(follow_to_link)
                if follow_to_link.startswith("/"):
                    follow_to_link = normalize_url(urljoin(url, follow_to_link))
                    follow_to_queue_link = follow_to_link
                    followed_link_modify.modify(follow_to_link)
                else:
                    follow_to_queue_link = normalize_url(urljoin(url, follow_to_link))
                # if follow_from AND (we're on the same prefix OR we don't follow further), queue the URL
                # `-> if follow_should, follow the URL
                # `-> this also means patching up the element
                # (not follow_should) or 
                if follow_from and ((not follow_should) or self.is_targetted_location(follow_to_queue_link)):
                    url_follow = follow_should
                    #logger.info(follow_to_queue_link)
                    if self.is_targetted_location(follow_to_queue_link):
                        if self.add_url_to_queue(follow_to_queue_link, url_follow):
                            pbar.total += 1
                        success = True
                    else:
                        pbar.total += 1
                        success = self._scrape_immediate(follow_to_queue_link, url_follow, pbar, show_progress, wayback_date)

                    if success and follow_to_link.startswith("http://"):
                        #logger.info(target_file_path)
                        #logger.info("steps_to_descend from: " + normalize_url(url, keep_index=True).replace("http://", "").replace("https://", ""))
                        steps_to_descend = len(normalize_url(url, keep_index=True).replace("http://", "").replace("https://", "").split("/")) - 1
                        follow_to_relative = ("../" * steps_to_descend) + normalize_url(follow_to_link).replace("http://", "")
                        #logger.info(follow_to_relative)
                        followed_link_modify.modify(follow_to_relative)
                        #time.sleep(0.25)

            create_dir_parent(target_file_path)
            # logger.info("saving html " + target_file_path + " " + str(follow_from))
            with open(target_file_path, "w") as f:
                f.write(f"<!-- Archived by AniPyke on {datetime.datetime.now().isoformat()} from {url} -->")
                f.write(str(html))
        else:
            # logger.info("saving non-html " + target_file_path)
            # binary file
            create_dir_parent(target_file_path)
            with open(target_file_path, "wb") as f:
                f.write(data)
        return True

    def scrape2(self, show_progress, wayback_date):
        pbar = tqdm(total=len(self.urls_queue), unit="page", disable=not show_progress)

        while len(self.urls_queue) > 0:
            url = self.urls_queue[0][0]
            follow_from = self.urls_queue[0][1]
            self.urls_queue = self.urls_queue[1:]
            self._scrape_immediate(url, follow_from, pbar, show_progress, wayback_date)

    def scrape(self, show_progress=True, wayback_date=None):
        self.scrape2(show_progress, wayback_date)

        # warc_path = "warcs/" + datetime.datetime.now().isoformat() + ".warc.gz"
        # create_dir_parent(warc_path)
        # http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')
        # record = writer.create_warc_record(url, 'response', payload=io.BytesIO(data), http_headers=http_headers)
        # with open(warc_path, 'wb') as output:
        # writer = WARCWriter(output, gzip=True)
                


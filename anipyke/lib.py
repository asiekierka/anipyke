from bs4 import BeautifulSoup
import datetime
import logging
import os
import re
import shutil
import sys
import url_normalize

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG if (os.environ.get("DEBUG") is not None) and (len(os.environ.get("DEBUG")) > 0) else logging.INFO)
logger = logging.getLogger('')
rootPaths = ["manual_websites/", "websites/"]

def filepath_to_anipike_path(filePath):
    if "~anipike/" in filePath:
        filePath = filePath[filePath.index("~anipike/") + 9:]
    elif "anipike.com/" in filePath:
        filePath = filePath[filePath.index("anipike.com/") + 12:]
    else:
        return None
    if filePath[0:4].isnumeric():
        filePath = filePath.split("/", maxsplit=1)[1]
    if filePath == "index.htm":
        filePath = "index.html"
    return filePath

def ext_is_html(fileName):
    return fileName.lower().endswith(".htm") or fileName.lower().endswith(".html")

def list_anipike_pages(filter, html_only=True):
    #for i in range(1, len(sys.argv)):
    #    rootPath = sys.argv[i]
    for rootPath in rootPaths:
        logger.info(f"scanning {rootPath}")
        for parentPath, dirNames, fileNames in os.walk(rootPath):
            if "anipike" not in parentPath:
                continue
            for fileName in fileNames:
                if html_only and not ext_is_html(fileName):
                    continue
                filePath = os.path.join(parentPath, fileName)
                if (filter is not None) and (not filter(filePath)):
                    continue
                yield filePath

anipike_page_date_re = re.compile("Last Update: ([0-9]+)/([0-9]+)/([0-9]+)")

def get_anipike_page_date(html):
    text = html.get_text()
    match = anipike_page_date_re.search(text)
    if match is None:
        return None
    m_year = int(match.group(3))
    if m_year < 70:
        m_year += 2000
    elif m_year < 100:
        m_year += 1900
    m_month = int(match.group(1))
    m_day = int(match.group(2))
    m_date = datetime.date(m_year, m_month, m_day)
    logger.debug(f"anipike date is {m_date}")
    return m_date

def map_html_urls(html, mapper, deleter=None):
    for el in html.select("a"):
        if el.attrs is None:
            continue
        if "href" in el.attrs:
            el.attrs["href"] = mapper(el.attrs["href"])
            if (el.attrs["href"] is None) and (deleter is not None):
                deleter(el)
    for el in html.select("img"):
        if el.attrs is None:
            continue
        if "src" in el.attrs:
            el.attrs["src"] = mapper(el.attrs["src"])
            if (el.attrs["src"] is None) and (deleter is not None):
                deleter(el)

def is_anipike_subpage(html):
    a_elements = html.select("a")
    return (len(a_elements) > 0) and ("href" in a_elements[0].attrs) and a_elements[0].attrs["href"].startswith("../")

def read_html(filePath):
    logger.debug(f"reading {filePath}")
    with open(filePath, "rb") as f:
        data = f.read()
    html = BeautifulSoup(data, 'lxml', from_encoding='windows-1252')
    return html

def remove_index_url(url):
    if url.lower().endswith("/index.html") or url.lower().endswith("/index.htm"):
        url = url[:url.rindex("/")]
    return url

def remove_html_last_url(url):
    if url.lower().endswith(".html") or url.lower().endswith(".htm"):
        url = url[:url.rindex("/")]
    return url

def normalize_url(url, keep_index=False):
    if not keep_index:
        url = remove_index_url(url)
    res = url_normalize.url_normalize(url).replace("https://", "http://")
    if res.endswith("/"):
        res = res[:-1]
    return res

def patch_file(path, userdata, patcher):
    orig_path = path + ".origanipyke"
    if not os.path.exists(orig_path):
        shutil.move(path, orig_path)
    else:
        with open(orig_path, "rb") as f:
            data = f.read()
        data = patcher(data, userdata)
        with open(path, "wb") as f:
            f.write(data)

def add_html_meta_utf8(html):
    new_meta = html.new_tag("meta")
    new_meta.attrs["charset"] = "utf-8"
    html.head.append(new_meta)
        
def get_url_variants(url):
    url_variants = [url]
    if url.startswith("http://www."):
        url_variants.append(url.replace("http://www.", "http://"))
    else:
        if len(url[url.index("://")+3:].split("/", maxsplit=1)[0].split(".")) <= 2:
            url_variants.append(url.replace("http://", "http://www."))
    return url_variants

def create_dir_parent(path):
    try:
        os.makedirs(os.path.dirname(path))
    except FileExistsError:
        pass
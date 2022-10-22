from anipyke.lib import *
from bs4 import BeautifulSoup, Comment
from urllib.parse import urljoin
import anipyke.db as db
import copy
import logging
import os
import re
import shutil
import sqlalchemy
import sys

def patch_url(url_href, userdata, tag):
    if userdata["url"].startswith("mailto:"):
        return None

    home_url = remove_html_last_url(normalize_url(userdata["url"]))
    # trim home_url to the longest common component
    while True:
        home_url_rsplit = home_url.rsplit("/", maxsplit=1)
        if (len(home_url_rsplit) == 2) and (url_href.endswith(f"/{home_url_rsplit[1]}")):
            url_href = url_href[:-len(home_url_rsplit[1])-1]
            home_url = home_url[:-len(home_url_rsplit[1])-1]
        else:
            break

    home_url_variants = get_url_variants(home_url)
    if url_href.startswith("/"):
        url_href = urljoin(home_url, url_href)
    for uv in home_url_variants:
        print(f"{uv} {url_href}")
        if url_href.startswith(uv):
            url_href = url_href[len(uv):]
            if url_href.startswith("/"):
                url_href = url_href[1:]
            print(f"patched absolute url {tag} -> {url_href}")
            return url_href
    return None
def patch_html_links(html, userdata):
    for el in html.select("a"):
        if (el.attrs is not None) and ("href" in el.attrs):
            url_href = patch_url(el.attrs["href"], userdata, "a")
            if url_href is not None:
                el.attrs["href"] = url_href

    for el in html.select("img"):
        if (el.attrs is not None) and ("src" in el.attrs):
            url_href = patch_url(el.attrs["src"], userdata, "img")
            if url_href is not None:
                el.attrs["src"] = url_href


def patch_html_angelfire(data, userdata):
    html = BeautifulSoup(data, 'lxml')

    patch_html_links(html, userdata)

    s = list(filter(lambda x: (x.attrs is not None) and ("src" in x.attrs) and ("udmserve.net" in x.attrs["src"]), html.select('script')))
    if len(s) == 1:
        x = s[0]
        while x is not None:
            xx = x.previous_element
            if x.name == "body":
                break
            if hasattr(x, "decompose"):
                x.decompose()
            x = xx
    else:
        pass
        # raise Exception("angelfire-unpatched html??")



    add_html_meta_utf8(html)
    return str(html).encode('utf-8')

logger.info(f"=== Patching AngelFire websites ===")

for rootPath in ["./new_websites/www.angelfire.com/", "./new_websites/angelfire.com/"]:
    for parentPath, dirNames, fileNames in os.walk(rootPath):
        for fileName in fileNames:
            if fileName.endswith(".origanipyke"):
                fileName = fileName[:-12]
            if ext_is_html(fileName):
                full_path = os.path.join(parentPath, fileName)
                url = full_path.split("/", maxsplit=2)[-1]
                logger.info(f"patching {url}")
                patch_file(full_path, {"url":url}, patch_html_angelfire)
from anipyke.lib import *
import anipyke.db as db
import copy
import logging
import os
import re
import sqlalchemy
import sys

# Read all index files to figure out all valid subpage filenames
subpage_filenames = {}

logger.info("=== Indexing subpages ===")

def process_subpages_from(filePath, mapper):
    global subpage_filenames
    try:
        html = read_html(filePath)
        if (html.select("a")[0].attrs["href"].startswith("../")):
            # this is not a root file!
            return
        page_date = get_anipike_page_date(html)
        for el in mapper(html):
            if "href" in el.attrs:
                subpage = el.attrs["href"]
                if (subpage.startswith("#")):
                    continue
                if ("#" in subpage):
                    subpage = subpage[0:subpage.index("#")]
                if ("/" in subpage) or (":" in subpage) or ("www.ayacon.org.uk" in subpage) or ("@" in subpage):
                    continue
                if ("index.htm" in subpage):
                    continue
                if ("." not in subpage):
                    continue
                if subpage in ["text.html", "broken.html", "guestpick.html", "aniform.html"]:
                    continue
                if subpage.endswith("-txt.html"):
                    continue
                if subpage not in subpage_filenames:
                    logging.info(f"Found {subpage}, first in {filePath}")
                    subpage_filenames[subpage] = db.AnipikeSubpage()
                    subpage_filenames[subpage].subpage = subpage
                if page_date is not None:
                    subpage_filenames[subpage].interval.add(page_date)
    except Exception as e:
        logger.debug(e)
        return

for filePath in list_anipike_pages(lambda x: "index.htm" in x):
    process_subpages_from(filePath, lambda html: html.select("table")[1].select("a"))

prev_len_subpage = 0
new_subpage_filenames = []
while prev_len_subpage != len(subpage_filenames):
    logger.info(f"Expanded subpage list from {prev_len_subpage} to {len(subpage_filenames)}")
    prev_len_subpage = len(subpage_filenames)
    new_subpage_filenames = list(subpage_filenames.keys())
    for filePath in list_anipike_pages(lambda x: ("index.htm" not in x) and (filepath_to_anipike_path(x) in new_subpage_filenames)):
        process_subpages_from(filePath, lambda html: html.select("a"))
    new_subpage_filenames = list(filter(lambda x: x not in new_subpage_filenames, subpage_filenames))

# Parse every single subpage...
logger.info(subpage_filenames.keys())

with db.new_session() as session:
    session.execute(
        sqlalchemy.delete(db.AnipikeSubpage)
    )
    for subpage in subpage_filenames.values():
        session.add(subpage)
    session.commit()

# logger.info("=== Indexing linked pages ===")
#
# for filePath in list_anipike_files(lambda x: x.split("/")[-1] in subpage_filenames):
#     html = read_html(filePath)
#     for el_ul in html.select("ul"):
#         for el in el_ul.select("a"):
#             print(el)
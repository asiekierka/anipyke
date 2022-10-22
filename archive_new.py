from anipyke.lib import *
from anipyke.scraper import WebScraper
import anipyke.db as db
import copy
import logging
import os
import re
import shutil
import sqlalchemy
import subprocess
import sys

args = sys.argv
no_create = True

logger.info(f"=== Archiving webpage ===")

with db.new_session() as session:
    url = normalize_url(args[1])
    if url.endswith("/"):
        url = url[:-1]
    url_queue = [url]

    for url_found in session.execute(
        sqlalchemy.select(db.AnipikeWebsite)
            .where(db.AnipikeWebsite.link_normalized.startswith(url))
    ).scalars():
        url_proposed = url_found.link_normalized
        if url_proposed.endswith("/"):
            url_proposed = url_proposed[:-1]
        if url_proposed not in url_queue:
            url_queue.append(url_proposed)
    
    date_str = datetime.datetime.now().date().strftime("%Y%m%d")
    scraper = WebScraper(url_queue, f"new_websites/{date_str}")
    scraper.scrape()

    for url in url_queue:
        url_location = db.UrlLocation()
        url_location.url = url
        url_location.prefix = remove_index_url(url).replace("http://", "")
        url_location.source = "new"
        url_location.subkey = scraper.target_path
        url_location.date = datetime.datetime.now().date()
        url_location.add_date = datetime.datetime.now()
        url_location.local_path = scraper.target_path
        session.add(url_location)
    session.commit()

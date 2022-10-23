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

urls_scraped = {}
job_id = datetime.datetime.now().isoformat()

def archive(url, session, create_locations=True, local_path=None):
    url = normalize_url(url)
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
    
    if local_path is None:
        date_str = datetime.datetime.now().date().strftime("%Y%m%d")
        local_path = f"new_websites/{date_str}"
    scraper = WebScraper(url_queue, local_path, urls_scraped=urls_scraped)
    scraper.scrape()

    if create_locations:
        for url in url_queue:
            url_location = db.UrlLocation()
            url_location.url = url
            url_location.prefix = remove_index_url(url).replace("http://", "")
            url_location.source = "new"
            url_location.subkey = scraper.target_path
            url_location.date = datetime.datetime.now().date()
            url_location.add_date = datetime.datetime.now()
            url_location.local_path = scraper.target_path
            url_location.job_id = job_id
            session.add(url_location)
        session.commit()


with db.new_session() as session:
    if args[1] == "rebuild":
        job_ids = {}

        for url_found in session.execute(
            sqlalchemy.select(db.UrlLocation)
                .where(sqlalchemy.or_(db.UrlLocation.local_path.in_(args[2:]), db.UrlLocation.job_id.in_(args[2:])))
        ).scalars():
            if (url_found.job_id is not None) and (url_found.job_id in job_ids):
                continue
            archive(url_found.url, session, create_locations=False, local_path=url_found.local_path)
            job_ids[url_found.job_id] = True
    else:
        archive(args[1], session)
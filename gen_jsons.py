from anipyke.lib import *
from anipyke.scraper import WebScraper
import anipyke.db as db
import copy
import json
import logging
import os
import re
import shutil
import sqlalchemy
import subprocess
import sys

args = sys.argv
no_create = True

logger.info(f"=== Generating JSONs ===")

with db.new_session() as session:
    websites = {}

    for url_found in session.execute(
        sqlalchemy.select(db.AnipikeWebsite)
    ).scalars():
        websites[url_found.link] = {
            "url_normalized": url_found.link_normalized,
            "from": url_found.interval.from_date.strftime("%Y-%m-%d"),
            "to": url_found.interval.to_date.strftime("%Y-%m-%d")
        }

    with open("anipike_websites.json", "w") as f:
        json.dump(websites, f)

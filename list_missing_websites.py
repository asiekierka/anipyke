from anipyke.lib import *
import anipyke.db as db
import copy
import datetime
import logging
import os
import re
import shutil
import sqlalchemy
import subprocess
import sys

date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')

sync_pairs = {}
missing_websites = 0
all_websites = 0

with db.new_session() as session:
    for website in session.execute(
        sqlalchemy.select(db.AnipikeWebsite)
            .where(sqlalchemy.and_(db.AnipikeWebsite.from_date <= date, db.AnipikeWebsite.to_date >= date))
    ).scalars():
        if "://" not in website.link:
            continue
        url = db.get_archived_urls(website.link, None)
        all_websites += 1
        if len(url) <= 0:
            print(website.link)
            missing_websites += 1

logging.info(f"Preserved {all_websites - missing_websites}/{all_websites} websites for {sys.argv[1]}")
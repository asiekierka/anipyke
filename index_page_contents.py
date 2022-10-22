from anipyke.lib import *
import anipyke.db as db
import copy
import logging
import os
import re
import sqlalchemy
import sys

logger.info("=== Indexing page contents and webpages ===")

subpage_date_pair = {}
websites = {}

with db.new_session() as session:
    session.execute(
        sqlalchemy.delete(db.AnipikePageContents)
    )
    session.execute(
        sqlalchemy.delete(db.AnipikeWebsite)
    )
    for filePath in list_anipike_pages(lambda x: True):
        aniPath = filepath_to_anipike_path(filePath)
        if aniPath is None:
            continue
        html = read_html(filePath)
        aniDate = get_anipike_page_date(html)
        if aniDate is None:
            continue
        obj = db.AnipikePageContents()
        obj.subpage = aniPath
        obj.date = aniDate

        for el in html.select("a"):
            if el.attrs is None:
                continue
            if "href" in el.attrs:
                link = el.attrs["href"]
                if link not in websites:
                    websites[link] = db.AnipikeWebsite()
                    websites[link].link = link
                    websites[link].link_normalized = normalize_url(link, keep_index=True)
                    websites[link].link_name = el.get_text(strip=True)
                websites[link].interval.add(aniDate)

        obj.contents = str(html)
        #if aniPath == "index.html":
            # index files
        #else:
            # subpage files

        if obj.contents is not None:
            sdp_key = f"{obj.subpage}_{obj.date}"
            if sdp_key not in subpage_date_pair:
                session.add(obj)
                subpage_date_pair[sdp_key] = obj
            else:
                other = subpage_date_pair[sdp_key]
                if obj.contents != other.contents:
                    logger.warning(f"Conflicting data at {sdp_key}")

    for i in websites.keys():
        session.add(websites[i])
    session.commit()

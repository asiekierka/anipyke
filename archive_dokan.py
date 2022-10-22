from anipyke.lib import *
import anipyke.db as db
import copy
import logging
import os
import re
import shutil
import sqlalchemy
import sys

logger.info(f"=== Archiving Dokan webpages ===")

with db.new_session() as session:
    # for user_obj in session.execute(sqlalchemy.select(db.UrlLocation).where(db.UrlLocation.source == "dokan")).scalars():
    #     p = user_obj.to_root_path()
    #     if (not p.startswith("arch/")) or (".." in p):
    #         raise Exception("!?")
    #     try:
    #         shutil.rmtree(p)
    #     except Exception:
    #         pass
    session.execute(sqlalchemy.delete(db.UrlLocation).where(db.UrlLocation.source == "dokan"))

    # List all Dokan directories
    for d in filter(lambda x: x.lower().endswith("webs"), os.listdir("./manual_websites/")):
        dokan_path = f"manual_websites/{d}"
        if os.path.isdir(dokan_path):
            url_path = None
            for d in filter(lambda x: x.lower() == "webs.ini", os.listdir(dokan_path)):
                ini_filename = f"{dokan_path}/{d}"
                with open(ini_filename, "rb") as f:
                    ini_data = f.read().decode("windows-1252")
                url_location = db.UrlLocation()
                for l in ini_data.splitlines():
                    if "=" not in l:
                        continue
                    if "*." in l:
                        continue
                    if "." not in l:
                        continue
                    if l.endswith(".exe"):
                        continue
                    value = l.split("=", maxsplit=1)[1].strip()
                    value = re.sub("\\\\", "/", value)
                    if value.startswith("www.anipike.com"):
                        continue
                    if ("piscina2" in value.lower()):
                        continue
                    value_url = normalize_url(value)
                    print(value_url)
                    url_location = db.UrlLocation()
                    url_location.url = value_url
                    url_location.prefix = remove_index_url(value)
                    url_location.source = "dokan"
                    url_location.subkey = dokan_path
                    url_location.date = datetime.date.fromtimestamp(os.path.getmtime(ini_filename))
                    url_location.local_path = dokan_path
                    session.add(url_location)
                    url_path = url_location.to_root_path()
            # if url_path is not None:
            #     shutil.copytree(dokan_path, url_path)

    session.commit()

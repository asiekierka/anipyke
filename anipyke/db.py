from sqlalchemy import *
from sqlalchemy.ext.mutable import MutableComposite
from sqlalchemy.orm import *
from waybackpy import WaybackMachineCDXServerAPI
from .lib import *
import dataclasses
import datetime
import hashlib
import os
import requests
import time

engine = create_engine("sqlite+pysqlite:///anipyke.db?timeout=300", echo=False)

def new_session():
    return Session(engine)

class Base(DeclarativeBase):
    pass

@dataclasses.dataclass
class Interval(MutableComposite):
    from_date: datetime.date
    to_date: datetime.date

    def __repr__(self):
        return f"Interval({self.from_date} - {self.to_date})"

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        self.changed()

    def add(self, date):
        if date is None:
            raise Exception("Tried to add None date!")
        if (self.from_date is None) or (self.from_date > date):
            self.from_date = date
        if (self.to_date is None) or (self.to_date < date):
            self.to_date = date
        self.changed()

class AnipikePageContents(Base):
    __tablename__ = "anipike_page_contents"

    subpage: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    date: Mapped[datetime.date] = mapped_column(primary_key=True, nullable=False)
    contents: Mapped[str] = mapped_column(Text)

class AnipikeSubpage(Base):
    __tablename__ = "anipike_subpage"

    subpage: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    interval: Mapped[Interval] = composite(mapped_column("from_date"), mapped_column("to_date"))

    def __init__(self):
        self.interval = Interval(None, None)

    def __repr__(self):
        return f"AnipikeSubpage({self.subpage!r} @ {self.interval!r})"

class AnipikeSubpageStructure(Base):
    __tablename__ = "anipike_subpage_structure"
    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    subpage: Mapped[str] = mapped_column(nullable=False)
    header1: Mapped[str] = mapped_column(nullable=True)
    header2: Mapped[str] = mapped_column(nullable=True)
    header3: Mapped[str] = mapped_column(nullable=True)
    order: Mapped[int] = mapped_column(nullable=False)
    interval: Mapped[Interval] = composite(mapped_column("from_date"), mapped_column("to_date"))

class AnipikeWebpage(Base):
    __tablename__ = "anipike_webpage"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    subpage: Mapped[str] = mapped_column(nullable=False)
    link: Mapped[str] = mapped_column(nullable=True)
    link_name: Mapped[str] = mapped_column(nullable=True)
    header1: Mapped[str] = mapped_column(nullable=True)
    header2: Mapped[str] = mapped_column(nullable=True)
    header3: Mapped[str] = mapped_column(nullable=True)
    interval: Mapped[Interval] = composite(mapped_column("from_date"), mapped_column("to_date"))

    def __init__(self):
        self.interval = Interval(None, None)

class AnipikeWebsite(Base):
    __tablename__ = "anipike_website"

    link: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    link_normalized: Mapped[str] = mapped_column(nullable=True)
    interval: Mapped[Interval] = composite(mapped_column("from_date"), mapped_column("to_date"))
    link_name: Mapped[str] = mapped_column(nullable=True)

    def __init__(self):
        self.interval = Interval(None, None)

class UrlLocation(Base):
    __tablename__ = "url_location"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    url: Mapped[str] = mapped_column(nullable=False)
    date: Mapped[datetime.date] = mapped_column(Date)
    source: Mapped[str] = mapped_column()
    subkey: Mapped[str] = mapped_column()
    prefix: Mapped[str] = mapped_column()
    local_path: Mapped[str] = mapped_column(nullable=True)
    remote_path: Mapped[str] = mapped_column(nullable=True)
    add_date: Mapped[datetime.datetime] = mapped_column()
    job_id: Mapped[str] = mapped_column(nullable=True)

    def to_root_path(self):
        date_key = self.date.strftime("%Y-%m-%d")
        full_key = f"{date_key}_{self.source}_{self.subkey}"
        hash_key = hashlib.md5(full_key.encode("utf-8")).hexdigest()

        return f"arch/{self.date.year}/{hash_key[0:2]}/{hash_key[2:]}"

    def to_url_path(self):
        return f"{self.to_root_path()}/{self.prefix}"

    def to_local_path(self):
        return f"{self.local_path}/{self.prefix}"

    def __init__(self):
        self.interval = Interval(None, None)

class WaybackCache(Base):
    __tablename__ = "wayback_cache"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column()
    date: Mapped[datetime.datetime] = mapped_column(nullable=True)
    contents: Mapped[bytes] = mapped_column(LargeBinary, nullable=True)
    mimetype: Mapped[str] = mapped_column(nullable=True)

skip_wayback_websites = [
    "http://members.tripod.com/GeneralsLove", # none on wayback, 100+ missing TXT files, takes ages
    "http://members.tripod.com/~K_Seiya",
    "http://members.tripod.com/~Kuno_Higatashi2"
]

def get_http_contents_from_wayback(url, date, ignore_html=False):
    if any(url.startswith(x) for x in skip_wayback_websites):
        return None, False
    with new_session() as session:
        cache_entries = []
        for e in session.execute(
            select(WaybackCache)
                .where(WaybackCache.url == url)
        ).scalars():
            cache_entries.append(e)
        if len(cache_entries) <= 0:
            user_agent = "Mozilla/5.0 (X11; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/131.0"
            cached_keys = {}
            retries = 7
            retry_sleep = 15
            while retries > 0:
                try:
                    cdx = WaybackMachineCDXServerAPI(url, user_agent, max_tries=1, filters=["statuscode:200"], collapses=["digest"])
                    for snapshot in cdx.snapshots():
                        if snapshot.timestamp in cached_keys:
                            continue
                        cached_keys[snapshot.timestamp] = True
                        archive_url = f"https://web.archive.org/web/{snapshot.timestamp}id_/{snapshot.original}"
                        try:
                            time.sleep(20)
                            r = requests.get(archive_url, stream=True, timeout=60)
                        except Exception:
                            logger.error(f"Could not download {archive_url} - requests connection error")
                            continue
                        logger.info(f"Found on Wayback: {archive_url}")
                        if r.status_code == 200:
                            r.raw.decode_content = True
                            try:
                                data = r.raw.read()
                            except Exception:
                                logger.error(f"Could not download {archive_url} - requests error")
                                continue
                            entry = WaybackCache()
                            entry.url = url
                            entry.date = snapshot.datetime_timestamp
                            entry.mimetype = snapshot.mimetype
                            entry.contents = data
                            cache_entries.append(entry)
                    retries = 0
                except requests.exceptions.RetryError as exc:
                    retries -= 1
                    if retries <= 0:
                        raise exc
                    logger.info(f"Overburdening Wayback, backing off for {retry_sleep} seconds...")
                    time.sleep(retry_sleep)
                    retry_sleep = int(retry_sleep * 1.5)
            if len(cache_entries) <= 0:
                entry = WaybackCache()
                entry.url = url
                cache_entries.append(entry)
            
            for c in cache_entries:
                session.add(c)
            session.commit()
                
        cache_entries.sort(key=lambda x: [0 if ((x.mimetype is None) or x.mimetype.startswith("unk")) else 1, x.date], reverse=True)

        contents = None
        mimetype = None
        date_diff = None
        for c in cache_entries:
            if ignore_html and (c.mimetype is not None) and ("html" in c.mimetype):
                continue
            if contents is None:
                contents = c.contents
                mimetype = c.mimetype
            if (date is None) and (contents is not None):
                break
            if (c.date is not None) and (date is not None) and ((date_diff is None) or (abs(c.date - date) < date_diff)):
                contents = c.contents
                mimetype = c.mimetype
                date_diff = abs(c.date - date)
        return contents, ((mimetype is not None) and ("html" in mimetype))

def get_latest_anipike_file(url, date):
    if url.endswith(".htm") or url.endswith(".html") or ("." not in url):
        if ("." not in url):
            if not url.endswith("/"):
                url += "/"
            url += "index.html"
        # load html from database
        with new_session() as session:
            result = None
            if date is not None:
                result = session.execute(
                    select(AnipikePageContents)
                        .where(and_(AnipikePageContents.subpage == url, AnipikePageContents.date <= date))
                        .order_by(AnipikePageContents.date.desc())
                        .limit(1)
                ).scalar_one_or_none()
            if result is None:
                # get a newer file, if older or equal does not exist
                result = session.execute(
                    select(AnipikePageContents)
                        .where(AnipikePageContents.subpage == url)
                        .order_by(AnipikePageContents.date)
                        .limit(1)
                ).scalar_one_or_none()
            if result is not None:
                return result.contents
    else:
        # copy file from static/
        if os.path.exists("static/" + url):
            with open("static/" + url, "rb") as f:
                return f.read()
    # file not found
    return None

def url_location_to_url(url, result):
    url_suffix = "" if url is None else url[len(result.url):]
    if url_suffix.startswith("/"):
        url_suffix = url_suffix[1:]
    url_prefix = result.to_url_path()
    if (not url_prefix.endswith("/")) and (len(url_suffix) > 0):
        url_prefix += "/"
    local_url_prefix = result.to_local_path()
    if (not local_url_prefix.endswith("/")) and (len(url_suffix) > 0):
        local_url_prefix += "/"
    if os.path.exists(local_url_prefix + url_suffix):
        return "/" + url_prefix + url_suffix
    else:
        return None

still_online_urls = {
    "http://otakuworld.com": "http://otakuworld.com",
    "http://www.otakuworld.com": "http://www.otakuworld.com",
    "http://niko-niko.net": "http://niko-niko.net",
    "http://www.therossman.com": "http://www.therossman.com",
    "!http://cnn.com/WORLD/9712/17/video.seizures.update ": "http://cnn.com/WORLD/9712/17/video.seizures.update/index.html",
    "!http://theria.net/slayers": "http://www.theria.net/index.html",
    "!http://theria.net/slayers/index.html": "http://www.theria.net/index.html",
    "!http://theria.net/yaminomatsuei": "http://www.theria.net/archive-top.html",
    "!http://theria.net/yst": "http://www.theria.net/archive-top.html",
    "!http://theria.net/yst/archive/index.html#lyrics": "http://www.theria.net/yst-lyrics.html",
    "http://digilander.iol.it/haranban": "http://www.tvcartoonmania.com",
    "http://hp.vector.co.jp/authors/VA008023": "http://hp.vector.co.jp/authors/VA008023"
}

def get_archived_urls(url, date):
    # short-circuit a few sites
    if "otakuworld.com/guide" not in url:
        for x in still_online_urls.keys():
            if x.startswith("!"):
                if url == x[1:]:
                    logger.info(f"Using manual replacement for {x[1:]}")
                    return [still_online_urls[x]], ["online"]
            if url.startswith(x):
                return [url.replace(x, still_online_urls[x])], ["online"]
    # load html from database
    with new_session() as session:
        result = None
        url_variants = list(map(lambda x: x if x.endswith("/") else (x+"/"), get_url_variants(url)))
        finds = []
        for uv in url_variants:
            for result in session.execute(
                select(UrlLocation)
                    .where(bindparam('url', uv).startswith(UrlLocation.url))
            ).scalars():
                finds.append((url_location_to_url(url, result), result.date, result.source))
        finds = list(filter(lambda x: x[0] is not None, finds))
        if date is not None:
            finds_prev = list(filter(lambda x: x[1] <= date, finds))
            finds_next = list(filter(lambda x: x[1] > date, finds))
            finds_prev.sort(key=lambda x: x[1], reverse=True)
            finds_next.sort(key=lambda x: x[1], reverse=False)
            finds = finds_prev + finds_next
        else:
            finds.sort(key=lambda x: x[1], reverse=False)
        return list(map(lambda x: x[0], finds)), list(map(lambda x: x[2], finds))

Base.metadata.create_all(engine)

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

def get_http_contents_from_wayback(url, date, ignore_html=False):
    with new_session() as session:
        cache_entries = []
        for e in session.execute(
            select(WaybackCache)
                .where(WaybackCache.url == url)
        ).scalars():
            cache_entries.append(e)
        if len(cache_entries) <= 0:
            user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
            cdx = WaybackMachineCDXServerAPI(url, user_agent, filters=["statuscode:200"], collapses=["digest"])
            cached_keys = {}
            for snapshot in cdx.snapshots():
                if snapshot.timestamp in cached_keys:
                    continue
                cached_keys[snapshot.timestamp] = True
                try:
                    r = requests.get(snapshot.archive_url, stream=True, timeout=60)
                except Exception:
                    logger.error(f"Could not download {snapshot.archive_url} - requests connection error")
                    continue
                logger.info(f"Found on Wayback: {snapshot.archive_url}")
                if r.status_code == 200:
                    r.raw.decode_content = True
                    try:
                        data = r.raw.read()
                    except Exception:
                        logger.error(f"Could not download {snapshot.archive_url} - requests error")
                        continue
                    entry = WaybackCache()
                    entry.url = url
                    entry.date = snapshot.datetime_timestamp
                    entry.mimetype = snapshot.mimetype
                    entry.contents = data
                    cache_entries.append(entry)
            time.sleep(5) # or else we get 503'd sometimes

            if len(cache_entries) <= 0:
                entry = WaybackCache()
                entry.url = url
                cache_entries.append(entry)
            
            for c in cache_entries:
                session.add(c)
            session.commit()
                
        cache_entries.sort(key=lambda x: [0 if ((x.mimetype is None) or x.mimetype.startswith("unk")) else 1, x.date], reverse=True)

        contents = None
        date_diff = None
        for c in cache_entries:
            if ignore_html and (c.mimetype is not None) and ("html" in c.mimetype):
                continue
            if contents is None:
                contents = c.contents
            if (date is None) and (contents is not None):
                break
            if (c.date is not None) and (date is not None) and ((date_diff is None) or (abs(c.date - date) < date_diff)):
                contents = c.contents
                date_diff = abs(c.date - date)
        return contents

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

def get_archived_urls(url, date):
    # load html from database
    with new_session() as session:
        result = None
        url_variants = list(map(lambda x: x if x.endswith("/") else (x+"/"), get_url_variants(url)))
        if "fredart" in url:
            print(url_variants)
        finds = []
        if date is not None:
            for uv in url_variants:
                for result in session.execute(
                    select(UrlLocation)
                        .where(and_(bindparam('url', uv).startswith(UrlLocation.url), UrlLocation.date <= date))
                        .order_by(UrlLocation.date.desc())
                ).scalars():
                    finds.append(url_location_to_url(url, result))
            for uv in url_variants:
                for result in session.execute(
                    select(UrlLocation)
                        .where(and_(bindparam('url', uv).startswith(UrlLocation.url), UrlLocation.date > date))
                        .order_by(UrlLocation.date)
                ).scalars():
                    finds.append(url_location_to_url(url, result))
        else:
            for uv in url_variants:
                # get a newer file, if older or equal does not exist
                for result in session.execute(
                    select(UrlLocation)
                        .where(bindparam('url', uv).startswith(UrlLocation.url))
                        .order_by(UrlLocation.date)
                ).scalars():
                    finds.append(url_location_to_url(url, result))
    return list(filter(lambda x: x is not None, finds))

Base.metadata.create_all(engine)

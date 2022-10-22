from sqlalchemy import *
from sqlalchemy.ext.mutable import MutableComposite
from sqlalchemy.orm import *
from .lib import *
import dataclasses
import datetime
import hashlib
import os

engine = create_engine("sqlite+pysqlite:///anipyke.db", echo=False)

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

def get_archived_url(url, date):
    # load html from database
    with new_session() as session:
        result = None
        url_variants = get_url_variants(url)
        if date is not None:
            for url in url_variants:
                result = session.execute(
                    select(UrlLocation)
                        .where(and_(bindparam('url', url).startswith(UrlLocation.url), UrlLocation.date <= date))
                        .order_by(UrlLocation.date.desc())
                        .limit(1)
                ).scalar_one_or_none()
                if result is not None:
                    break
        if result is None:
            for url in url_variants:
                # get a newer file, if older or equal does not exist
                result = session.execute(
                    select(UrlLocation)
                        .where(bindparam('url', url).startswith(UrlLocation.url))
                        .order_by(UrlLocation.date)
                        .limit(1)
                ).scalar_one_or_none()
                if result is not None:
                    break
        if result is not None:
            url_suffix = url[len(result.url):]
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
    # file not found
    return None

Base.metadata.create_all(engine)

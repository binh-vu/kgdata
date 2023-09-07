"""Utilities for downloading knowledge graph dumps"""
from __future__ import annotations

import contextlib
import datetime
import os
import re
import subprocess
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Optional, cast
from urllib.parse import urlparse

import requests
from hugedict.sqlitedict import SqliteDict
from loguru import logger
from rsoup.core import Document
from tqdm.auto import tqdm


@dataclass
class DumpFile:
    date: datetime.date
    url: str


class BaseDumpCollection(ABC):
    @abstractmethod
    def get_main_category(self) -> str:
        ...

    def get_categories(self) -> dict[str, list[DumpFile]]:
        return {
            name: items
            for name, items in self.__dict__.items()
            if (isinstance(items, list) and all(isinstance(v, DumpFile) for v in items))
        }

    def list_dates(
        self,
        *others: BaseDumpCollection,
        other_score: float = 1.0,
        self_score: float = 1.0,
    ) -> tuple[list[datetime.date], dict[datetime.date, dict[str, dict]]]:
        """list dump dates of files in the main category, sorted by freshness and how close the other dump files are to it."""
        categories = self.get_categories()
        main_category = self.get_main_category()
        candidates = [file.date for file in categories[main_category]]

        explanation = {}
        for date in candidates:
            self_diff_days = sum(
                [
                    abs(date - min(items, key=lambda x: abs(date - x.date)).date).days
                    for name, items in categories.items()
                    if name != main_category
                ]
            ) / max(len(categories) - 1, 1)
            other_diff_days = [
                min(
                    abs(date - x.date).days
                    for x in other.get_categories()[other.get_main_category()]
                )
                * other_score
                for other in others
            ]
            explanation[date] = {
                "self": {
                    "score": self_diff_days * self_score,
                    "explanation": {
                        name: [
                            item.date.isoformat()
                            for item in sorted(items, key=lambda x: abs(date - x.date))[
                                :3
                            ]
                        ]
                        for name, items in categories.items()
                        if name != main_category
                    },
                },
                "others": {
                    "score": sum(other_diff_days) / max(1, len(other_diff_days)),
                    "explanation": {
                        other.__class__.__name__.lower(): [
                            x.date.isoformat()
                            for x in sorted(
                                other.get_categories()[other.get_main_category()],
                                key=lambda x: abs(date - x.date).days,
                            )[:3]
                        ]
                        for other in others
                    },
                },
                "total": self_diff_days * self_score
                + sum(other_diff_days) / max(1, len(other_diff_days)),
            }

        today = datetime.datetime.now().date()
        return (
            sorted(
                candidates, key=lambda date: (explanation[date]["total"], today - date)
            ),
            explanation,
        )

    def list_files(
        self, date: str | datetime.date, verbose: bool = False
    ) -> list[DumpFile]:
        if isinstance(date, str):
            date = datetime.date.fromisoformat(date)

        files = []
        for name, items in self.get_categories().items():
            items = sorted(items, key=lambda x: abs(x.date - date))
            if verbose:
                logger.info(
                    "{}: using a dump at {}. The next closest ones are {} and {}.",
                    name,
                    items[0].date.strftime("%Y-%m-%d"),
                    items[1].date.strftime("%Y-%m-%d"),
                    items[2].date.strftime("%Y-%m-%d"),
                )
            files.append(items[0])

        return files

    def create_download_jobs(
        self, files: list[DumpFile], rootdir: str | Path
    ) -> list[tuple[str, Path]]:
        rootdir = Path(rootdir)
        main_category_urls = {
            file.url for file in self.get_categories()[self.get_main_category()]
        }
        main_category_files = [file for file in files if file.url in main_category_urls]
        assert len(main_category_files) == 1, "Should have exactly one main file"

        dump_dir = rootdir / main_category_files[0].date.strftime("%Y%m%d") / "dumps"
        dump_dir.mkdir(parents=True, exist_ok=True)

        jobs = []
        for file in files:
            outfile = dump_dir / get_url_filename(file.url)
            jobs.append((file.url, outfile))
        return jobs

    @abstractmethod
    def fetch(self, url: str):
        """Fetch the dump collection from the given URL"""
        ...

    def parse_urls(self, urls: dict[str, re.Match]) -> list[DumpFile]:
        items = []
        for url, m in urls.items():
            date = m.group(1)
            item = DumpFile(
                date=datetime.date.fromisoformat(date),
                url=url,
            )
            items.append(item)
        return items


@dataclass
class WikidataDump(BaseDumpCollection):
    entities: list[DumpFile] = field(default_factory=list)
    page: list[DumpFile] = field(default_factory=list)
    redirect: list[DumpFile] = field(default_factory=list)

    def get_main_category(self) -> str:
        return "entities"

    def fetch(
        self,
        url: str = "https://dumps.wikimedia.org/wikidatawiki",
        pbar: Optional[tqdm] = None,
    ):
        # fmt: off
        self.entities = self.parse_urls(match_url(url, "entities", RegexPattern(r"\d{8}"), RegexPattern(r"wikidata-(\d{8})-all\.json\.bz2"), pbar=pbar))
        self.page = self.parse_urls(match_url(url, RegexPattern(r"\d{8}"), RegexPattern(r"wikidatawiki-(\d{8})-page\.sql\.gz"), pbar=pbar))
        self.redirect = self.parse_urls(match_url(url, RegexPattern(r"\d{8}"), RegexPattern(r"wikidatawiki-(\d{8})-redirect\.sql\.gz"), pbar=pbar))
        # fmt: on


@dataclass
class WikipediaDump(BaseDumpCollection):
    static_html: list[DumpFile] = field(default_factory=list)

    def get_main_category(self) -> str:
        return "static_html"

    def fetch(
        self,
        url: str = "https://dumps.wikimedia.org/other/enterprise_html/runs",
        pbar: Optional[tqdm] = None,
    ):
        # fmt: off
        self.static_html = self.parse_urls(match_url(url, RegexPattern(r"\d{8}"), RegexPattern(r"enwiki-NS0-(\d{8})-ENTERPRISE-HTML.json.tar.gz"), pbar=pbar))


@lru_cache
@SqliteDict.cache_fn()
def get(url: str):
    resp = requests.get(url)
    assert resp.status_code == 200, url
    return resp.content.decode()


@dataclass
class RegexPattern:
    value: str


def match_url(
    *parts: str | RegexPattern, pbar: Optional[tqdm] = None
) -> dict[str, re.Match]:
    try:
        idx = next(i for i in range(len(parts)) if isinstance(parts[i], RegexPattern))
    except StopIteration:
        raise ValueError(
            "No regex pattern found in glob. You should directly use get()"
        )

    # construct a base URL
    assert len(parts) > 0 and isinstance(parts[0], str)
    base_url = parts[0]
    for i in range(1, idx):
        part = parts[i]
        assert isinstance(part, str)
        base_url = join_url(base_url, part)

    current_urls = {base_url: None}
    for i in range(idx, len(parts)):
        part = parts[i]
        assert isinstance(part, RegexPattern)
        next_urls = {}

        if pbar is not None:
            if pbar.total is None:
                pbar.total = len(current_urls)
            else:
                pbar.total = pbar.total + len(current_urls)
            pbar.refresh()

        for current_url in current_urls.keys():
            url_pattern = re.compile(current_url + "/" + part.value)
            doc = Document(current_url, get(current_url))
            for el in doc.select("a"):
                href = join_url(current_url, el.attr("href"))
                m = url_pattern.match(href)
                if m is None:
                    continue
                next_urls[href] = m

            if pbar is not None:
                pbar.update(1)
        current_urls = next_urls

    assert all(m is not None for m in current_urls.values())
    return cast(dict[str, re.Match], current_urls)


def join_url(base_url: str, url: str):
    parsed_url = urlparse(base_url)
    root_url = parsed_url.scheme + "://" + parsed_url.netloc
    # abspath will remove the trailing slash
    return root_url + os.path.abspath(os.path.join(parsed_url.path, url))


def get_url_filename(url: str):
    parsed_url = urlparse(url)
    return os.path.basename(parsed_url.path)


def wget(url: str, outfile: Path | str):
    p = subprocess.Popen(
        [
            "wget",
            "-c",
            "-O",
            str(outfile),
            url,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    logger.debug("Start a wget process with pid {}", p.pid)

    output = []

    def read_line():
        line = (
            p.stdout.readline().decode(  # pyright: ignore[reportOptionalMemberAccess]
                "utf-8"
            )
        )
        if line != "":
            assert line[-1] == "\n"
            line = line[:-1]
        return line

    def read_bytes(line: str) -> Optional[int]:
        m = re.match(r"(\d+.?) [,.]", line)
        if m is None:
            return None
        size = m.group(1)
        if size[-1] == "K":
            return int(size[:-1]) * 1024
        raise NotImplementedError(size)

    try:
        assert p.stdout is not None
        total_bytes = -1
        # find the total bytes
        for _ in range(100):
            line = read_line()
            output.append(line)

            if line.startswith("Length: "):
                m = re.match(r"Length: (\d+)", line)
                assert m is not None
                total_bytes = int(m.group(1))
                break

        if total_bytes == -1:
            logger.error("Cannot find the download size: " + "\n".join(output))
            raise RuntimeError("Cannot find the download size")

        # read to the progress bar to find the current bytes
        current_bytes = -1
        for _ in range(100):
            line = read_line()
            output.append(line)

            if (b := read_bytes(line)) is not None:
                current_bytes = b

        if current_bytes == -1:
            logger.error("Cannot find the download progress: " + "\n".join(output))
            raise RuntimeError("Cannot find the download progress")

        with tqdm(
            desc=f"Download {Path(outfile).name}",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
            total=total_bytes,
        ) as pbar:
            pbar.update(current_bytes)
            while True:
                line = read_line()
                if line == "":
                    if p.poll() is not None:
                        break
                    continue

                if (b := read_bytes(line)) is not None:
                    pbar.update(b - current_bytes)
                    current_bytes = b
            if pbar.n != pbar.total:  # for the asthetic
                pbar.update(total_bytes - pbar.n)
    finally:
        if p.poll() is None:
            p.terminate()
            p.wait()

    if p.returncode != 0:
        logger.error("wget failed: " + "\n".join(output))
        raise RuntimeError()


class WGet:
    RECENT_OUTPUT_SIZE = 100

    @dataclass
    class Job:
        url: str
        outfile: Path
        process: subprocess.Popen
        pbar: tqdm
        process_output: deque[str] = field(default_factory=deque)

    def __init__(self):
        self.jobs: dict[str, WGet.Job] = {}

    @contextlib.contextmanager
    @staticmethod
    def start():
        try:
            obj = WGet()
            yield obj
        finally:
            obj.grateful_stop()

    def download(self, url: str, outfile: Path):
        """Start a wget process to download a file"""
        if self.is_successful_downloaded(outfile):
            logger.info("Skip download {} because it already exists", outfile.name)
            return

        p = subprocess.Popen(
            [
                "wget",
                "-c",
                "-O",
                str(outfile),
                url,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        pbar = tqdm(
            desc=f"Download {outfile.name}",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
            total=0,
        )
        self.jobs[url] = WGet.Job(url, outfile, p, pbar)

    def monitor(self):
        """Monitoring the download processes"""
        for job in self.jobs.values():
            current_bytes, total_bytes = self.get_download_progress(job.url)
            job.pbar.total = total_bytes
            job.pbar.update(current_bytes)
            job.pbar.refresh()

        jobs = list(self.jobs.values())
        while len(self.jobs) > 0:
            for job in jobs:
                line = self.read_job_output_line(job)
                if line == "":
                    if job.process.poll() is not None:
                        # this job is finished -- clean it up and remove from the list of current jobs
                        self.on_job_finish(job.url)
                        jobs = list(self.jobs.values())
                        continue

                if (b := self.read_bytes(line)) is not None:
                    job.pbar.update(b - job.pbar.n)

        for job in list(self.jobs.values()):
            self.on_job_finish(job.url)

    def on_job_finish(self, url: str):
        job = self.jobs[url]
        assert job.process.poll() is not None
        if job.process.returncode == 0:
            # successfully downloaded
            self.mark_success(job.outfile)
            if job.pbar.n != job.pbar.total:
                # for the asthetic
                job.pbar.update(job.pbar.total - job.pbar.n)
            job.pbar.close()
            self.jobs.pop(url)
        else:
            # fail to download
            logger.error(
                "[PID={}] Error while downloading URL: {}.\nReason: Unknown.\nRecent output:\n{}",
                job.process.pid,
                job.url,
                "\n".join(job.process_output),
            )
            raise RuntimeError(f"Failed to download the file at: {url}")

    def mark_success(self, outfile: Path):
        """Mark the file as successfully downloaded"""
        (outfile.parent / (outfile.name + ".success")).touch()

    def is_successful_downloaded(self, outfile: Path) -> bool:
        """Check if the file is successfully downloaded"""
        return (outfile.parent / (outfile.name + ".success")).exists()

    def get_download_progress(self, url: str):
        """Consuming some of the output of wget to get the download progress"""
        job = self.jobs[url]

        total_bytes = -1
        # find the total bytes
        for _ in range(100):
            line = self.read_job_output_line(job)
            if line.startswith("Length: "):
                m = re.match(r"Length: (\d+)", line)
                assert m is not None
                total_bytes = int(m.group(1))
                break

        if total_bytes == -1:
            logger.error(
                "[PID={}] Error while downloading URL: {}.\nReason: Can't determine the file size.\nOutput:\n{}",
                job.process.pid,
                job.url,
                "\n".join(job.process_output),
            )
            raise RuntimeError("Failed to determine the download progress")

        # read to the progress bar to find the current bytes
        current_bytes = -1
        for _ in range(100):
            line = self.read_job_output_line(job)
            if (b := self.read_bytes(line)) is not None:
                current_bytes = b

        if current_bytes == -1:
            logger.error(
                "[PID={}] Error while downloading URL: {}.\nReason: Can't determine the current progress.\nOutput:\n{}",
                job.process.pid,
                job.url,
                "\n".join(job.process_output),
            )
            raise RuntimeError("Failed to determine the download progress")

        return current_bytes, total_bytes

    def read_job_output_line(self, job: Job) -> str:
        line = self.read_line(job.process)
        if line != "":
            job.process_output.append(line)
            if len(job.process_output) > WGet.RECENT_OUTPUT_SIZE:
                job.process_output.popleft()
        return line

    def read_line(self, p: subprocess.Popen) -> str:
        assert p.stdout is not None
        line = p.stdout.readline().decode("utf-8")
        if line != "":
            assert line[-1] == "\n"
            line = line[:-1]
        return line

    def read_bytes(self, line: str) -> Optional[int]:
        m = re.match(r" *(\d+.?) [,.]", line)
        if m is None:
            return None
        size = m.group(1)
        if size[-1] == "K":
            return int(size[:-1]) * 1024
        raise NotImplementedError(size)

    def grateful_stop(self):
        for job in self.jobs.values():
            if job.process.poll() is None:
                job.process.terminate()
        for job in self.jobs.values():
            if job.process.poll() is None:
                job.process.wait()
            job.pbar.close()

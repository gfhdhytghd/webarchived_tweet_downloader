#!/usr/bin/env python3
"""
download_archive.py
====================

This script helps you collect archived tweets for a given Twitter account from
the Internet Archive's Wayback Machine and compile them into a small static
archive bundle. Because the Wayback Machine stores each tweet as a JSON
object, the script queries the Wayback CDX API to locate all available
snapshots of tweets from the chosen account and then retrieves the raw JSON
payload for each snapshot. It extracts visible tweet text, saves the raw JSON
responses, downloads media into an asset folder, and writes five HTML files:
the chronological archive plus four sorted variants.

Usage:

    python download_archive.py <twitter_username>
    python download_archive.py <twitter_username> --json-only
    python download_archive.py <twitter_username> --html-from-json

Example:

    python download_archive.py AnIncandescence
    python download_archive.py AnIncandescence --json-only --workers 24
    python download_archive.py AnIncandescence --html-from-json

Limitations:

1.  The script depends on the public Wayback CDX API.  If your network
    environment blocks direct requests to the Wayback Machine, you will need
    to run this script on a machine with unrestricted internet access.

2.  The Wayback Machine may rate‑limit frequent requests.  The script
    introduces a short delay between downloads to avoid triggering rate
    limiting; however, if you encounter HTTP 429 responses, you may need to
    increase the delay or resume the download later.

3.  Some archived tweets may be missing or inaccessible.  The script skips
    over any snapshots that cannot be retrieved or parsed.

4.  This script does not filter or censor content. If the account's
    timeline contains sensitive material, it will appear in the resulting
    archive bundle. Please exercise caution when sharing or viewing the output.

Author: Assistant
Date: April 13, 2026 (America/New_York)
"""

import argparse
import contextlib
import sys
import hashlib
import json
import html
import math
import mimetypes
import os
import random
import re
import threading
import time
import fcntl
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("This script requires the 'requests' library. You can install it with 'pip install requests'.")
    sys.exit(1)


CDX_BASE_URL = "https://web.archive.org/cdx/search/cdx"
USER_AGENT = "Mozilla/5.0 (compatible; archive-exporter/1.0; +https://web.archive.org)"
SESSION_LOCAL = threading.local()
DEFAULT_MAX_WORKERS = 12
DEFAULT_JSON_RETRY_PASSES = 3
JSON_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_MEDIA_REPAIR_PASSES = 3
MEDIA_REPAIR_BACKOFF_SECONDS = 2.0
JSON_TIMEOUT = 15
IMAGE_TIMEOUT = 10
MEDIA_CDX_TIMEOUT = 20
MEDIA_CDX_FALLBACK_LIMIT = 3
REQUEST_ATTEMPTS = 4
TWEET_URL_ID_RE = re.compile(r"/status/(\d+)")
SNAPSHOT_FILENAME_RE = re.compile(r"^(?P<timestamp>\d{14})_(?P<tweet_id>.+)\.json$")
MIME_EXTENSION_MAP = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
    "video/mp4": ".mp4",
}
IMAGE_CACHE_LOCK = threading.RLock()
MEDIA_CDX_LOOKUP_LOCK = threading.RLock()
MEDIA_CDX_LOOKUP_CACHE: Dict[Tuple[str, str, str, str], List[Tuple[str, str]]] = {}
SNAPSHOT_MANIFEST_NAME = "snapshots.json"
MEDIA_INDEX_NAME = "media_index.json"
ARCHIVE_FILTER_CSS = """
.archive-controls{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 4px;padding:12px 0;border-bottom:1px solid var(--line)}
.archive-toggle{display:inline-flex;align-items:center;gap:10px;padding:6px 14px;border:1px solid var(--line);border-radius:9999px;background:var(--bg);cursor:pointer;user-select:none;transition:background .15s,border-color .15s}
.archive-toggle:hover{background:var(--hover)}
.archive-toggle-input{position:absolute;opacity:0;pointer-events:none}
.archive-toggle-switch{position:relative;width:36px;height:20px;flex:0 0 auto;border-radius:9999px;background:var(--muted);opacity:.35;transition:background .15s,opacity .15s}
.archive-toggle-switch::after{content:"";position:absolute;top:2px;left:2px;width:16px;height:16px;border-radius:50%;background:#fff;transition:transform .15s}
.archive-toggle-input:checked+.archive-toggle-switch{background:var(--accent);opacity:1}
.archive-toggle-input:checked+.archive-toggle-switch::after{transform:translateX(16px)}
.archive-toggle-label{font-size:14px;font-weight:500;color:var(--ink)}
.archive-toggle-meta{color:var(--muted);font-size:13px}
@media (max-width:600px){.archive-controls{gap:6px}.archive-toggle{width:100%;justify-content:space-between;padding:8px 12px}}
"""
ARCHIVE_FILTER_JS = """
(() => {
  const TWEET_FILTERS = [
    {
      key: "reply",
      label: "隐藏回复帖",
      storageKey: `${getArchiveStorageNamespace()}-hide-replies`,
      predicate: (tweet, firstSegment) => {
        const meta = tweet.dataset.isReply;
        if (meta === "true") return true;
        if (meta === "false") return false;
        return /^@\\S+/.test(firstSegment);
      },
    },
    {
      key: "repost",
      label: "隐藏转贴",
      storageKey: `${getArchiveStorageNamespace()}-hide-reposts`,
      predicate: (tweet, firstSegment) => {
        const meta = tweet.dataset.isRepost;
        if (meta === "true") return true;
        if (meta === "false") return false;
        return /^RT\\s+@\\S+/i.test(firstSegment);
      },
    },
  ];

  setupTweetFilters();

  function setupTweetFilters() {
    const tweets = Array.from(document.querySelectorAll(".tweet"));
    if (!tweets.length) return;

    const classifiedFilters = TWEET_FILTERS.map((filter) => ({ ...filter, tweets: [] }));

    tweets.forEach((tweet) => {
      const body = tweet.querySelector("p");
      const firstSegment = getFirstSegmentText(body);
      classifiedFilters.forEach((filter) => {
        const matches = filter.predicate(tweet, firstSegment);
        tweet.classList.toggle(`tweet-is-${filter.key}`, matches);
        if (matches) {
          filter.tweets.push(tweet);
        }
      });
    });

    const activeFilters = classifiedFilters.filter((filter) => filter.tweets.length);
    if (!activeFilters.length) return;

    const controls = document.createElement("div");
    controls.className = "archive-controls";
    controls.innerHTML = activeFilters
      .map(
        (filter) => `
          <label class="archive-toggle">
            <input class="archive-toggle-input" type="checkbox" data-filter-key="${filter.key}" />
            <span class="archive-toggle-switch" aria-hidden="true"></span>
            <span class="archive-toggle-label">${filter.label}</span>
            <span class="archive-toggle-meta">${filter.tweets.length} / ${tweets.length}</span>
          </label>
        `,
      )
      .join("");

    const intro = document.querySelector(".archive-subtitle");
    const shell = document.querySelector(".archive-shell");
    if (!shell) return;
    if (intro?.parentNode === shell) {
      shell.insertBefore(controls, intro.nextSibling);
    } else {
      shell.insertBefore(controls, shell.firstChild);
    }

    activeFilters.forEach((filter) => {
      const checkbox = controls.querySelector(`[data-filter-key="${filter.key}"]`);
      if (!checkbox) return;
      checkbox.checked = readFilterState(filter.storageKey);
      checkbox.addEventListener("change", () => {
        writeFilterState(filter.storageKey, checkbox.checked);
        applyTweetFilters(tweets, activeFilters, controls);
      });
    });

    applyTweetFilters(tweets, activeFilters, controls);
  }

  function getFirstSegmentText(body) {
    if (!body) return "";
    const [firstSegmentHtml = ""] = body.innerHTML.split(/<br\\s*\\/?>/i);
    const scratch = document.createElement("div");
    scratch.innerHTML = firstSegmentHtml;
    return (scratch.textContent || "").trim();
  }

  function applyTweetFilters(tweets, filters, controls) {
    const enabledFilters = new Set(
      filters
        .filter((filter) => controls.querySelector(`[data-filter-key="${filter.key}"]`)?.checked)
        .map((filter) => filter.key),
    );

    tweets.forEach((tweet) => {
      const shouldHide = filters.some(
        (filter) => enabledFilters.has(filter.key) && tweet.classList.contains(`tweet-is-${filter.key}`),
      );
      tweet.hidden = shouldHide;
    });
  }

  function getArchiveStorageNamespace() {
    const file = (window.location.pathname.split("/").pop() || "archive.html").replace(/\\.html$/i, "");
    return file.replace(/_(time_desc|media_first_time_desc|text_length_desc|text_entropy_desc)$/i, "");
  }

  function readFilterState(storageKey) {
    try {
      return window.localStorage.getItem(storageKey) === "1";
    } catch {
      return false;
    }
  }

  function writeFilterState(storageKey, enabled) {
    try {
      window.localStorage.setItem(storageKey, enabled ? "1" : "0");
    } catch {
      // Ignore storage failures and keep the filter session-local.
    }
  }
})();
"""
ARCHIVE_CSS = """
:root{--bg:#fff;--hover:#f7f9f9;--ink:#0f1419;--muted:#536471;--line:#eff3f4;--accent:#1d9bf0}
*{box-sizing:border-box;margin:0}
html{-webkit-text-size-adjust:100%}
body{margin:0;color:var(--ink);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:var(--bg);line-height:1.5}
.archive-shell{max-width:600px;margin:0 auto;border-left:1px solid var(--line);border-right:1px solid var(--line);min-height:100vh}
.archive-header{padding:12px 16px;border-bottom:1px solid var(--line);position:sticky;top:0;background:rgba(255,255,255,.85);backdrop-filter:blur(12px);z-index:10}
.profile{padding:16px;border-bottom:1px solid var(--line)}
.profile-avatar{width:80px;height:80px;border-radius:50%;object-fit:cover;background:var(--line)}
.profile-name{margin-top:10px;font-size:20px;font-weight:800;line-height:1.2}
.profile-handle{font-size:14px;color:var(--muted)}
.profile-bio{margin:10px 0 0;font-size:15px;line-height:1.45}
.profile-location{display:inline-block;margin-top:8px;font-size:13px;color:var(--muted)}
.profile-stats{display:flex;gap:16px;margin-top:10px;font-size:14px;color:var(--muted)}
.profile-stats strong{color:var(--ink);font-weight:700}
.archive-badge{display:none}
.archive-title{font-size:20px;font-weight:700;line-height:1.3;letter-spacing:normal}
.archive-subtitle{margin:2px 0 0;color:var(--muted);font-size:13px;line-height:1.4}
.tweet{padding:12px 16px;border-bottom:1px solid var(--line);transition:background .15s}
.tweet:hover{background:var(--hover)}
h3{font-size:13px;font-weight:400;color:var(--muted)}
p{margin:4px 0 0;font-size:15px;color:var(--ink);line-height:1.5;overflow-wrap:anywhere}
.tweet p:last-child{margin-bottom:4px}
.tweet-media{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:2px;margin:10px 0 4px;border-radius:16px;overflow:hidden}
.tweet-media-item{width:100%}
.tweet-media-item img{display:block;width:100%;height:auto;cursor:zoom-in;background:var(--line);transition:opacity .15s}
.tweet-media-item video{display:block;width:100%;height:auto;background:#000}
.tweet-media-item img:hover{opacity:.88}
.tweet-ref{margin:10px 0 4px;padding:12px;border:1px solid var(--line);border-radius:12px;background:var(--bg)}
.tweet-ref p{margin:4px 0 0;font-size:14px;color:var(--ink);line-height:1.45}
.tweet-ref-header{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap}
.tweet-ref-name{font-size:14px;font-weight:700;color:var(--ink)}
.tweet-ref-handle{font-size:13px;color:var(--muted)}
.tweet-ref-time{display:block;margin-top:6px;font-size:12px;color:var(--muted)}
.tweet-ref .tweet-media{margin:8px 0 4px;border-radius:12px}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
.lightbox{position:fixed;inset:0;z-index:1000;display:none;align-items:center;justify-content:center;padding:24px;background:rgba(0,0,0,.7);cursor:pointer}
.lightbox.is-open{display:flex}
.lightbox-image{display:block;max-width:min(100vw - 48px,1400px);max-height:calc(100vh - 48px);border-radius:12px;background:#000}
body.lightbox-open{overflow:hidden}
@media (max-width:600px){.archive-shell{border-left:none;border-right:none}.tweet{padding:10px 16px}.tweet-media{border-radius:12px}}
"""
ARCHIVE_JS = """
(() => {
  const images = Array.from(document.querySelectorAll(".tweet-media-item img"));
  if (!images.length) return;

  const lightbox = document.createElement("div");
  lightbox.className = "lightbox";
  lightbox.setAttribute("aria-hidden", "true");
  lightbox.innerHTML = `<img class="lightbox-image" alt="" />`;
  document.body.appendChild(lightbox);

  const lightboxImage = lightbox.querySelector(".lightbox-image");

  function closeLightbox() {
    lightbox.classList.remove("is-open");
    lightbox.setAttribute("aria-hidden", "true");
    document.body.classList.remove("lightbox-open");
    lightboxImage.removeAttribute("src");
    lightboxImage.alt = "";
  }

  function openLightbox(image) {
    lightboxImage.src = image.currentSrc || image.src;
    lightboxImage.alt = image.alt || "";
    lightbox.classList.add("is-open");
    lightbox.setAttribute("aria-hidden", "false");
    document.body.classList.add("lightbox-open");
  }

  lightbox.addEventListener("click", closeLightbox);

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && lightbox.classList.contains("is-open")) {
      closeLightbox();
    }
  });

  images.forEach((image) => {
    image.loading = "lazy";
    image.decoding = "async";
    image.tabIndex = 0;
    image.setAttribute("role", "button");
    image.addEventListener("click", () => openLightbox(image));
    image.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        openLightbox(image);
      }
    });
  });
})();
"""
INDEX_PAGE_CSS = """
:root{--bg:#fff;--ink:#0f1419;--muted:#536471;--line:#eff3f4;--accent:#1d9bf0;--hover:#f7f9f9}
*{box-sizing:border-box;margin:0}
body{margin:0;color:var(--ink);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:var(--bg);line-height:1.5}
main{max-width:600px;margin:0 auto;border-left:1px solid var(--line);border-right:1px solid var(--line);min-height:100vh;padding:0 0 40px}
.hero{padding:12px 16px 12px;border-bottom:1px solid var(--line);position:sticky;top:0;background:rgba(255,255,255,.85);backdrop-filter:blur(12px);z-index:10}
.kicker{display:none}
h1{font-size:20px;font-weight:700;line-height:1.3}
.lead{color:var(--muted);font-size:13px;margin:2px 0 0;line-height:1.4}
.grid{display:flex;flex-direction:column}
.card{display:block;padding:16px;border-bottom:1px solid var(--line);text-decoration:none;color:inherit;transition:background .15s}
.card:hover{background:var(--hover)}
.eyebrow{display:none}
.card h2{font-size:15px;font-weight:700;line-height:1.3}
.card p{margin:2px 0 0;color:var(--muted);font-size:13px;line-height:1.4}
.footer{padding:12px 16px;color:var(--muted);font-size:13px;border-top:1px solid var(--line)}
@media (max-width:600px){main{border-left:none;border-right:none}}
"""


@dataclass(frozen=True)
class ArchiveEntry:
    block: str
    dt: datetime
    time_text: str
    body_text: str
    body_length: int
    entropy: float
    has_media: bool
    original_url: str
    index: int


@dataclass(frozen=True)
class SnapshotRecord:
    timestamp: str
    original_url: str
    json_path: Path


@dataclass(frozen=True)
class TweetMediaItem:
    kind: str
    url: str
    content_type: str = ""
    poster_url: str = ""


@dataclass(frozen=True)
class UserProfile:
    username: str
    display_name: str
    description: str
    location: str
    avatar_url: str
    followers: int
    following: int


@dataclass(frozen=True)
class ReferencedTweetInfo:
    kind: str  # "quoted" or "replied_to"
    text: str
    author: str
    display_name: str
    created_at: str
    url: str
    media: Tuple["TweetMediaItem", ...] = ()


def normalize_text_for_entropy(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def calculate_entropy(text: str) -> float:
    if not text:
        return 0.0
    counts = Counter(text)
    total = len(text)
    return -sum((count / total) * math.log2(count / total) for count in counts.values())


def guess_extension(image_url: str, mime_type: str) -> str:
    if mime_type in MIME_EXTENSION_MAP:
        return MIME_EXTENSION_MAP[mime_type]
    guessed = mimetypes.guess_type(image_url)[0]
    if guessed in MIME_EXTENSION_MAP:
        return MIME_EXTENSION_MAP[guessed]
    suffix = Path(image_url.split("?", 1)[0]).suffix
    return suffix if suffix else ".img"


def build_asset_source_variants(asset_url: str) -> List[str]:
    variants: List[str] = []
    seen = set()

    def add(url: str) -> None:
        if not url or url in seen:
            return
        seen.add(url)
        variants.append(url)

    add(asset_url)
    parsed = urlparse(asset_url)
    if parsed.netloc.endswith("pbs.twimg.com") and parsed.path.startswith("/media/"):
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        suffix = Path(parsed.path).suffix.lower()
        format_hint = query.get("format", "")
        normalized_path = parsed.path

        if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp"}:
            format_hint = format_hint or suffix.lstrip(".")
            normalized_path = parsed.path[: -len(suffix)]

        if format_hint:
            name_hints = [
                query.get("name", ""),
                "orig",
                "4096x4096",
                "large",
                "medium",
                "small",
                "900x900",
            ]
            for name_hint in name_hints:
                normalized_query = dict(query)
                normalized_query["format"] = format_hint
                if name_hint:
                    normalized_query["name"] = name_hint
                else:
                    normalized_query.pop("name", None)
                normalized_url = urlunparse(
                    parsed._replace(path=normalized_path, query=urlencode(normalized_query))
                )
                add(normalized_url)

    if parsed.netloc.endswith("video.twimg.com") and parsed.query:
        add(urlunparse(parsed._replace(query="")))

    return variants


def build_asset_lookup_queries(asset_url: str) -> List[Tuple[str, str]]:
    queries: List[Tuple[str, str]] = []
    seen = set()

    def add(url: str, match_type: str = "exact") -> None:
        if not url:
            return
        key = (url, match_type)
        if key in seen:
            return
        seen.add(key)
        queries.append(key)

    parsed = urlparse(asset_url)
    source_variants = build_asset_source_variants(asset_url)
    for source_url in source_variants:
        add(source_url, "exact")

    if parsed.query:
        add(urlunparse(parsed._replace(query="")), "exact")

    prefix_url = urlunparse(parsed._replace(query=""))
    if prefix_url:
        add(prefix_url, "prefix")

    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".m3u8"}:
        stem_prefix = urlunparse(parsed._replace(path=parsed.path[: -len(suffix)], query=""))
        add(stem_prefix, "prefix")

    if parsed.netloc.endswith("pbs.twimg.com") and parsed.path.startswith("/media/"):
        media_stem = urlunparse(parsed._replace(path=parsed.path.rsplit(".", 1)[0], query=""))
        add(media_stem, "prefix")

    return queries


def build_archived_urls_for_source_url(source_url: str, snapshot_timestamp: str) -> List[str]:
    urls: List[str] = []
    seen = set()

    def add(url: str) -> None:
        if not url or url in seen:
            return
        seen.add(url)
        urls.append(url)

    if snapshot_timestamp:
        add(f"https://web.archive.org/web/{snapshot_timestamp}im_/{source_url}")
        add(f"https://web.archive.org/web/{snapshot_timestamp}if_/{source_url}")
        add(f"https://web.archive.org/web/{snapshot_timestamp}/{source_url}")
    add(source_url)

    return urls


def build_asset_candidate_urls(asset_url: str, snapshot_timestamp: str) -> List[str]:
    candidates: List[str] = []
    seen = set()

    def add(url: str) -> None:
        if not url or url in seen:
            return
        seen.add(url)
        candidates.append(url)

    for source_url in build_asset_source_variants(asset_url):
        for candidate in build_archived_urls_for_source_url(source_url, snapshot_timestamp):
            add(candidate)

    return candidates


def query_closest_media_captures(source_url: str, snapshot_timestamp: str, match_type: str = "exact") -> List[Tuple[str, str]]:
    cache_key = (source_url, snapshot_timestamp, match_type, "relaxed")
    with MEDIA_CDX_LOOKUP_LOCK:
        cached = MEDIA_CDX_LOOKUP_CACHE.get(cache_key)
        if cached is not None:
            return cached

    rows: List[Tuple[str, str]] = []
    seen = set()

    def collect_rows(extra_params: Optional[Dict[str, str]] = None) -> bool:
        params = {
            "url": source_url,
            "output": "json",
            "fl": "timestamp,original,statuscode,mimetype",
            "limit": str(MEDIA_CDX_FALLBACK_LIMIT),
            "closest": snapshot_timestamp,
            "collapse": "digest",
        }
        if match_type == "prefix":
            params["matchType"] = "prefix"
        if extra_params:
            params.update(extra_params)
        try:
            resp = get_with_retry(CDX_BASE_URL, params=params, timeout=MEDIA_CDX_TIMEOUT)
            payload = resp.json()
        except Exception:
            return False

        for row in payload[1:]:
            if len(row) < 2 or not row[0] or not row[1]:
                continue
            statuscode = _coerce_string(row[2]).strip() if len(row) >= 3 else ""
            mimetype = _coerce_string(row[3]).strip() if len(row) >= 4 else ""
            if statuscode and statuscode not in {"200", "206", "302", "-"}:
                continue
            if mimetype.startswith("text/"):
                continue
            candidate = (row[0], row[1])
            if candidate in seen:
                continue
            seen.add(candidate)
            rows.append(candidate)
        return bool(rows)

    if not collect_rows({"filter": "statuscode:200"}):
        collect_rows()

    with MEDIA_CDX_LOOKUP_LOCK:
        MEDIA_CDX_LOOKUP_CACHE[cache_key] = rows
    return rows


def build_closest_capture_candidate_urls(asset_url: str, snapshot_timestamp: str) -> List[str]:
    candidates: List[str] = []
    seen = set()

    def add(url: str) -> None:
        if not url or url in seen:
            return
        seen.add(url)
        candidates.append(url)

    for source_url, match_type in build_asset_lookup_queries(asset_url):
        for capture_timestamp, original_url in query_closest_media_captures(source_url, snapshot_timestamp, match_type=match_type):
            for candidate in build_archived_urls_for_source_url(original_url, capture_timestamp):
                add(candidate)

    return candidates


def build_image_candidate_urls(image_url: str, snapshot_timestamp: str) -> List[str]:
    return build_asset_candidate_urls(image_url, snapshot_timestamp)


def build_video_candidate_urls(video_url: str, snapshot_timestamp: str) -> List[str]:
    return build_asset_candidate_urls(video_url, snapshot_timestamp)


def ensure_asset_directories(asset_dir: Path) -> None:
    asset_dir.mkdir(parents=True, exist_ok=True)
    (asset_dir / "media").mkdir(parents=True, exist_ok=True)
    (asset_dir / "json").mkdir(parents=True, exist_ok=True)


def ensure_static_files(asset_dir: Path) -> None:
    ensure_asset_directories(asset_dir)
    (asset_dir / "archive.css").write_text(ARCHIVE_CSS + "\n" + ARCHIVE_FILTER_CSS, encoding="utf-8")
    (asset_dir / "archive.js").write_text(ARCHIVE_FILTER_JS + "\n" + ARCHIVE_JS, encoding="utf-8")


def build_output_paths(user: str) -> Tuple[Path, Path, Path, Path, str]:
    output_dir = Path(f"{user}_archive").resolve()
    output_path = output_dir / f"{user}_archive.html"
    asset_dir = output_dir / f"{user}_archive_assets"
    media_dir = asset_dir / "media"
    json_dir = asset_dir / "json"
    return output_path, asset_dir, media_dir, json_dir, asset_dir.name


def snapshot_manifest_path(asset_dir: Path) -> Path:
    return asset_dir / SNAPSHOT_MANIFEST_NAME


def media_index_path(asset_dir: Path) -> Path:
    return asset_dir / MEDIA_INDEX_NAME


def media_index_lock_path(asset_dir: Path) -> Path:
    return asset_dir / f"{MEDIA_INDEX_NAME}.lock"


@contextlib.contextmanager
def locked_media_index(asset_dir: Path):
    lock_path = media_index_lock_path(asset_dir)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def parse_snapshot_filename(filename: str) -> Optional[Tuple[str, str]]:
    match = SNAPSHOT_FILENAME_RE.match(filename)
    if not match:
        return None
    return match.group("timestamp"), match.group("tweet_id")


def reconstruct_original_url(user: str, tweet_id: str) -> str:
    return f"https://twitter.com/{user}/status/{tweet_id}"


def build_snapshot_records(snapshots: List[Tuple[str, str]], json_dir: Path) -> List[SnapshotRecord]:
    records: List[SnapshotRecord] = []
    for timestamp, original_url in snapshots:
        json_path = build_json_output_path(json_dir, timestamp, original_url, {})
        records.append(SnapshotRecord(timestamp=timestamp, original_url=original_url, json_path=json_path))
    return records


def write_snapshot_manifest(asset_dir: Path, user: str, records: List[SnapshotRecord]) -> None:
    payload = {
        "user": user,
        "snapshots": [
            {
                "timestamp": record.timestamp,
                "original_url": record.original_url,
                "json_filename": record.json_path.name,
            }
            for record in records
        ],
    }
    snapshot_manifest_path(asset_dir).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def load_snapshot_records(asset_dir: Path, json_dir: Path, user: str) -> List[SnapshotRecord]:
    manifest = snapshot_manifest_path(asset_dir)
    records: List[SnapshotRecord] = []

    if manifest.exists():
        try:
            payload = json.loads(manifest.read_text(encoding="utf-8"))
        except Exception:
            payload = {}

        snapshots = payload.get("snapshots", []) if isinstance(payload, dict) else []
        for item in snapshots:
            if not isinstance(item, dict):
                continue
            timestamp = _coerce_string(item.get("timestamp")).strip()
            original_url = _coerce_string(item.get("original_url")).strip()
            json_filename = _coerce_string(item.get("json_filename")).strip()
            if not json_filename:
                continue
            if not original_url:
                parsed = parse_snapshot_filename(json_filename)
                if not parsed:
                    continue
                _, tweet_id = parsed
                original_url = reconstruct_original_url(user, tweet_id)
            if not timestamp:
                parsed = parse_snapshot_filename(json_filename)
                if not parsed:
                    continue
                timestamp, _ = parsed
            records.append(
                SnapshotRecord(
                    timestamp=timestamp,
                    original_url=original_url,
                    json_path=json_dir / json_filename,
                )
            )

    if records:
        return sorted(records, key=lambda record: (record.timestamp, record.original_url, record.json_path.name))

    for json_path in sorted(json_dir.glob("*.json")):
        parsed = parse_snapshot_filename(json_path.name)
        if not parsed:
            continue
        timestamp, tweet_id = parsed
        records.append(
            SnapshotRecord(
                timestamp=timestamp,
                original_url=reconstruct_original_url(user, tweet_id),
                json_path=json_path,
            )
        )
    return records


def resolve_snapshot_records(
    user: str,
    asset_dir: Path,
    json_dir: Path,
    resume: bool = True,
    refresh: bool = False,
) -> Tuple[List[SnapshotRecord], str]:
    if resume and not refresh:
        records = load_snapshot_records(asset_dir, json_dir, user)
        if records:
            return records, "local"

    snapshots = fetch_snapshots(user)
    if not snapshots:
        return [], "remote"
    records = build_snapshot_records(snapshots, json_dir)
    return records, "remote"


def count_existing_snapshot_json(records: List[SnapshotRecord]) -> int:
    ready_count = 0
    for record in records:
        if record.json_path.exists() and load_snapshot_json(record.json_path) is not None:
            ready_count += 1
    return ready_count


def split_ready_and_missing_snapshot_records(records: List[SnapshotRecord]) -> Tuple[List[SnapshotRecord], List[SnapshotRecord]]:
    ready_records: List[SnapshotRecord] = []
    missing_records: List[SnapshotRecord] = []
    for record in records:
        if record.json_path.exists() and load_snapshot_json(record.json_path) is not None:
            ready_records.append(record)
        else:
            missing_records.append(record)
    return ready_records, missing_records


def load_media_cache(asset_dir: Path, asset_dir_name: str) -> Dict[str, str]:
    cache_path = media_index_path(asset_dir)
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}

    cleaned: Dict[str, str] = {}
    expected_prefix = f"{asset_dir_name}/media/"
    for image_url, relative_path in payload.items():
        if not isinstance(image_url, str) or not isinstance(relative_path, str):
            continue
        if not relative_path.startswith(expected_prefix):
            continue
        if (asset_dir / "media" / Path(relative_path).name).exists():
            cleaned[image_url] = relative_path
    return cleaned


def _clean_media_cache_entries(asset_dir: Path, asset_dir_name: str, cache: Dict[str, str]) -> Dict[str, str]:
    cleaned: Dict[str, str] = {}
    expected_prefix = f"{asset_dir_name}/media/"
    for media_url, relative_path in cache.items():
        if not isinstance(media_url, str) or not isinstance(relative_path, str):
            continue
        if not relative_path.startswith(expected_prefix):
            continue
        if (asset_dir / "media" / Path(relative_path).name).exists():
            cleaned[media_url] = relative_path
    return cleaned


def write_media_cache(asset_dir: Path, cache: Dict[str, str]) -> None:
    cache_path = media_index_path(asset_dir)
    with IMAGE_CACHE_LOCK:
        asset_dir_name = asset_dir.name
        with locked_media_index(asset_dir):
            disk_cache = load_media_cache(asset_dir, asset_dir_name)
            merged_cache = _clean_media_cache_entries(asset_dir, asset_dir_name, disk_cache)
            merged_cache.update(_clean_media_cache_entries(asset_dir, asset_dir_name, cache))
            cache.clear()
            cache.update(merged_cache)
            payload = json.dumps(dict(sorted(merged_cache.items())), ensure_ascii=False, indent=2) + "\n"
            tmp_path = cache_path.with_name(f"{cache_path.name}.{os.getpid()}.tmp")
            tmp_path.write_text(payload, encoding="utf-8")
            tmp_path.replace(cache_path)


def persist_media_cache_entry(media_dir: Path, cache: Optional[Dict[str, str]]) -> None:
    if cache is None:
        return
    write_media_cache(media_dir.parent, cache)


def sort_entries(entries: List[ArchiveEntry], mode: str) -> List[ArchiveEntry]:
    if mode == "chronological":
        return sorted(entries, key=lambda entry: (entry.dt, entry.index))
    if mode == "time_desc":
        return sorted(entries, key=lambda entry: (entry.dt, entry.index), reverse=True)
    if mode == "media_first_time_desc":
        return sorted(
            entries,
            key=lambda entry: (0 if entry.has_media else 1, -entry.dt.timestamp(), -entry.index),
        )
    if mode == "text_length_desc":
        return sorted(
            entries,
            key=lambda entry: (-entry.body_length, -entry.dt.timestamp(), -entry.index),
        )
    if mode == "text_entropy_desc":
        return sorted(
            entries,
            key=lambda entry: (-entry.entropy, -entry.body_length, -entry.dt.timestamp(), -entry.index),
        )
    raise ValueError(f"Unsupported sort mode: {mode}")


def get_session() -> requests.Session:
    """
    Reuse a requests session per thread for faster downloads.
    """
    session = getattr(SESSION_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        SESSION_LOCAL.session = session
    return session


def web_archive_http_fallback_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme == "https" and parsed.netloc == "web.archive.org":
        return urlunparse(parsed._replace(scheme="http"))
    return ""


def get_with_retry(url: str, *, timeout: int, params: Optional[dict] = None) -> requests.Response:
    """
    Perform a GET request with a few retries and short exponential backoff.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        request_url = url
        try:
            while True:
                try:
                    resp = get_session().get(request_url, params=params, timeout=timeout)
                    resp.raise_for_status()
                    return resp
                except requests.exceptions.SSLError as exc:
                    last_exc = exc
                    fallback_url = web_archive_http_fallback_url(request_url)
                    if fallback_url and fallback_url != request_url:
                        request_url = fallback_url
                        continue
                    raise
        except requests.HTTPError as exc:
            last_exc = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code is not None and 400 <= status_code < 500 and status_code not in {408, 429}:
                break
            if attempt == REQUEST_ATTEMPTS:
                break
            time.sleep((0.4 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.2))
        except Exception as exc:
            last_exc = exc
            if attempt == REQUEST_ATTEMPTS:
                break
            time.sleep((0.4 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.2))
    raise RuntimeError(f"GET failed after {REQUEST_ATTEMPTS} attempts for {url}") from last_exc


def fetch_snapshots(user: str) -> List[Tuple[str, str]]:
    """
    Query the CDX API for all archived tweets from a given user.  Returns a
    list of tuples (timestamp, original_url).  The API returns one entry for
    every capture of every tweet.  We collapse by content digest to avoid
    duplicates.

    Parameters
    ----------
    user : str
        Twitter username without the leading '@'.  For example
        'AnIncandescence'.

    Returns
    -------
    List[Tuple[str, str]]
        A list of (timestamp, original_url) pairs sorted by timestamp.
    """
    params = {
        # With matchType=prefix, the CDX API expects a real prefix rather than
        # a glob. A literal trailing '*' causes zero matches for this account.
        "url": f"twitter.com/{user}/status/",
        # Request JSON output for easier parsing.  The first row contains
        # headers; subsequent rows are lists of fields as ordered below.
        "output": "json",
        # Fields: timestamp and original URL.
        "fl": "timestamp,original",
        # Deduplicate identical content using digest.
        "collapse": "digest",
        # Archived tweet payloads for this endpoint often record statuscode as
        # "-" in CDX, so filtering for 200 would incorrectly drop everything.
        # Use prefix matching to include all statuses under the user path.
        "matchType": "prefix",
    }
    try:
        resp = get_with_retry(CDX_BASE_URL, params=params, timeout=JSON_TIMEOUT)
    except Exception as exc:
        print(f"Error querying CDX API: {exc}")
        return []

    try:
        rows = resp.json()
    except json.JSONDecodeError as exc:
        print(f"Failed to parse JSON from CDX API: {exc}")
        return []

    # First row contains field names.  Subsequent rows contain data.
    snapshots: List[Tuple[str, str]] = []
    for row in rows[1:]:
        if len(row) >= 2:
            timestamp, original = row[0], row[1]
            snapshots.append((timestamp, original))
    # Sort by timestamp ascending
    snapshots.sort(key=lambda x: x[0])
    return snapshots


def _append_photo_media_from_entities(container: dict, media_items: List[TweetMediaItem]) -> None:
    """
    Collect photo URLs from legacy Twitter entity payloads when present.
    """
    for entity_key in ("extended_entities", "entities"):
        entity_block = container.get(entity_key, {})
        entity_media_items = entity_block.get("media", []) if isinstance(entity_block, dict) else []
        for media in entity_media_items:
            if not isinstance(media, dict):
                continue
            if media.get("type") != "photo":
                continue
            media_url = media.get("media_url_https") or media.get("media_url") or media.get("url")
            if media_url:
                media_items.append(TweetMediaItem(kind="image", url=media_url))


def select_best_video_variant(media: dict) -> Optional[Tuple[str, str]]:
    """
    Prefer the highest bitrate MP4 variant for archived Twitter videos.
    """
    best_mp4: Optional[Tuple[int, str, str]] = None
    fallback: Optional[Tuple[str, str]] = None

    for variant in media.get("variants", []):
        if not isinstance(variant, dict):
            continue
        url = _coerce_string(variant.get("url")).strip()
        content_type = _coerce_string(variant.get("content_type")).strip()
        if not url or not content_type:
            continue
        if fallback is None:
            fallback = (url, content_type)
        if content_type != "video/mp4":
            continue
        bit_rate = variant.get("bit_rate")
        if not isinstance(bit_rate, int):
            bit_rate = -1
        candidate = (bit_rate, url, content_type)
        if best_mp4 is None or candidate[0] > best_mp4[0]:
            best_mp4 = candidate

    if best_mp4 is not None:
        return best_mp4[1], best_mp4[2]
    return fallback


def find_primary_tweet_node(data: dict, original_url: str) -> Optional[dict]:
    target_tweet_id_match = TWEET_URL_ID_RE.search(original_url)
    target_tweet_id = target_tweet_id_match.group(1) if target_tweet_id_match else ""
    fallback_node: Optional[dict] = None

    for node in _iter_candidate_tweet_nodes(data):
        metadata = _extract_metadata_from_node(node)
        if fallback_node is None and (metadata["tweet_id"] or _extract_tweet_text_from_node(node)):
            fallback_node = node
        if target_tweet_id and metadata["tweet_id"] == target_tweet_id:
            return node

    return fallback_node


def _find_referenced_tweet_nodes(data: dict, primary_node: dict, ref_type: str) -> List[dict]:
    """Find referenced tweet nodes by type ('quoted' or 'replied_to')."""
    referenced = primary_node.get("referenced_tweets", [])
    target_ids = {
        _coerce_string(item.get("id")).strip()
        for item in referenced
        if isinstance(item, dict) and item.get("type") == ref_type and _coerce_string(item.get("id")).strip()
    }
    if not target_ids:
        return []

    matches: List[dict] = []
    seen_ids = set()
    includes = data.get("includes", {})
    tweets = includes.get("tweets", []) if isinstance(includes, dict) else []
    for node in tweets:
        if not isinstance(node, dict):
            continue
        metadata = _extract_metadata_from_node(node)
        tweet_id = metadata.get("tweet_id", "")
        if tweet_id in target_ids and tweet_id not in seen_ids:
            matches.append(node)
            seen_ids.add(tweet_id)

    if seen_ids == target_ids:
        return matches

    for node in _iter_candidate_tweet_nodes(data):
        metadata = _extract_metadata_from_node(node)
        tweet_id = metadata.get("tweet_id", "")
        if tweet_id in target_ids and tweet_id not in seen_ids:
            matches.append(node)
            seen_ids.add(tweet_id)
            if seen_ids == target_ids:
                break

    return matches


def find_quoted_tweet_nodes(data: dict, primary_node: dict) -> List[dict]:
    return _find_referenced_tweet_nodes(data, primary_node, "quoted")


def find_replied_to_tweet_nodes(data: dict, primary_node: dict) -> List[dict]:
    return _find_referenced_tweet_nodes(data, primary_node, "replied_to")


def _extract_note_tweet_text(container: dict) -> str:
    """
    Prefer expanded note-tweet text when Twitter stores long posts separately.
    """
    candidates = [
        container.get("note_tweet_results", {}).get("result", {}).get("text"),
        container.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {}).get("text"),
    ]
    legacy = container.get("legacy")
    if isinstance(legacy, dict):
        candidates.extend(
            [
                legacy.get("note_tweet_results", {}).get("result", {}).get("text"),
                legacy.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {}).get("text"),
            ]
        )
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    return ""


def _extract_tweet_text_from_node(node: dict) -> str:
    """
    Extract visible tweet text from a variety of Twitter API payload shapes.
    """
    note_text = _extract_note_tweet_text(node)

    direct_markers = (
        "id",
        "id_str",
        "rest_id",
        "author_id",
        "conversation_id",
        "conversation_id_str",
        "edit_history_tweet_ids",
        "created_at",
        "attachments",
    )
    if any(marker in node for marker in direct_markers):
        for key in ("full_text", "text"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return note_text or value

    legacy = node.get("legacy")
    if isinstance(legacy, dict):
        legacy_markers = (
            "id_str",
            "conversation_id_str",
            "created_at",
            "favorite_count",
            "retweet_count",
            "reply_count",
            "quote_count",
            "entities",
            "extended_entities",
        )
        if any(marker in legacy for marker in legacy_markers) or "rest_id" in node:
            for key in ("full_text", "text"):
                value = legacy.get(key)
                if isinstance(value, str) and value.strip():
                    return note_text or value

    return ""


def _coerce_string(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _extract_metadata_from_node(node: dict) -> Dict[str, str]:
    legacy = node.get("legacy") if isinstance(node.get("legacy"), dict) else {}
    tweet_id = _coerce_string(
        node.get("rest_id") or node.get("id_str") or legacy.get("id_str") or node.get("id")
    )
    conversation_id = _coerce_string(
        node.get("conversation_id_str")
        or legacy.get("conversation_id_str")
        or node.get("conversation_id")
        or legacy.get("conversation_id")
    )
    in_reply_to_status_id = _coerce_string(
        node.get("in_reply_to_status_id_str")
        or legacy.get("in_reply_to_status_id_str")
        or node.get("in_reply_to_status_id")
        or legacy.get("in_reply_to_status_id")
    )
    in_reply_to_user_id = _coerce_string(
        node.get("in_reply_to_user_id_str")
        or legacy.get("in_reply_to_user_id_str")
        or node.get("in_reply_to_user_id")
        or legacy.get("in_reply_to_user_id")
    )
    in_reply_to_screen_name = _coerce_string(
        node.get("in_reply_to_screen_name") or legacy.get("in_reply_to_screen_name")
    )
    text = _extract_tweet_text_from_node(node)
    is_repost = "true" if (
        "retweeted_status_result" in node
        or "retweeted_status" in node
        or "retweeted_status_result" in legacy
        or "retweeted_status" in legacy
        or _coerce_string(node.get("retweeted_status_id_str") or legacy.get("retweeted_status_id_str"))
        or text.startswith("RT @")
    ) else "false"
    return {
        "tweet_id": tweet_id,
        "conversation_id": conversation_id,
        "in_reply_to_status_id": in_reply_to_status_id,
        "in_reply_to_user_id": in_reply_to_user_id,
        "in_reply_to_screen_name": in_reply_to_screen_name,
        "is_repost": is_repost,
    }


def _iter_candidate_tweet_nodes(node):
    if isinstance(node, list):
        for item in node:
            yield from _iter_candidate_tweet_nodes(item)
        return
    if not isinstance(node, dict):
        return

    metadata = _extract_metadata_from_node(node)
    if metadata["tweet_id"] or _extract_tweet_text_from_node(node):
        yield node

    for value in node.values():
        yield from _iter_candidate_tweet_nodes(value)


def extract_tweet_metadata(data: dict, original_url: str) -> Dict[str, str]:
    target_tweet_id_match = TWEET_URL_ID_RE.search(original_url)
    target_tweet_id = target_tweet_id_match.group(1) if target_tweet_id_match else ""
    fallback_metadata: Optional[Dict[str, str]] = None

    for node in _iter_candidate_tweet_nodes(data):
        metadata = _extract_metadata_from_node(node)
        if metadata["tweet_id"] and fallback_metadata is None:
            fallback_metadata = metadata
        if target_tweet_id and metadata["tweet_id"] == target_tweet_id:
            fallback_metadata = metadata
            break

    metadata = fallback_metadata or {
        "tweet_id": target_tweet_id,
        "conversation_id": "",
        "in_reply_to_status_id": "",
        "in_reply_to_user_id": "",
        "in_reply_to_screen_name": "",
        "is_repost": "false",
    }

    is_reply = "unknown"
    is_thread_root = "unknown"
    if metadata["in_reply_to_status_id"]:
        is_reply = "true"
        is_thread_root = "false"
    elif metadata["tweet_id"] and metadata["conversation_id"]:
        if metadata["tweet_id"] == metadata["conversation_id"]:
            is_reply = "false"
            is_thread_root = "true"
        else:
            is_reply = "true"
            is_thread_root = "false"

    metadata["is_reply"] = is_reply
    metadata["is_thread_root"] = is_thread_root
    return metadata


def _find_quote_tco_urls(node: dict) -> set:
    """Return the set of t.co short URLs that point to quoted tweets."""
    referenced = node.get("referenced_tweets", [])
    quoted_ids = {
        _coerce_string(item.get("id")).strip()
        for item in referenced
        if isinstance(item, dict) and item.get("type") == "quoted" and _coerce_string(item.get("id")).strip()
    }
    if not quoted_ids:
        return set()

    tco_urls: set = set()
    for entity_block_key in ("entities", "extended_entities"):
        entity_block = node.get(entity_block_key, {})
        if not isinstance(entity_block, dict):
            continue
        for url_entity in entity_block.get("urls", []):
            if not isinstance(url_entity, dict):
                continue
            expanded = url_entity.get("expanded_url", "")
            tco = url_entity.get("url", "")
            if not tco or not expanded:
                continue
            for qid in quoted_ids:
                if qid in expanded:
                    tco_urls.add(tco)
                    break
    legacy = node.get("legacy")
    if isinstance(legacy, dict):
        for entity_block_key in ("entities", "extended_entities"):
            entity_block = legacy.get(entity_block_key, {})
            if not isinstance(entity_block, dict):
                continue
            for url_entity in entity_block.get("urls", []):
                if not isinstance(url_entity, dict):
                    continue
                expanded = url_entity.get("expanded_url", "")
                tco = url_entity.get("url", "")
                if not tco or not expanded:
                    continue
                for qid in quoted_ids:
                    if qid in expanded:
                        tco_urls.add(tco)
                        break
    return tco_urls


def _strip_tco_urls(text: str, tco_urls: set) -> str:
    for tco in tco_urls:
        text = text.replace(tco, "")
    return text.strip()


def _extract_user_info(data: dict, node: dict) -> Tuple[str, str]:
    """Return (screen_name, display_name) for a tweet node's author."""
    author_id = _coerce_string(node.get("author_id"))
    includes = data.get("includes", {})
    if isinstance(includes, dict):
        for user in includes.get("users", []):
            if isinstance(user, dict):
                uid = _coerce_string(user.get("id"))
                if author_id and uid == author_id:
                    return (
                        _coerce_string(user.get("username") or user.get("screen_name")),
                        _coerce_string(user.get("name")),
                    )
    legacy = node.get("legacy") if isinstance(node.get("legacy"), dict) else {}
    user_block = node.get("core", {}).get("user_results", {}).get("result", {})
    if isinstance(user_block, dict):
        ul = user_block.get("legacy", {})
        if isinstance(ul, dict):
            sn = _coerce_string(ul.get("screen_name"))
            dn = _coerce_string(ul.get("name"))
            if sn:
                return sn, dn
    screen_name = _coerce_string(node.get("screen_name") or legacy.get("screen_name"))
    display_name = _coerce_string(node.get("name") or legacy.get("name"))
    return screen_name, display_name


def extract_tweet_content(data: dict, original_url: str) -> Tuple[str, List[TweetMediaItem], Dict[str, str], List[ReferencedTweetInfo]]:
    """
    Extract visible tweet text, typed media items, reply metadata, and quoted tweets from a payload.
    """
    texts: List[str] = []
    media_items: List[TweetMediaItem] = []
    media_by_key: Dict[str, dict] = {}
    seen_texts = set()
    ref_tweets: List[ReferencedTweetInfo] = []

    includes = data.get("includes", {})
    if isinstance(includes, dict):
        for media in includes.get("media", []):
            if isinstance(media, dict) and media.get("media_key"):
                media_by_key[media["media_key"]] = media

    def collect_text_from_node(node: dict) -> None:
        text = _extract_tweet_text_from_node(node)
        if text and text not in seen_texts:
            texts.append(text)
            seen_texts.add(text)

    def collect_media_from_node(node: dict, target: Optional[List[TweetMediaItem]] = None) -> None:
        dest = target if target is not None else media_items
        attachments = node.get("attachments", {})
        media_keys = attachments.get("media_keys", []) if isinstance(attachments, dict) else []
        attachment_media = []
        for media_key in media_keys:
            media = media_by_key.get(media_key)
            if not isinstance(media, dict):
                continue
            attachment_media.append(media)

        for media in attachment_media:
            media_type = media.get("type")
            if media_type == "photo" and media.get("url"):
                dest.append(TweetMediaItem(kind="image", url=media["url"]))
            elif media_type in {"video", "animated_gif"}:
                variant = select_best_video_variant(media)
                if variant:
                    dest.append(
                        TweetMediaItem(
                            kind="video",
                            url=variant[0],
                            content_type=variant[1],
                            poster_url=_coerce_string(media.get("preview_image_url")).strip(),
                        )
                    )
                elif media.get("preview_image_url"):
                    dest.append(TweetMediaItem(kind="image", url=media["preview_image_url"]))

        if not attachment_media:
            _append_photo_media_from_entities(node, dest)
        legacy = node.get("legacy")
        if isinstance(legacy, dict):
            if not attachment_media:
                _append_photo_media_from_entities(legacy, dest)

        if node.get("type") == "photo":
            photo_url = node.get("url") or node.get("media_url_https") or node.get("media_url")
            if photo_url:
                dest.append(TweetMediaItem(kind="image", url=photo_url))
        elif node.get("type") in {"video", "animated_gif"}:
            variant = select_best_video_variant(node)
            if variant:
                dest.append(
                    TweetMediaItem(
                        kind="video",
                        url=variant[0],
                        content_type=variant[1],
                        poster_url=_coerce_string(node.get("preview_image_url")).strip(),
                    )
                )
            elif node.get("preview_image_url"):
                dest.append(TweetMediaItem(kind="image", url=node["preview_image_url"]))

    quote_tco_urls: set = set()
    primary_node = find_primary_tweet_node(data, original_url)
    if primary_node is not None:
        quote_tco_urls = _find_quote_tco_urls(primary_node)
        collect_text_from_node(primary_node)
        collect_media_from_node(primary_node)
        for ref_type in ("replied_to", "quoted"):
            finder = find_replied_to_tweet_nodes if ref_type == "replied_to" else find_quoted_tweet_nodes
            for ref_node in finder(data, primary_node):
                rt_media: List[TweetMediaItem] = []
                collect_media_from_node(ref_node, target=rt_media)
                rt_text = _extract_tweet_text_from_node(ref_node)
                rt_author, rt_display_name = _extract_user_info(data, ref_node)
                rt_meta = _extract_metadata_from_node(ref_node)
                rt_id = rt_meta.get("tweet_id", "")
                rt_url = f"https://twitter.com/{rt_author}/status/{rt_id}" if rt_author and rt_id else ""
                rt_created = _coerce_string(ref_node.get("created_at"))
                if rt_text or rt_media:
                    ref_tweets.append(ReferencedTweetInfo(
                        kind=ref_type, text=rt_text, author=rt_author,
                        display_name=rt_display_name, created_at=rt_created,
                        url=rt_url, media=tuple(rt_media),
                    ))
    else:
        for node in _iter_candidate_tweet_nodes(data):
            collect_text_from_node(node)
            collect_media_from_node(node)

    deduped_media_items: List[TweetMediaItem] = []
    seen = set()
    for media_item in media_items:
        dedupe_key = (media_item.kind, media_item.url)
        if media_item.url and dedupe_key not in seen:
            deduped_media_items.append(media_item)
            seen.add(dedupe_key)

    combined_text = "\n".join(texts)
    if quote_tco_urls:
        combined_text = _strip_tco_urls(combined_text, quote_tco_urls)

    return combined_text, deduped_media_items, extract_tweet_metadata(data, original_url), ref_tweets


def fetch_snapshot_json(snapshot_timestamp: str, original_url: str) -> str:
    """
    Retrieve the archived tweet JSON payload as raw text and validate that it parses.
    """
    snapshot_url = f"https://web.archive.org/web/{snapshot_timestamp}id_/{original_url}"
    try:
        resp = get_with_retry(snapshot_url, timeout=JSON_TIMEOUT)
        resp.json()
    except Exception:
        return ""
    return resp.text


def load_snapshot_json(json_path: Path) -> Optional[dict]:
    try:
        return json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_json_output_path(json_dir: Path, snapshot_timestamp: str, original_url: str, metadata: Dict[str, str]) -> Path:
    tweet_id = metadata.get("tweet_id") or ""
    if not tweet_id:
        match = TWEET_URL_ID_RE.search(original_url)
        tweet_id = match.group(1) if match else "unknown"
    return json_dir / f"{snapshot_timestamp}_{tweet_id}.json"


def write_snapshot_json(
    json_dir: Path,
    asset_dir_name: str,
    snapshot_timestamp: str,
    original_url: str,
    metadata: Dict[str, str],
    raw_json: str,
) -> str:
    json_path = build_json_output_path(json_dir, snapshot_timestamp, original_url, metadata)
    json_path.write_text(raw_json, encoding="utf-8")
    return f"{asset_dir_name}/json/{json_path.name}"


def ensure_snapshot_json(record: SnapshotRecord, resume: bool = True) -> str:
    if resume and record.json_path.exists() and load_snapshot_json(record.json_path) is not None:
        return "reused"

    raw_json = fetch_snapshot_json(record.timestamp, record.original_url)
    if not raw_json:
        return "missing"

    record.json_path.parent.mkdir(parents=True, exist_ok=True)
    record.json_path.write_text(raw_json, encoding="utf-8")
    return "downloaded"


def prepare_snapshot_json_and_media(
    record: SnapshotRecord,
    resume: bool = True,
    asset_dir_name: Optional[str] = None,
    media_dir: Optional[Path] = None,
    image_cache: Optional[Dict[str, str]] = None,
) -> str:
    json_status = ensure_snapshot_json(record, resume=resume)
    if json_status == "missing":
        return json_status
    if asset_dir_name and media_dir is not None:
        prefetch_snapshot_media(record, asset_dir_name, media_dir, image_cache)
    return json_status


def build_tweet_data_attributes(metadata: Dict[str, str], json_relative_path: str) -> str:
    attrs = {
        "data-tweet-id": metadata.get("tweet_id", ""),
        "data-conversation-id": metadata.get("conversation_id", ""),
        "data-in-reply-to-status-id": metadata.get("in_reply_to_status_id", ""),
        "data-in-reply-to-user-id": metadata.get("in_reply_to_user_id", ""),
        "data-in-reply-to-screen-name": metadata.get("in_reply_to_screen_name", ""),
        "data-is-reply": metadata.get("is_reply", "unknown"),
        "data-is-thread-root": metadata.get("is_thread_root", "unknown"),
        "data-is-repost": metadata.get("is_repost", "false"),
        "data-json-path": json_relative_path,
    }
    rendered = []
    for key, value in attrs.items():
        rendered.append(f"{key}='{html.escape(value, quote=True)}'")
    return " ".join(rendered)


def download_image_asset(
    image_url: str,
    snapshot_timestamp: str,
    media_dir: Path,
    asset_dir_name: str,
    cache: Optional[Dict[str, str]] = None,
) -> str:
    """
    Download an image once and return its relative asset path.
    """
    with IMAGE_CACHE_LOCK:
        if cache is not None and image_url in cache:
            cached_relative_path = cache[image_url]
            if (media_dir / Path(cached_relative_path).name).exists():
                return cached_relative_path
            cache.pop(image_url, None)

    candidate_urls = build_image_candidate_urls(image_url, snapshot_timestamp)
    resp = None
    for candidate_url in candidate_urls:
        try:
            resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
            break
        except Exception:
            resp = None
    if resp is None:
        for candidate_url in build_closest_capture_candidate_urls(image_url, snapshot_timestamp):
            try:
                resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
                break
            except Exception:
                resp = None
    if resp is None:
        return ""

    mime_type = resp.headers.get("Content-Type", "").split(";", 1)[0].strip()
    if not mime_type.startswith("image/"):
        guessed_mime = mimetypes.guess_type(image_url)[0] or ""
        mime_type = guessed_mime if guessed_mime.startswith("image/") else "image/jpeg"

    extension = guess_extension(image_url, mime_type)
    digest = hashlib.sha256(resp.content).hexdigest()[:24]
    filename = f"{digest}{extension}"
    destination = media_dir / filename

    with IMAGE_CACHE_LOCK:
        if not destination.exists():
            destination.write_bytes(resp.content)
        relative_path = f"{asset_dir_name}/media/{filename}"
        if cache is not None:
            cache[image_url] = relative_path
            persist_media_cache_entry(media_dir, cache)
    return relative_path


def download_video_asset(
    video_url: str,
    snapshot_timestamp: str,
    media_dir: Path,
    asset_dir_name: str,
    cache: Optional[Dict[str, str]] = None,
) -> str:
    """
    Download one archived video variant and return its relative asset path.
    """
    with IMAGE_CACHE_LOCK:
        if cache is not None and video_url in cache:
            cached_relative_path = cache[video_url]
            if (media_dir / Path(cached_relative_path).name).exists():
                return cached_relative_path
            cache.pop(video_url, None)

    candidate_urls = build_video_candidate_urls(video_url, snapshot_timestamp)
    resp = None
    for candidate_url in candidate_urls:
        try:
            resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
            break
        except Exception:
            resp = None
    if resp is None:
        for candidate_url in build_closest_capture_candidate_urls(video_url, snapshot_timestamp):
            try:
                resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
                break
            except Exception:
                resp = None
    if resp is None:
        return ""

    mime_type = resp.headers.get("Content-Type", "").split(";", 1)[0].strip()
    if not mime_type.startswith("video/"):
        guessed_mime = mimetypes.guess_type(video_url)[0] or ""
        mime_type = guessed_mime if guessed_mime.startswith("video/") else "video/mp4"

    extension = guess_extension(video_url, mime_type)
    digest = hashlib.sha256(resp.content).hexdigest()[:24]
    filename = f"{digest}{extension}"
    destination = media_dir / filename

    with IMAGE_CACHE_LOCK:
        if not destination.exists():
            destination.write_bytes(resp.content)
        relative_path = f"{asset_dir_name}/media/{filename}"
        if cache is not None:
            cache[video_url] = relative_path
            persist_media_cache_entry(media_dir, cache)
    return relative_path


def prefetch_snapshot_media(
    record: SnapshotRecord,
    asset_dir_name: str,
    media_dir: Path,
    image_cache: Optional[Dict[str, str]] = None,
) -> None:
    data = load_snapshot_json(record.json_path)
    if data is None:
        return

    _, media_items, _, ref_tweets = extract_tweet_content(data, record.original_url)
    all_media = list(media_items)
    for qt in ref_tweets:
        all_media.extend(qt.media)
    for media_item in all_media:
        if media_item.kind == "image":
            download_image_asset(media_item.url, record.timestamp, media_dir, asset_dir_name, image_cache)
        elif media_item.kind == "video":
            download_video_asset(media_item.url, record.timestamp, media_dir, asset_dir_name, image_cache)
            if media_item.poster_url:
                download_image_asset(media_item.poster_url, record.timestamp, media_dir, asset_dir_name, image_cache)


def collect_snapshot_media_jobs(record: SnapshotRecord) -> List[Tuple[str, str, str]]:
    data = load_snapshot_json(record.json_path)
    if data is None:
        return []

    _, media_items, _, ref_tweets = extract_tweet_content(data, record.original_url)
    jobs: List[Tuple[str, str, str]] = []

    def add_media_item(media_item: TweetMediaItem) -> None:
        if media_item.kind == "image" and media_item.url:
            jobs.append(("image", media_item.url, record.timestamp))
        elif media_item.kind == "video" and media_item.url:
            jobs.append(("video", media_item.url, record.timestamp))
            if media_item.poster_url:
                jobs.append(("image", media_item.poster_url, record.timestamp))

    for media_item in media_items:
        add_media_item(media_item)
    for ref_tweet in ref_tweets:
        for media_item in ref_tweet.media:
            add_media_item(media_item)

    deduped_jobs: List[Tuple[str, str, str]] = []
    seen = set()
    for kind, url, timestamp in jobs:
        key = (kind, url)
        if key in seen:
            continue
        seen.add(key)
        deduped_jobs.append((kind, url, timestamp))
    return deduped_jobs


def media_job_is_cached(media_dir: Path, cache: Optional[Dict[str, str]], url: str) -> bool:
    if cache is None:
        return False
    with IMAGE_CACHE_LOCK:
        relative_path = cache.get(url)
    if not relative_path:
        return False
    return (media_dir / Path(relative_path).name).exists()


def repair_missing_media_from_snapshot_records(
    records: List[SnapshotRecord],
    asset_dir: Path,
    workers: int,
    resume: bool = True,
) -> Tuple[int, int, int, int]:
    ensure_asset_directories(asset_dir)
    asset_dir_name = asset_dir.name
    media_dir = asset_dir / "media"
    image_cache = load_media_cache(asset_dir, asset_dir_name) if resume else {}

    unique_jobs: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
    for record in records:
        for kind, url, timestamp in collect_snapshot_media_jobs(record):
            unique_jobs.setdefault((kind, url), (kind, url, timestamp))

    total_jobs = len(unique_jobs)
    missing_jobs = [
        job for job in unique_jobs.values()
        if not media_job_is_cached(media_dir, image_cache, job[1])
    ]
    missing_before = len(missing_jobs)

    if not missing_jobs:
        write_media_cache(asset_dir, image_cache)
        return total_jobs, 0, 0, 0

    def run_job(job: Tuple[str, str, str]) -> bool:
        kind, url, timestamp = job
        if kind == "image":
            return bool(download_image_asset(url, timestamp, media_dir, asset_dir_name, image_cache))
        return bool(download_video_asset(url, timestamp, media_dir, asset_dir_name, image_cache))

    remaining_jobs = list(missing_jobs)

    for pass_index in range(1, DEFAULT_MEDIA_REPAIR_PASSES + 1):
        if not remaining_jobs:
            break
        print(
            f"Media repair pass {pass_index}/{DEFAULT_MEDIA_REPAIR_PASSES} "
            f"for {len(remaining_jobs)} missing targets...",
            flush=True,
        )

        pass_repaired = 0
        pass_failed = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = {}
            next_to_submit = 0

            while next_to_submit < len(remaining_jobs) and len(pending) < workers * 4:
                future = executor.submit(run_job, remaining_jobs[next_to_submit])
                pending[future] = remaining_jobs[next_to_submit]
                next_to_submit += 1

            completed_count = 0
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    job = pending.pop(future)
                    completed_count += 1
                    try:
                        success = future.result()
                    except Exception:
                        success = False
                    if success:
                        pass_repaired += 1
                    else:
                        pass_failed += 1

                while next_to_submit < len(remaining_jobs) and len(pending) < workers * 4:
                    future = executor.submit(run_job, remaining_jobs[next_to_submit])
                    pending[future] = remaining_jobs[next_to_submit]
                    next_to_submit += 1

                if completed_count % 25 == 0 or completed_count == len(remaining_jobs):
                    print(
                        f"Media repair progress: {completed_count}/{len(remaining_jobs)} checked, "
                        f"{pass_repaired} repaired this pass, {pass_failed} still failing this pass",
                        flush=True,
                    )

        write_media_cache(asset_dir, image_cache)
        remaining_jobs = [
            job for job in remaining_jobs
            if not media_job_is_cached(media_dir, image_cache, job[1])
        ]
        if remaining_jobs and pass_index < DEFAULT_MEDIA_REPAIR_PASSES:
            print(
                f"Retrying {len(remaining_jobs)}/{missing_before} still-missing media targets after a short backoff...",
                flush=True,
            )
            time.sleep(MEDIA_REPAIR_BACKOFF_SECONDS * pass_index)

    missing_after = len(remaining_jobs)
    repaired_count = missing_before - missing_after
    write_media_cache(asset_dir, image_cache)
    return total_jobs, missing_before, repaired_count, missing_after


def render_snapshot_entry(
    record: SnapshotRecord,
    index: int,
    asset_dir_name: str,
    media_dir: Path,
    image_cache: Optional[Dict[str, str]] = None,
) -> Optional[ArchiveEntry]:
    """
    Render one archived tweet snapshot from a local JSON file.
    """
    try:
        dt_obj = datetime.strptime(record.timestamp, "%Y%m%d%H%M%S")
        dt = dt_obj.strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        dt_obj = datetime.min
        dt = record.timestamp

    data = load_snapshot_json(record.json_path)
    if data is None:
        return None

    text, media_items, metadata, ref_tweets = extract_tweet_content(data, record.original_url)
    if not text and not media_items:
        return None

    safe_text = html.escape(text).replace("\n", "<br/>\n")
    text_html = f"  <p>{safe_text}</p>\n" if safe_text else ""
    safe_url = html.escape(record.original_url)
    json_relative_path = f"{asset_dir_name}/json/{record.json_path.name}"
    tweet_attrs = build_tweet_data_attributes(metadata, json_relative_path)

    media_html_parts = []
    for media_item in media_items:
        if media_item.kind == "image":
            image_path = download_image_asset(media_item.url, record.timestamp, media_dir, asset_dir_name, image_cache)
            if not image_path:
                continue
            media_html_parts.append(
                f"  <div class='tweet-media-item'><img src='{html.escape(image_path)}' alt='Archived media from tweet captured on {html.escape(dt)}' loading='lazy' decoding='async'/></div>\n"
            )
            continue
        if media_item.kind == "video":
            video_path = download_video_asset(media_item.url, record.timestamp, media_dir, asset_dir_name, image_cache)
            if not video_path:
                continue
            poster_attr = ""
            if media_item.poster_url:
                poster_path = download_image_asset(
                    media_item.poster_url,
                    record.timestamp,
                    media_dir,
                    asset_dir_name,
                    image_cache,
                )
                if poster_path:
                    poster_attr = f" poster='{html.escape(poster_path)}'"
            mime_type = media_item.content_type or "video/mp4"
            media_html_parts.append(
                "  <div class='tweet-media-item'>"
                f"<video class='tweet-video' controls preload='metadata' playsinline{poster_attr}>"
                f"<source src='{html.escape(video_path)}' type='{html.escape(mime_type)}'/>"
                "Your browser does not support the video tag."
                "</video></div>\n"
            )

    media_html = ""
    if media_html_parts:
        media_html = "  <div class='tweet-media'>\n" + "".join(media_html_parts) + "  </div>\n"

    quote_html = ""
    for rt in ref_tweets:
        rt_safe_text = html.escape(rt.text).replace("\n", "<br/>\n")
        # Author header: display name + @username
        rt_header_parts = []
        if rt.display_name:
            rt_header_parts.append(f"<span class='tweet-ref-name'>{html.escape(rt.display_name)}</span>")
        if rt.author:
            rt_header_parts.append(f"<span class='tweet-ref-handle'>@{html.escape(rt.author)}</span>")
        rt_header_html = f"    <div class='tweet-ref-header'>{' '.join(rt_header_parts)}</div>\n" if rt_header_parts else ""
        rt_text_html = f"    <p>{rt_safe_text}</p>\n" if rt_safe_text else ""
        rt_media_parts = []
        for rm in rt.media:
            if rm.kind == "image":
                rm_path = download_image_asset(rm.url, record.timestamp, media_dir, asset_dir_name, image_cache)
                if rm_path:
                    rt_media_parts.append(
                        f"      <div class='tweet-media-item'><img src='{html.escape(rm_path)}' loading='lazy' decoding='async'/></div>\n"
                    )
            elif rm.kind == "video":
                rm_path = download_video_asset(rm.url, record.timestamp, media_dir, asset_dir_name, image_cache)
                if rm_path:
                    rm_poster = ""
                    if rm.poster_url:
                        pp = download_image_asset(rm.poster_url, record.timestamp, media_dir, asset_dir_name, image_cache)
                        if pp:
                            rm_poster = f" poster='{html.escape(pp)}'"
                    rm_mime = rm.content_type or "video/mp4"
                    rt_media_parts.append(
                        f"      <div class='tweet-media-item'><video class='tweet-video' controls preload='metadata' playsinline{rm_poster}>"
                        f"<source src='{html.escape(rm_path)}' type='{html.escape(rm_mime)}'/></video></div>\n"
                    )
        rt_media_html = ""
        if rt_media_parts:
            rt_media_html = "    <div class='tweet-media'>\n" + "".join(rt_media_parts) + "    </div>\n"
        rt_time_html = f"    <span class='tweet-ref-time'>{html.escape(rt.created_at)}</span>\n" if rt.created_at else ""
        quote_html += (
            f"  <blockquote class='tweet-ref tweet-ref-{rt.kind}'>\n"
            f"{rt_header_html}"
            f"{rt_text_html}"
            f"{rt_media_html}"
            f"{rt_time_html}"
            "  </blockquote>\n"
        )

    block = (
        f"<div class='tweet' {tweet_attrs}>\n  <h3>{dt}</h3>\n{text_html}{media_html}{quote_html}  <p><a href='{safe_url}'>View original tweet</a></p>\n</div>\n"
    )
    return ArchiveEntry(
        block=block,
        dt=dt_obj,
        time_text=dt,
        body_text=text,
        body_length=len(text),
        entropy=calculate_entropy(normalize_text_for_entropy(text)),
        has_media=bool(media_html_parts),
        original_url=record.original_url,
        index=index,
    )


def extract_user_profile(user: str, json_dir: Path) -> Optional[UserProfile]:
    """Scan JSON files and return the most up-to-date profile for the target user."""
    best: Optional[dict] = None
    best_followers = -1
    for json_path in json_dir.glob("*.json"):
        data = load_snapshot_json(json_path)
        if data is None:
            continue
        includes = data.get("includes", {})
        if not isinstance(includes, dict):
            continue
        for u in includes.get("users", []):
            if not isinstance(u, dict):
                continue
            if _coerce_string(u.get("username")).lower() != user.lower():
                continue
            fc = 0
            pm = u.get("public_metrics")
            if isinstance(pm, dict):
                fc = pm.get("followers_count", 0) or 0
            if fc > best_followers:
                best_followers = fc
                best = u
    if best is None:
        return None
    pm = best.get("public_metrics", {}) if isinstance(best.get("public_metrics"), dict) else {}
    avatar_url = _coerce_string(best.get("profile_image_url"))
    # Request higher resolution avatar (400x400 instead of 48x48 _normal)
    if avatar_url:
        avatar_url = avatar_url.replace("_normal.", "_400x400.")
    return UserProfile(
        username=_coerce_string(best.get("username")),
        display_name=_coerce_string(best.get("name")),
        description=_coerce_string(best.get("description")),
        location=_coerce_string(best.get("location")),
        avatar_url=avatar_url,
        followers=pm.get("followers_count", 0) or 0,
        following=pm.get("following_count", 0) or 0,
    )


def render_document_start(user: str, total: int, asset_dir_name: str, title_suffix: str = "", profile: Optional[UserProfile] = None, avatar_path: str = "") -> str:
    title = f"Archived tweets of @{user}"
    if title_suffix:
        title = f"{title} ({title_suffix})"
    tweet_word = "tweet" if total == 1 else "tweets"
    subtitle = f"{total} archived {tweet_word}"

    profile_html = ""
    if profile:
        avatar_html = ""
        if avatar_path:
            avatar_html = f"    <img class='profile-avatar' src='{html.escape(avatar_path)}' alt='' />\n"
        name_html = f"    <div class='profile-name'>{html.escape(profile.display_name)}</div>\n" if profile.display_name else ""
        handle_html = f"    <div class='profile-handle'>@{html.escape(profile.username)}</div>\n" if profile.username else ""
        bio_html = f"    <p class='profile-bio'>{html.escape(profile.description)}</p>\n" if profile.description else ""
        location_html = f"    <span class='profile-location'>{html.escape(profile.location)}</span>\n" if profile.location else ""
        stats_html = (
            "    <div class='profile-stats'>\n"
            f"      <span><strong>{profile.following}</strong> Following</span>\n"
            f"      <span><strong>{profile.followers}</strong> Followers</span>\n"
            "    </div>\n"
        )
        profile_html = (
            "  <div class='profile'>\n"
            f"{avatar_html}"
            f"{name_html}"
            f"{handle_html}"
            f"{bio_html}"
            f"{location_html}"
            f"{stats_html}"
            "  </div>\n"
        )

    return (
        "<!DOCTYPE html>\n"
        "<html lang='en'>\n"
        "<head>\n"
        "<meta charset='UTF-8'>\n"
        "<meta name='viewport' content='width=device-width, initial-scale=1.0'>\n"
        f"<title>{html.escape(title)}</title>\n"
        f"<link rel='stylesheet' href='{html.escape(asset_dir_name)}/archive.css'>\n"
        "</head>\n"
        "<body>\n"
        "<div class='archive-shell'>\n"
        f"{profile_html}"
        "  <header class='archive-header'>\n"
        f"    <h1 class='archive-title'>{html.escape(title)}</h1>\n"
        f"    <p class='archive-subtitle'>{html.escape(subtitle)}</p>\n"
        "  </header>\n"
    )


def render_document_end(asset_dir_name: str) -> str:
    return f"  <script src='{html.escape(asset_dir_name)}/archive.js'></script>\n</div>\n</body>\n</html>"


def render_index_document(user: str, total: int, output_path: Path) -> str:
    title = f"{user} Archive"
    tweet_word = "tweet" if total == 1 else "tweets"
    cards = [
        (output_path.name, "Chronological", "Browse the archive from oldest to newest."),
        (f"{output_path.stem}_time_desc{output_path.suffix}", "Time Desc", "Browse the archive from newest to oldest."),
        (
            f"{output_path.stem}_media_first_time_desc{output_path.suffix}",
            "Media First",
            "Show posts with images first, then sort by newest first.",
        ),
        (
            f"{output_path.stem}_text_length_desc{output_path.suffix}",
            "Longest Text",
            "Sort by visible post text length from longest to shortest.",
        ),
        (
            f"{output_path.stem}_text_entropy_desc{output_path.suffix}",
            "Highest Entropy",
            "Sort by visible text character entropy from highest to lowest.",
        ),
    ]
    card_html = "\n".join(
        (
            f"      <a class='card' href='{html.escape(href)}'>\n"
            f"        <span class='eyebrow'>{html.escape(title)}</span>\n"
            f"        <h2>{html.escape(label)}</h2>\n"
            f"        <p>{html.escape(description)}</p>\n"
            "      </a>"
        )
        for href, label, description in cards
    )
    lead = f"{total} archived {tweet_word}"
    return (
        "<!DOCTYPE html>\n"
        "<html lang='en'>\n"
        "<head>\n"
        "<meta charset='UTF-8'>\n"
        "<meta name='viewport' content='width=device-width, initial-scale=1.0'>\n"
        f"<title>{html.escape(title)}</title>\n"
        f"<style>{INDEX_PAGE_CSS}</style>\n"
        "</head>\n"
        "<body>\n"
        "<main>\n"
        "  <section class='hero'>\n"
        "    <span class='kicker'>Wayback Archive</span>\n"
        f"    <h1>{html.escape(title)}</h1>\n"
        f"    <p class='lead'>{html.escape(lead)}</p>\n"
        "  </section>\n"
        "  <section class='grid'>\n"
        f"{card_html}\n"
        "  </section>\n"
        "  <p class='footer'>Generated by <a href='https://github.com/gfhdhytghd/webarchived_tweet_downloader'>webarchived_tweet_downloader</a></p>\n"
        "</main>\n"
        "</body>\n"
        "</html>\n"
    )


def write_index_html(user: str, total: int, output_path: Path) -> Path:
    index_path = output_path.with_name("index.html")
    index_path.write_text(render_index_document(user, total, output_path), encoding="utf-8")
    return index_path


def write_archive_html(
    entries: List[ArchiveEntry],
    user: str,
    output_path: Path,
    asset_dir_name: str,
    title_suffix: str = "",
    profile: Optional[UserProfile] = None,
    avatar_path: str = "",
) -> None:
    with output_path.open("w", encoding="utf-8") as f:
        f.write(render_document_start(user, len(entries), asset_dir_name, title_suffix, profile=profile, avatar_path=avatar_path))
        for entry in entries:
            f.write(entry.block)
        f.write(render_document_end(asset_dir_name))


def get_archive_output_jobs(output_path: Path) -> List[Tuple[str, str, Path]]:
    return [
        ("chronological", "", output_path),
        ("time_desc", "time desc", output_path.with_name(f"{output_path.stem}_time_desc{output_path.suffix}")),
        (
            "media_first_time_desc",
            "media first, time desc",
            output_path.with_name(f"{output_path.stem}_media_first_time_desc{output_path.suffix}"),
        ),
        (
            "text_length_desc",
            "text length desc",
            output_path.with_name(f"{output_path.stem}_text_length_desc{output_path.suffix}"),
        ),
        (
            "text_entropy_desc",
            "text entropy desc",
            output_path.with_name(f"{output_path.stem}_text_entropy_desc{output_path.suffix}"),
        ),
    ]


def write_additional_archive_outputs(
    entries: List[ArchiveEntry],
    user: str,
    output_path: Path,
    asset_dir_name: str,
    profile: Optional[UserProfile] = None,
    avatar_path: str = "",
) -> List[Path]:
    written_paths: List[Path] = []
    for mode, title_suffix, target in get_archive_output_jobs(output_path)[1:]:
        write_archive_html(sort_entries(entries, mode), user, target, asset_dir_name, title_suffix, profile=profile, avatar_path=avatar_path)
        written_paths.append(target)
    written_paths.append(write_index_html(user, len(entries), output_path))
    return written_paths


def write_all_archive_outputs(entries: List[ArchiveEntry], user: str, output_path: Path, asset_dir_name: str, profile: Optional[UserProfile] = None, avatar_path: str = "") -> List[Path]:
    chronological_mode, chronological_suffix, chronological_target = get_archive_output_jobs(output_path)[0]
    write_archive_html(
        sort_entries(entries, chronological_mode),
        user,
        chronological_target,
        asset_dir_name,
        chronological_suffix,
        profile=profile,
        avatar_path=avatar_path,
    )
    return [chronological_target] + write_additional_archive_outputs(entries, user, output_path, asset_dir_name, profile=profile, avatar_path=avatar_path)


def download_snapshot_records(
    records: List[SnapshotRecord],
    workers: int,
    resume: bool = True,
    asset_dir_name: Optional[str] = None,
    media_dir: Optional[Path] = None,
    image_cache: Optional[Dict[str, str]] = None,
) -> Tuple[int, int]:
    total = len(records)
    if total == 0:
        return 0, 0

    ready_records, missing_records = split_ready_and_missing_snapshot_records(records) if resume else ([], records[:])
    downloaded_count = 0
    ready_count = len(ready_records)

    if ready_count:
        print(f"Reusing {ready_count}/{total} existing JSON snapshots...", flush=True)

    for pass_index in range(1, DEFAULT_JSON_RETRY_PASSES + 1):
        if not missing_records:
            break

        if DEFAULT_JSON_RETRY_PASSES > 1:
            print(
                f"JSON fetch pass {pass_index}/{DEFAULT_JSON_RETRY_PASSES} for {len(missing_records)} unresolved snapshots...",
                flush=True,
            )

        completed_count = 0
        next_round_missing: List[SnapshotRecord] = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = {}
            next_to_submit = 0

            while next_to_submit < len(missing_records) and len(pending) < workers * 4:
                future = executor.submit(
                    prepare_snapshot_json_and_media,
                    missing_records[next_to_submit],
                    False,
                    asset_dir_name,
                    media_dir,
                    image_cache,
                )
                pending[future] = missing_records[next_to_submit]
                next_to_submit += 1

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    record = pending.pop(future)
                    completed_count += 1
                    try:
                        status = future.result()
                    except Exception:
                        status = "missing"

                    if status == "downloaded":
                        downloaded_count += 1
                        ready_count += 1
                    elif status != "reused":
                        next_round_missing.append(record)

                while next_to_submit < len(missing_records) and len(pending) < workers * 4:
                    future = executor.submit(
                        prepare_snapshot_json_and_media,
                        missing_records[next_to_submit],
                        False,
                        asset_dir_name,
                        media_dir,
                        image_cache,
                    )
                    pending[future] = missing_records[next_to_submit]
                    next_to_submit += 1

                if completed_count % 25 == 0 or completed_count == len(missing_records):
                    print(
                        f"JSON ready: {ready_count}/{total} (downloaded this run: {downloaded_count})",
                        flush=True,
                    )

        missing_records = next_round_missing
        if missing_records and pass_index < DEFAULT_JSON_RETRY_PASSES:
            print(
                f"Retrying {len(missing_records)}/{total} still-missing JSON snapshots after a short backoff...",
                flush=True,
            )
            time.sleep(JSON_RETRY_BACKOFF_SECONDS * pass_index)

    if missing_records:
        print(
            f"Still missing {len(missing_records)}/{total} JSON snapshots after {DEFAULT_JSON_RETRY_PASSES} passes.",
            flush=True,
        )

    return ready_count, total


def fetch_and_render_snapshot_entry(
    record: SnapshotRecord,
    index: int,
    asset_dir_name: str,
    media_dir: Path,
    image_cache: Optional[Dict[str, str]] = None,
    resume: bool = True,
) -> Tuple[str, Optional[ArchiveEntry]]:
    json_status = prepare_snapshot_json_and_media(
        record,
        resume=resume,
        asset_dir_name=asset_dir_name,
        media_dir=media_dir,
        image_cache=image_cache,
    )
    if json_status == "missing":
        return json_status, None
    return json_status, render_snapshot_entry(record, index, asset_dir_name, media_dir, image_cache)


def build_archives_from_snapshot_records(
    records: List[SnapshotRecord],
    user: str,
    output_path: Path,
    asset_dir: Path,
    workers: int,
    resume: bool = True,
) -> List[Path]:
    """
    Render local snapshot JSON files into the base archive plus four sorted variants.

    Parameters
    ----------
    records : List[SnapshotRecord]
        Snapshot metadata pointing at locally stored JSON files.
    user : str
        Twitter username.  Used for the title and header.
    output_path : Path
        Path where the base chronological HTML file will be written.
    asset_dir : Path
        Directory containing shared CSS, JS, JSON, and media assets.
    """
    ensure_static_files(asset_dir)
    media_dir = asset_dir / "media"
    asset_dir_name = asset_dir.name
    image_cache = load_media_cache(asset_dir, asset_dir_name) if resume else {}

    json_dir = asset_dir / "json"
    profile = extract_user_profile(user, json_dir)
    avatar_path = ""
    if profile and profile.avatar_url:
        avatar_path = download_image_asset(profile.avatar_url, "", media_dir, asset_dir_name, image_cache)

    total = len(records)
    next_to_submit = 0
    next_to_finalize = 0
    rendered_count = 0
    last_reported_rendered = -1
    finalized_entries: List[ArchiveEntry] = []
    completed: Dict[int, Optional[ArchiveEntry]] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {}

        while next_to_submit < total and len(pending) < workers * 4:
            future = executor.submit(
                render_snapshot_entry,
                records[next_to_submit],
                next_to_submit,
                asset_dir_name,
                media_dir,
                image_cache,
            )
            pending[future] = next_to_submit
            next_to_submit += 1

        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                index = pending.pop(future)
                try:
                    completed[index] = future.result()
                except Exception:
                    completed[index] = None

            while next_to_submit < total and len(pending) < workers * 4:
                future = executor.submit(
                    render_snapshot_entry,
                    records[next_to_submit],
                    next_to_submit,
                    asset_dir_name,
                    media_dir,
                    image_cache,
                )
                pending[future] = next_to_submit
                next_to_submit += 1

            while next_to_finalize in completed:
                entry = completed.pop(next_to_finalize)
                if entry is not None:
                    finalized_entries.append(entry)
                    rendered_count += 1
                next_to_finalize += 1
                if (
                    rendered_count != last_reported_rendered
                    and rendered_count
                    and (rendered_count % 25 == 0 or next_to_finalize == total)
                ):
                    print(f"Rendered {rendered_count}/{total} archive entries...", flush=True)
                    last_reported_rendered = rendered_count

    write_media_cache(asset_dir, image_cache)
    print(f"Archive entries rendered: {rendered_count}/{total}")
    return write_all_archive_outputs(finalized_entries, user, output_path, asset_dir_name, profile=profile, avatar_path=avatar_path)


def build_archives(snapshots: List[Tuple[str, str]], user: str, output_path: Path, asset_dir: Path, workers: int, resume: bool = True) -> List[Path]:
    ensure_asset_directories(asset_dir)
    records = build_snapshot_records(snapshots, asset_dir / "json")
    write_snapshot_manifest(asset_dir, user, records)
    ensure_static_files(asset_dir)
    media_dir = asset_dir / "media"
    asset_dir_name = asset_dir.name
    image_cache = load_media_cache(asset_dir, asset_dir_name) if resume else {}

    total = len(records)
    reused_json_count = count_existing_snapshot_json(records) if resume else 0
    ready_json_count = reused_json_count
    downloaded_json_count = 0
    next_to_submit = 0
    next_to_finalize = 0
    last_reported_ready_json = -1
    rendered_count = 0
    last_reported_rendered = -1
    finalized_entries: List[ArchiveEntry] = []
    completed: Dict[int, Tuple[str, Optional[ArchiveEntry]]] = {}

    if reused_json_count:
        print(f"Reusing {reused_json_count}/{total} existing JSON snapshots...", flush=True)
    print("Rebuilding HTML from available JSON in chronological order...", flush=True)

    with output_path.open("w", encoding="utf-8") as base_file:
        base_file.write(render_document_start(user, total, asset_dir_name))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = {}

            while next_to_submit < total and len(pending) < workers * 4:
                future = executor.submit(
                    fetch_and_render_snapshot_entry,
                    records[next_to_submit],
                    next_to_submit,
                    asset_dir_name,
                    media_dir,
                    image_cache,
                    resume,
                )
                pending[future] = next_to_submit
                next_to_submit += 1

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    index = pending.pop(future)
                    try:
                        completed[index] = future.result()
                    except Exception:
                        completed[index] = (False, None)

                while next_to_submit < total and len(pending) < workers * 4:
                    future = executor.submit(
                        fetch_and_render_snapshot_entry,
                        records[next_to_submit],
                        next_to_submit,
                        asset_dir_name,
                        media_dir,
                        image_cache,
                        resume,
                    )
                    pending[future] = next_to_submit
                    next_to_submit += 1

                while next_to_finalize in completed:
                    json_status, entry = completed.pop(next_to_finalize)
                    if json_status == "downloaded":
                        downloaded_json_count += 1
                        ready_json_count += 1
                    if entry is not None:
                        base_file.write(entry.block)
                        base_file.flush()
                        finalized_entries.append(entry)
                        rendered_count += 1
                    next_to_finalize += 1
                    if (
                        ready_json_count != last_reported_ready_json
                        and ready_json_count
                        and (ready_json_count % 25 == 0 or next_to_finalize == total)
                    ):
                        print(
                            f"JSON ready: {ready_json_count}/{total} (downloaded this run: {downloaded_json_count})",
                            flush=True,
                        )
                        last_reported_ready_json = ready_json_count
                    if (
                        rendered_count != last_reported_rendered
                        and rendered_count
                        and (rendered_count % 25 == 0 or next_to_finalize == total)
                    ):
                        print(f"Rendered {rendered_count}/{total} archive entries...", flush=True)
                        last_reported_rendered = rendered_count

        base_file.write(render_document_end(asset_dir_name))

    print(f"JSON snapshots ready: {ready_json_count}/{total}")
    print(f"JSON snapshots downloaded this run: {downloaded_json_count}")
    write_media_cache(asset_dir, image_cache)

    ready_records, missing_records = split_ready_and_missing_snapshot_records(records)
    if missing_records:
        print(
            f"Retrying {len(missing_records)}/{total} missing JSON snapshots before finalizing sorted outputs...",
            flush=True,
        )
        download_snapshot_records(
            missing_records,
            workers,
            resume=False,
            asset_dir_name=asset_dir_name,
            media_dir=media_dir,
            image_cache=image_cache,
        )
        ready_records, missing_records = split_ready_and_missing_snapshot_records(records)

    print(f"Finalizing HTML from {len(ready_records)}/{total} available JSON snapshots...", flush=True)
    return build_archives_from_snapshot_records(
        ready_records,
        user,
        output_path,
        asset_dir,
        workers,
        resume=resume,
    )


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download archived tweets into JSON and HTML archive bundles.",
    )
    parser.add_argument("twitter_username", help="Twitter/X username without the @ prefix")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--json-only",
        action="store_true",
        help="Only fetch snapshot JSON files and update the local manifest.",
    )
    mode_group.add_argument(
        "--html-from-json",
        "--html-only",
        dest="html_from_json",
        action="store_true",
        help="Skip snapshot JSON fetching and regenerate the HTML bundle from local JSON files.",
    )
    mode_group.add_argument(
        "--repair-media",
        action="store_true",
        help="Scan local JSON snapshots for missing media files and try to download them without regenerating HTML.",
    )
    parser.add_argument(
        "--workers",
        type=positive_int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Maximum concurrent workers for JSON/media processing (default: {DEFAULT_MAX_WORKERS})",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not reuse existing JSON or media files; fetch them again when applicable.",
    )
    parser.add_argument(
        "--refresh-snapshots",
        action="store_true",
        help="Ignore the local snapshots manifest and fetch the Wayback CDX snapshot list again.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    user = args.twitter_username.lstrip("@").strip()
    resume = not args.no_resume
    output_path, asset_dir, _, json_dir, _ = build_output_paths(user)

    if args.html_from_json or args.repair_media:
        print(f"Loading local snapshot JSON for @{user}...")
        records = load_snapshot_records(asset_dir, json_dir, user)
        if not records:
            print(f"No local JSON snapshots found in {json_dir}")
            sys.exit(1)
        available_records = [record for record in records if record.json_path.exists()]
        if not available_records:
            print(f"No JSON snapshot files are present in {json_dir}")
            sys.exit(1)
        if args.repair_media:
            print(f"Found {len(available_records)} local JSON snapshots. Repairing missing media...")
            total_jobs, missing_before, repaired_count, missing_after = repair_missing_media_from_snapshot_records(
                available_records,
                asset_dir,
                args.workers,
                resume=resume,
            )
            print(f"Unique media targets: {total_jobs}")
            print(f"Missing before repair: {missing_before}")
            print(f"Repaired this run: {repaired_count}")
            print(f"Still missing after repair: {missing_after}")
            print("Finished media repair stage.")
            print(f"  - {media_index_path(asset_dir)}")
            print(f"  - {asset_dir / 'media'}")
            return
        else:
            print(f"Found {len(available_records)} local JSON snapshots. Rendering HTML...")
            written_paths = build_archives_from_snapshot_records(
                available_records,
                user,
                output_path,
                asset_dir,
                args.workers,
                resume=resume,
            )
    else:
        print(f"Resolving snapshot list for @{user}...")
        records, record_source = resolve_snapshot_records(
            user,
            asset_dir,
            json_dir,
            resume=resume,
            refresh=args.refresh_snapshots,
        )
        if not records:
            print("No snapshots found or failed to retrieve list.")
            sys.exit(1)
        if record_source == "local":
            print(f"Loaded {len(records)} snapshot records from {snapshot_manifest_path(asset_dir)}")
        else:
            print(f"Fetched {len(records)} unique snapshots from Wayback CDX")
        snapshots = [(record.timestamp, record.original_url) for record in records]

        if args.json_only:
            ensure_asset_directories(asset_dir)
            write_snapshot_manifest(asset_dir, user, records)

            print(f"Found {len(records)} snapshot records. Resolving JSON...")
            available_count, total = download_snapshot_records(records, args.workers, resume=resume)
            print(f"JSON snapshots ready: {available_count}/{total}")
            print("Finished JSON download stage.")
            print(f"  - {snapshot_manifest_path(asset_dir)}")
            print(f"  - {json_dir}")
            return

        print(f"Found {len(snapshots)} snapshot records. Resolving JSON and rendering HTML...")
        written_paths = build_archives(
            snapshots,
            user,
            output_path,
            asset_dir,
            args.workers,
            resume=resume,
        )

    print("Finished. Wrote:")
    for path in written_paths:
        print(f"  - {path}")


if __name__ == "__main__":
    main()

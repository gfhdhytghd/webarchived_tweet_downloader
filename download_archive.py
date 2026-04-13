#!/usr/bin/env python3
"""
download_archive.py
====================

This script helps you collect archived tweets for a given Twitter account from
the Internet Archive's Wayback Machine and compile them into a single HTML
document.  Because the Wayback Machine stores each tweet as a JSON object,
the script queries the Wayback CDX API to locate all available snapshots of
tweets from the chosen account and then retrieves the raw JSON payload for
each snapshot.  It extracts the visible text of every tweet and writes a
chronological listing into an HTML file.

Usage:

    python download_archive.py <twitter_username> [output_html]

Example:

    python download_archive.py AnIncandescence anincandescence_archive.html

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

4.  This script does not filter or censor content.  If the account's
    timeline contains sensitive material, it will appear in the resulting
    HTML.  Please exercise caution when sharing or viewing the output.

Author: Assistant
Date: April 13, 2026 (America/New_York)
"""

import sys
import json
import html
import base64
import mimetypes
import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from typing import Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("This script requires the 'requests' library. You can install it with 'pip install requests'.")
    sys.exit(1)


CDX_BASE_URL = "https://web.archive.org/cdx/search/cdx"
USER_AGENT = "Mozilla/5.0 (compatible; archive-exporter/1.0; +https://web.archive.org)"
SESSION_LOCAL = threading.local()
MAX_WORKERS = 12
JSON_TIMEOUT = 15
IMAGE_TIMEOUT = 10
REQUEST_ATTEMPTS = 4


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


def get_with_retry(url: str, *, timeout: int, params: Optional[dict] = None) -> requests.Response:
    """
    Perform a GET request with a few retries and short exponential backoff.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            resp = get_session().get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
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


def _append_photo_urls_from_entities(container: dict, image_urls: List[str]) -> None:
    """
    Collect photo URLs from legacy Twitter entity payloads when present.
    """
    for entity_key in ("extended_entities", "entities"):
        entity_block = container.get(entity_key, {})
        media_items = entity_block.get("media", []) if isinstance(entity_block, dict) else []
        for media in media_items:
            if not isinstance(media, dict):
                continue
            if media.get("type") != "photo":
                continue
            media_url = media.get("media_url_https") or media.get("media_url") or media.get("url")
            if media_url:
                image_urls.append(media_url)


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


def extract_tweet_content(data: dict) -> Tuple[str, List[str]]:
    """
    Extract visible tweet text and photo URLs from a Wayback JSON payload.
    """
    texts: List[str] = []
    image_urls: List[str] = []
    media_by_key: Dict[str, dict] = {}
    seen_texts = set()

    includes = data.get("includes", {})
    if isinstance(includes, dict):
        for media in includes.get("media", []):
            if isinstance(media, dict) and media.get("media_key"):
                media_by_key[media["media_key"]] = media

    def collect_from_node(node) -> None:
        if isinstance(node, list):
            for item in node:
                collect_from_node(item)
            return
        if not isinstance(node, dict):
            return

        text = _extract_tweet_text_from_node(node)
        if text and text not in seen_texts:
            texts.append(text)
            seen_texts.add(text)

        attachments = node.get("attachments", {})
        media_keys = attachments.get("media_keys", []) if isinstance(attachments, dict) else []
        for media_key in media_keys:
            media = media_by_key.get(media_key)
            if not isinstance(media, dict):
                continue
            media_type = media.get("type")
            if media_type == "photo" and media.get("url"):
                image_urls.append(media["url"])
            elif media_type in {"video", "animated_gif"} and media.get("preview_image_url"):
                image_urls.append(media["preview_image_url"])

        _append_photo_urls_from_entities(node, image_urls)
        legacy = node.get("legacy")
        if isinstance(legacy, dict):
            _append_photo_urls_from_entities(legacy, image_urls)

        if node.get("type") == "photo":
            photo_url = node.get("url") or node.get("media_url_https") or node.get("media_url")
            if photo_url:
                image_urls.append(photo_url)
        elif node.get("type") in {"video", "animated_gif"} and node.get("preview_image_url"):
            image_urls.append(node["preview_image_url"])

        for value in node.values():
            collect_from_node(value)

    collect_from_node(data)

    deduped_image_urls: List[str] = []
    seen = set()
    for image_url in image_urls:
        if image_url and image_url not in seen:
            deduped_image_urls.append(image_url)
            seen.add(image_url)

    return "\n".join(texts), deduped_image_urls


def fetch_tweet_content(snapshot_timestamp: str, original_url: str) -> Tuple[str, List[str]]:
    """
    Retrieve the archived tweet JSON payload and extract text plus photo URLs.
    """
    snapshot_url = f"https://web.archive.org/web/{snapshot_timestamp}id_/{original_url}"
    try:
        resp = get_with_retry(snapshot_url, timeout=JSON_TIMEOUT)
        data = resp.json()
    except Exception:
        return "", []
    return extract_tweet_content(data)


def download_image_as_data_url(
    image_url: str,
    snapshot_timestamp: str,
    cache: Optional[Dict[str, str]] = None,
) -> str:
    """
    Download an image once and return it as a base64 data URL.
    """
    if cache is not None and image_url in cache:
        return cache[image_url]

    candidate_urls = [image_url, f"https://web.archive.org/web/{snapshot_timestamp}im_/{image_url}"]
    resp = None
    for candidate_url in candidate_urls:
        try:
            resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
            break
        except Exception:
            resp = None
    if resp is None:
        return ""

    mime_type = resp.headers.get("Content-Type", "").split(";", 1)[0].strip()
    if not mime_type.startswith("image/"):
        mime_type = mimetypes.guess_type(image_url)[0] or "application/octet-stream"

    data_url = f"data:{mime_type};base64,{base64.b64encode(resp.content).decode('ascii')}"
    if cache is not None:
        cache[image_url] = data_url
    return data_url


def render_snapshot_entry(snapshot: Tuple[str, str]) -> str:
    """
    Fetch one archived tweet snapshot and render it as an HTML fragment.
    """
    timestamp, original_url = snapshot
    try:
        dt = datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        dt = timestamp

    text, image_urls = fetch_tweet_content(timestamp, original_url)
    if not text and not image_urls:
        return ""

    safe_text = html.escape(text).replace("\n", "<br/>\n")
    text_html = f"  <p>{safe_text}</p>\n" if safe_text else ""
    safe_url = html.escape(original_url)

    media_html_parts = []
    for image_url in image_urls:
        data_url = download_image_as_data_url(image_url, timestamp)
        if not data_url:
            continue
        media_html_parts.append(
            f"  <div class='tweet-media-item'><img src='{data_url}' alt='Archived media from tweet captured on {html.escape(dt)}'/></div>\n"
        )

    media_html = ""
    if media_html_parts:
        media_html = "  <div class='tweet-media'>\n" + "".join(media_html_parts) + "  </div>\n"

    return (
        f"<div class='tweet'>\n  <h3>{dt}</h3>\n{text_html}{media_html}  <p><a href='{safe_url}'>View original tweet</a></p>\n</div>\n"
    )


def build_html(snapshots: List[Tuple[str, str]], user: str, outfile: str) -> None:
    """
    Download each tweet and write an HTML document with timestamped entries.

    Parameters
    ----------
    snapshots : List[Tuple[str, str]]
        List of (timestamp, original_url) pairs.
    user : str
        Twitter username.  Used for the title and header.
    outfile : str
        Path where the HTML file will be written.
    """
    with open(outfile, "w", encoding="utf-8") as f:
        f.write("<!DOCTYPE html>\n<html lang='en'>\n<head>\n<meta charset='UTF-8'>\n")
        f.write(f"<title>Archived tweets of @{html.escape(user)}</title>\n")
        f.write("<style>body{font-family:Arial, sans-serif;margin:40px;background-color:#f9f9f9;}"
                "h1{color:#333;} .tweet{border-bottom:1px solid #ccc;padding:15px 0;}"
                "h3{margin:0;color:#555;font-size:1.1em;} p{margin:8px 0;color:#222;}"
                ".tweet-media{display:flex;flex-wrap:wrap;gap:12px;margin:12px 0;}"
                ".tweet-media-item{max-width:min(100%, 420px);}"
                ".tweet-media-item img{display:block;max-width:100%;height:auto;border-radius:8px;border:1px solid #ddd;}"
                "a{color:#1a0dab;}\n</style>\n</head>\n<body>\n")
        f.write(f"<h1>Archived tweets of @{html.escape(user)}</h1>\n")
        total = len(snapshots)
        next_to_submit = 0
        next_to_write = 0
        completed: Dict[int, str] = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            pending = {}

            while next_to_submit < total and len(pending) < MAX_WORKERS * 4:
                future = executor.submit(render_snapshot_entry, snapshots[next_to_submit])
                pending[future] = next_to_submit
                next_to_submit += 1

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    index = pending.pop(future)
                    try:
                        completed[index] = future.result()
                    except Exception:
                        completed[index] = ""

                while next_to_submit < total and len(pending) < MAX_WORKERS * 4:
                    future = executor.submit(render_snapshot_entry, snapshots[next_to_submit])
                    pending[future] = next_to_submit
                    next_to_submit += 1

                while next_to_write in completed:
                    entry = completed.pop(next_to_write)
                    if entry:
                        f.write(entry)
                    next_to_write += 1
                    if next_to_write % 25 == 0:
                        print(f"Processed {next_to_write}/{total} archived tweets...", flush=True)
                        f.flush()

        f.write("</body>\n</html>")
        f.flush()


def main():
    if len(sys.argv) < 2:
        print("Usage: python download_archive.py <twitter_username> [output_html]")
        sys.exit(1)
    user = sys.argv[1].lstrip("@").strip()
    outfile = sys.argv[2] if len(sys.argv) > 2 else f"{user}_archive.html"
    print(f"Fetching list of archived tweets for @{user}...")
    snapshots = fetch_snapshots(user)
    if not snapshots:
        print("No snapshots found or failed to retrieve list.")
        sys.exit(1)
    print(f"Found {len(snapshots)} unique snapshots. Downloading content...")
    build_html(snapshots, user, outfile)
    print(f"Finished. Archive saved to {outfile}.")


if __name__ == "__main__":
    main()

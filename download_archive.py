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
    python download_archive.py <twitter_username> --html-from-json --no-media-repair
    X_BEARER_TOKEN=... python download_archive.py <twitter_username> --html-from-json --x-api-reply-chain-depth 8
    X_BEARER_TOKEN=... python download_archive.py <twitter_username> --repair-reply-chain-depth 8
    python download_archive.py <twitter_username> --reverse-resume

Example:

    python download_archive.py AnIncandescence
    python download_archive.py AnIncandescence --json-only --workers 24
    python download_archive.py AnIncandescence --html-from-json
    python download_archive.py AnIncandescence --html-from-json --no-media-repair
    X_BEARER_TOKEN=... python download_archive.py AnIncandescence --html-from-json --x-api-reply-chain-depth 8
    X_BEARER_TOKEN=... python download_archive.py AnIncandescence --repair-reply-chain-depth 8
    python download_archive.py AnIncandescence --reverse-resume

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
import base64
import contextlib
import sys
import hashlib
import hmac
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
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("This script requires the 'requests' library. You can install it with 'pip install requests'.")
    sys.exit(1)


CDX_BASE_URL = "https://web.archive.org/cdx/search/cdx"
X_API_BASE_URL = "https://api.x.com/2"
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
X_API_TIMEOUT = 15
MEDIA_CDX_EXISTS_TIMEOUT = 5
MEDIA_CDX_FALLBACK_LIMIT = 3
TWEET_CDX_FALLBACK_LIMIT = 10
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
MEDIA_CDX_EXISTS_CACHE: Dict[str, Optional[bool]] = {}
X_API_LOOKUP_LOCK = threading.RLock()
X_API_LOOKUP_CACHE: Dict[object, Tuple[Optional[dict], str]] = {}
TWEET_CDX_LOOKUP_LOCK = threading.RLock()
TWEET_CDX_LOOKUP_CACHE: Dict[Tuple[str, str, str], List[Tuple[str, str]]] = {}
WAYBACK_TWEET_PAYLOAD_LOCK = threading.RLock()
WAYBACK_TWEET_PAYLOAD_CACHE: Dict[Tuple[str, str, str], Tuple[Optional[dict], str, str]] = {}
LOCAL_TWEET_INDEX_LOCK = threading.RLock()
LOCAL_TWEET_INDEX_CACHE: Dict[str, Dict[str, List[Tuple[str, Path]]]] = {}
SNAPSHOT_MANIFEST_NAME = "snapshots.json"
MEDIA_INDEX_NAME = "media_index.json"
MEDIA_NEGATIVE_INDEX_NAME = "media_negative_index.json"
MEDIA_NEGATIVE_CACHE_VERSION = 1
X_API_JSON_CACHE_KEY = "_pale_fire"
X_API_REPLY_CHAINS_KEY = "x_api_reply_chains"
X_API_REPLY_CHAIN_CACHE_VERSION = 1
X_API_TWEET_FIELDS = (
    "attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,"
    "entities,geo,id,in_reply_to_user_id,lang,note_tweet,possibly_sensitive,public_metrics,"
    "referenced_tweets,reply_settings"
)
X_API_EXPANSIONS = "attachments.media_keys,author_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id"
X_API_USER_FIELDS = "created_at,description,entities,id,location,name,profile_image_url,protected,public_metrics,url,username,verified"
X_API_MEDIA_FIELDS = "height,media_key,preview_image_url,public_metrics,type,url,variants,width"
ARCHIVE_FILTER_CSS = """
.archive-controls{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 4px;padding:12px 16px;border-bottom:1px solid var(--line)}
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
.tweet-expand-btn{display:inline-flex;align-items:center;gap:4px;margin:8px 0 4px;padding:4px 12px;border:1px solid var(--line);border-radius:9999px;background:var(--bg);color:var(--accent);font-size:13px;font-weight:500;cursor:pointer;transition:background .15s,border-color .15s}
.tweet-expand-btn:hover{background:var(--hover);border-color:var(--accent)}
.tweet-comments{margin:4px 0}
.detail-panel{position:fixed;inset:0;z-index:100;display:flex}
.detail-panel[hidden]{display:none!important}
.detail-panel-backdrop{position:absolute;inset:0;background:rgba(0,0,0,.25)}
.detail-panel-content{position:relative;background:var(--bg);display:flex;flex-direction:column;overflow:hidden;z-index:1}
.detail-panel-header{display:flex;align-items:center;justify-content:space-between;padding:0 12px;border-bottom:1px solid var(--line);flex-shrink:0}
.detail-panel-tabs{display:flex;gap:0}
.detail-tab{padding:12px 16px;border:none;background:none;font-size:14px;font-weight:600;color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;transition:color .15s,border-color .15s}
.detail-tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.detail-tab:hover{color:var(--ink)}
.detail-panel-close{width:36px;height:36px;border:none;background:none;font-size:22px;color:var(--muted);cursor:pointer;border-radius:50%;display:flex;align-items:center;justify-content:center;flex-shrink:0;transition:background .15s,color .15s}
.detail-panel-close:hover{background:var(--hover);color:var(--ink)}
.detail-panel-body{flex:1;overflow-y:auto;-webkit-overflow-scrolling:touch}
.detail-tab-pane{padding:16px}
.detail-tab-pane[hidden]{display:none}
.detail-post h3{font-size:13px;font-weight:400;color:var(--muted)}
.detail-post p{margin:4px 0 0;font-size:15px;color:var(--ink);line-height:1.5;overflow-wrap:anywhere}
.detail-post .tweet-media{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:2px;margin:10px 0 4px;border-radius:16px;overflow:hidden}
.detail-post .tweet-media-item img{display:block;width:100%;height:auto;background:var(--line)}
.detail-post .tweet-media-item video{display:block;width:100%;height:auto;background:#000}
.detail-post .tweet-ref{margin:10px 0 4px;padding:12px;border:1px solid var(--line);border-radius:12px;background:var(--bg)}
.detail-post .tweet-ref p{font-size:14px}
.detail-post a{color:var(--accent);text-decoration:none}
.detail-post a:hover{text-decoration:underline}
.detail-tab-pane .tweet-ref{margin:0 0 12px;padding:12px;border:1px solid var(--line);border-radius:12px;background:var(--bg)}
.detail-tab-pane .tweet-ref p{margin:4px 0 0;font-size:14px;color:var(--ink);line-height:1.45}
.detail-tab-pane .tweet-ref-header{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap}
.detail-tab-pane .tweet-ref-name{font-size:14px;font-weight:700;color:var(--ink)}
.detail-tab-pane .tweet-ref-handle{font-size:13px;color:var(--muted)}
.detail-tab-pane .tweet-ref-time{display:block;margin-top:6px;font-size:12px;color:var(--muted)}
.detail-tab-pane .tweet-ref .tweet-media{display:grid;grid-template-columns:1fr;gap:2px;margin:8px 0 4px;border-radius:12px;overflow:hidden}
.detail-tab-pane .tweet-ref .tweet-media-item img{display:block;width:100%;height:auto;background:var(--line)}
.detail-empty{color:var(--muted);text-align:center;padding:32px 0;font-size:14px}
.detail-note{color:var(--muted);text-align:center;padding:8px 0;font-size:12px}
@media (min-width:601px){.detail-panel{justify-content:flex-end}.detail-panel-content{width:50%;max-width:600px;height:100vh;border-left:1px solid var(--line);box-shadow:-4px 0 24px rgba(0,0,0,.06)}}
@media (max-width:600px){.detail-panel{align-items:flex-end}.detail-panel-content{width:100%;max-height:80vh;border-radius:16px 16px 0 0;box-shadow:0 -4px 24px rgba(0,0,0,.12)}.detail-panel-body{overscroll-behavior:contain}}
"""
ARCHIVE_FILTER_JS = """
(() => {
  const TWEET_FILTERS = [
    {
      key: "repost",
      label: "\u9690\u85cf\u8f6c\u8d34",
      storageKey: `${getArchiveStorageNamespace()}-hide-reposts`,
      predicate: (tweet, firstSegment) => {
        const meta = tweet.dataset.isRepost;
        if (meta === "true") return true;
        if (meta === "false") return false;
        return /^RT\\s+@\\S+/i.test(firstSegment);
      },
    },
  ];

  const allTweets = Array.from(document.querySelectorAll(".tweet"));
  if (!allTweets.length) return;

  const controls = document.createElement("div");
  controls.className = "archive-controls";

  setupTweetFilters(allTweets, controls);
  setupCommentExpansion(allTweets, controls);
  insertControls(controls);
  setupDetailPanel();

  function insertControls(ctrls) {
    if (!ctrls.children.length) return;
    const intro = document.querySelector(".archive-subtitle");
    const shell = document.querySelector(".archive-shell");
    if (!shell) return;
    if (intro?.parentNode === shell) {
      shell.insertBefore(ctrls, intro.nextSibling);
    } else {
      shell.insertBefore(ctrls, shell.firstChild);
    }
  }

  function setupTweetFilters(tweets, ctrls) {
    const classifiedFilters = TWEET_FILTERS.map((filter) => ({ ...filter, tweets: [] }));

    tweets.forEach((tweet) => {
      const body = tweet.querySelector("p");
      const firstSegment = getFirstSegmentText(body);
      classifiedFilters.forEach((filter) => {
        const matches = filter.predicate(tweet, firstSegment);
        tweet.classList.toggle(`tweet-is-${filter.key}`, matches);
        if (matches) filter.tweets.push(tweet);
      });
    });

    const activeFilters = classifiedFilters.filter((f) => f.tweets.length);
    if (!activeFilters.length) return;

    activeFilters.forEach((filter) => {
      const label = document.createElement("label");
      label.className = "archive-toggle";
      label.innerHTML =
        `<input class="archive-toggle-input" type="checkbox" data-filter-key="${filter.key}"/>` +
        `<span class="archive-toggle-switch" aria-hidden="true"></span>` +
        `<span class="archive-toggle-label">${filter.label}</span>` +
        `<span class="archive-toggle-meta">${filter.tweets.length} / ${tweets.length}</span>`;
      ctrls.appendChild(label);

      const checkbox = label.querySelector("input");
      checkbox.checked = readFilterState(filter.storageKey);
      checkbox.addEventListener("change", () => {
        writeFilterState(filter.storageKey, checkbox.checked);
        applyTweetFilters(tweets, activeFilters, ctrls);
      });
    });

    applyTweetFilters(tweets, activeFilters, ctrls);
  }

  function setupCommentExpansion(tweets, ctrls) {
    const expandKey = `${getArchiveStorageNamespace()}-default-expand-comments`;
    const tweetsWithComments = tweets.filter((t) => t.querySelector(".tweet-comments"));
    if (!tweetsWithComments.length) return;

    const defaultExpand = readFilterState(expandKey);

    // Global toggle
    const toggle = document.createElement("label");
    toggle.className = "archive-toggle";
    toggle.innerHTML =
      `<input class="archive-toggle-input" type="checkbox" data-filter-key="expand-comments"/>` +
      `<span class="archive-toggle-switch" aria-hidden="true"></span>` +
      `<span class="archive-toggle-label">\u9ed8\u8ba4\u5c55\u5f00\u8bc4\u8bba\u533a</span>` +
      `<span class="archive-toggle-meta">${tweetsWithComments.length} / ${tweets.length}</span>`;
    ctrls.appendChild(toggle);

    const globalCheckbox = toggle.querySelector("input");
    globalCheckbox.checked = defaultExpand;
    globalCheckbox.addEventListener("change", () => {
      writeFilterState(expandKey, globalCheckbox.checked);
      tweetsWithComments.forEach((tweet) => setCommentState(tweet, globalCheckbox.checked));
    });

    // Per-tweet buttons
    tweetsWithComments.forEach((tweet) => {
      setCommentState(tweet, defaultExpand);
      const btn = tweet.querySelector(".tweet-expand-btn");
      if (btn) {
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          const comments = tweet.querySelector(".tweet-comments");
          if (comments) setCommentState(tweet, comments.hidden);
        });
      }
    });
  }

  function setCommentState(tweet, expanded) {
    const comments = tweet.querySelector(".tweet-comments");
    const btn = tweet.querySelector(".tweet-expand-btn");
    if (comments) comments.hidden = !expanded;
    if (btn) {
      const count = btn.dataset.commentCount;
      btn.textContent = expanded
        ? `\u6536\u8d77\u8bc4\u8bba (${count})`
        : `\u5c55\u5f00\u8bc4\u8bba (${count})`;
    }
  }

  function setupDetailPanel() {
    const panel = document.querySelector(".detail-panel");
    if (!panel) return;
    const backdrop = panel.querySelector(".detail-panel-backdrop");
    const closeBtn = panel.querySelector(".detail-panel-close");
    const tabs = Array.from(panel.querySelectorAll(".detail-tab"));
    const postPane = panel.querySelector("[data-tab-content='post']");
    const commentsPane = panel.querySelector("[data-tab-content='comments']");

    document.querySelectorAll(".tweet").forEach((tweet) => {
      tweet.addEventListener("click", (e) => {
        if (e.target.closest("a, button, video, input, .lightbox")) return;
        openPanel(tweet);
      });
    });

    function openPanel(tweet) {
      // Post tab: clone tweet without comments section and expand button
      const clone = tweet.cloneNode(true);
      const cd = clone.querySelector(".tweet-comments");
      const eb = clone.querySelector(".tweet-expand-btn");
      if (cd) cd.remove();
      if (eb) eb.remove();
      clone.removeAttribute("class");
      clone.className = "detail-post";
      postPane.innerHTML = "";
      postPane.appendChild(clone);

      // Comments tab
      const comments = tweet.querySelector(".tweet-comments");
      const commentsTab = tabs.find((t) => t.dataset.tab === "comments");
      if (comments && comments.children.length) {
        commentsPane.innerHTML =
          comments.innerHTML +
          "<p class='detail-note'>\u4ec5\u5305\u542b\u88ab\u4f5c\u8005\u4eb2\u81ea\u56de\u590d\u8fc7\u7684\u8bc4\u8bba</p>";
        if (commentsTab) commentsTab.textContent = `\u8bc4\u8bba\u533a (${comments.children.length})`;
      } else {
        commentsPane.innerHTML =
          "<p class='detail-empty'>\u6682\u65e0\u8bc4\u8bba</p>" +
          "<p class='detail-note'>\u4ec5\u5305\u542b\u88ab\u4f5c\u8005\u4eb2\u81ea\u56de\u590d\u8fc7\u7684\u8bc4\u8bba</p>";
        if (commentsTab) commentsTab.textContent = "\u8bc4\u8bba\u533a";
      }

      switchTab("post");
      panel.hidden = false;
      document.body.classList.add("detail-panel-open");
    }

    function closePanel() {
      panel.hidden = true;
      document.body.classList.remove("detail-panel-open");
    }

    function switchTab(name) {
      tabs.forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
      panel.querySelectorAll("[data-tab-content]").forEach((c) => {
        c.hidden = c.dataset.tabContent !== name;
      });
    }

    if (backdrop) backdrop.addEventListener("click", closePanel);
    if (closeBtn) closeBtn.addEventListener("click", closePanel);
    tabs.forEach((tab) => tab.addEventListener("click", () => switchTab(tab.dataset.tab)));

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !panel.hidden) closePanel();
    });
  }

  function applyTweetFilters(tweets, filters, ctrls) {
    const enabledFilters = new Set(
      filters
        .filter((f) => ctrls.querySelector(`[data-filter-key="${f.key}"]`)?.checked)
        .map((f) => f.key),
    );
    tweets.forEach((tweet) => {
      const shouldHide = filters.some(
        (f) => enabledFilters.has(f.key) && tweet.classList.contains(`tweet-is-${f.key}`),
      );
      tweet.hidden = shouldHide;
    });
  }

  function getFirstSegmentText(body) {
    if (!body) return "";
    const [firstSegmentHtml = ""] = body.innerHTML.split(/<br\\s*\\/?>/i);
    const scratch = document.createElement("div");
    scratch.innerHTML = firstSegmentHtml;
    return (scratch.textContent || "").trim();
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
.tweet{padding:12px 16px;border-bottom:1px solid var(--line);transition:background .15s;cursor:pointer}
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
.tweet-ref-missing{border-style:dashed;background:var(--hover)}
.tweet-ref-missing p{color:var(--muted)}
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
class ReplyChainLookupContext:
    json_dir: Optional[Path] = None
    snapshot_timestamp: str = ""
    author_hint: str = ""
    use_local_json: bool = True
    use_wayback: bool = True


@dataclass(frozen=True)
class XApiOAuth1Credentials:
    api_key: str = ""
    api_key_secret: str = ""
    access_token: str = ""
    access_token_secret: str = ""

    def is_complete(self) -> bool:
        return all((self.api_key, self.api_key_secret, self.access_token, self.access_token_secret))

    def cache_identity(self) -> str:
        raw = f"{self.api_key}\0{self.access_token}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class XApiAuth:
    bearer_token: str = ""
    oauth1: Optional[XApiOAuth1Credentials] = None

    def has_network_auth(self) -> bool:
        return bool(self.bearer_token) or (self.oauth1 is not None and self.oauth1.is_complete())

    def __bool__(self) -> bool:
        return self.has_network_auth()

    def describe(self) -> str:
        methods = []
        if self.oauth1 is not None and self.oauth1.is_complete():
            methods.append("OAuth 1.0a user context")
        if self.bearer_token:
            methods.append("Bearer token")
        return " + ".join(methods) if methods else "no X API credentials"


@dataclass(frozen=True)
class ReferencedTweetInfo:
    kind: str  # "quoted" or "replied_to"
    text: str
    author: str
    display_name: str
    created_at: str
    url: str
    media: Tuple["TweetMediaItem", ...] = ()
    unavailable_reason: str = ""


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
        media_dir = urlunparse(parsed._replace(path="/media/", query=""))
        add(media_dir, "prefix")

    if parsed.netloc.endswith("video.twimg.com"):
        path = parsed.path
        parent_path = str(Path(path).parent)
        if parent_path and parent_path != "." and parent_path != "/":
            add(urlunparse(parsed._replace(path=parent_path.rstrip("/") + "/", query="")), "prefix")

        root_prefixes = []
        for marker in ("/vid/", "/pl/"):
            if marker in path:
                before, _ = path.split(marker, 1)
                root_prefixes.append(before + marker)
        for prefix_path in root_prefixes:
            add(urlunparse(parsed._replace(path=prefix_path, query="")), "prefix")
            if prefix_path.endswith("/vid/"):
                add(urlunparse(parsed._replace(path=prefix_path[:-5] + "/pl/", query="")), "prefix")
            if prefix_path.endswith("/pl/"):
                add(urlunparse(parsed._replace(path=prefix_path[:-4] + "/vid/", query="")), "prefix")

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

    requested = urlparse(asset_url)
    requested_suffix = Path(requested.path).suffix.lower()

    def original_matches_request(original_url: str) -> bool:
        parsed = urlparse(original_url)
        suffix = Path(parsed.path).suffix.lower()
        if requested.netloc.endswith("video.twimg.com"):
            if requested_suffix == ".mp4":
                return suffix == ".mp4"
            if requested_suffix == ".m3u8":
                return suffix == ".m3u8"
        if requested.netloc.endswith("pbs.twimg.com"):
            if requested.path.startswith("/media/"):
                return parsed.netloc.endswith("pbs.twimg.com") and parsed.path.startswith("/media/")
            return parsed.netloc.endswith("pbs.twimg.com")
        return True

    for source_url, match_type in build_asset_lookup_queries(asset_url):
        for capture_timestamp, original_url in query_closest_media_captures(source_url, snapshot_timestamp, match_type=match_type):
            if not original_matches_request(original_url):
                continue
            for candidate in build_archived_urls_for_source_url(original_url, capture_timestamp):
                add(candidate)

    return candidates


def media_cdx_has_any_capture(asset_url: str) -> Optional[bool]:
    with MEDIA_CDX_LOOKUP_LOCK:
        cached = MEDIA_CDX_EXISTS_CACHE.get(asset_url)
        if cached is not None or asset_url in MEDIA_CDX_EXISTS_CACHE:
            return cached

    params = {
        "url": asset_url,
        "output": "json",
        "fl": "timestamp",
        "limit": "1",
    }
    try:
        resp = get_session().get(CDX_BASE_URL, params=params, timeout=MEDIA_CDX_EXISTS_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        result: Optional[bool] = None
    else:
        result = isinstance(payload, list) and len(payload) > 1

    with MEDIA_CDX_LOOKUP_LOCK:
        MEDIA_CDX_EXISTS_CACHE[asset_url] = result
    return result


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


def media_negative_index_path(asset_dir: Path) -> Path:
    return asset_dir / MEDIA_NEGATIVE_INDEX_NAME


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


def snapshot_record_tweet_id(record: SnapshotRecord) -> str:
    match = TWEET_URL_ID_RE.search(record.original_url)
    if match:
        return match.group(1)
    parsed = parse_snapshot_filename(record.json_path.name)
    if parsed:
        return parsed[1]
    return record.original_url


def print_verbose_status(record: SnapshotRecord, success: bool, failure_detail: str = "") -> None:
    if success:
        print(f"{snapshot_record_tweet_id(record)}:success", flush=True)
        return
    detail = failure_detail or "http=unknown"
    print(f"{snapshot_record_tweet_id(record)}:failed:{detail}", flush=True)


def extract_http_status_code(exc: Exception) -> str:
    cause = exc.__cause__ if isinstance(exc, RuntimeError) and exc.__cause__ is not None else exc
    if isinstance(cause, requests.HTTPError) and cause.response is not None:
        return str(cause.response.status_code)
    return ""


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


def order_snapshot_records_for_resume(
    records: List[SnapshotRecord],
    *,
    resume: bool,
    reverse_resume: bool,
) -> List[SnapshotRecord]:
    ordered = list(records)
    if resume and reverse_resume:
        ordered.reverse()
    return ordered


def order_indexed_snapshot_records_for_resume(
    records: List[SnapshotRecord],
    *,
    resume: bool,
    reverse_resume: bool,
) -> List[Tuple[int, SnapshotRecord]]:
    ordered = list(enumerate(records))
    if resume and reverse_resume:
        ordered.reverse()
    return ordered


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


def load_negative_media_cache(asset_dir: Path) -> Dict[str, dict]:
    cache_path = media_negative_index_path(asset_dir)
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("version") != MEDIA_NEGATIVE_CACHE_VERSION:
        return {}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return {}

    cleaned: Dict[str, dict] = {}
    for media_url, meta in entries.items():
        if not isinstance(media_url, str) or not isinstance(meta, dict):
            continue
        status = _coerce_string(meta.get("status")).strip()
        reason = _coerce_string(meta.get("reason")).strip()
        if not status and reason in {"hard-missing", "no-capture"}:
            status = "404"
        if not status and not reason:
            continue
        cleaned[media_url] = {
            "status": status,
            "reason": reason,
        }
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


def _clean_negative_media_cache_entries(cache: Dict[str, dict]) -> Dict[str, dict]:
    cleaned: Dict[str, dict] = {}
    for media_url, meta in cache.items():
        if not isinstance(media_url, str) or not isinstance(meta, dict):
            continue
        status = _coerce_string(meta.get("status")).strip()
        reason = _coerce_string(meta.get("reason")).strip()
        if not status and reason in {"hard-missing", "no-capture"}:
            status = "404"
        if not status and not reason:
            continue
        cleaned[media_url] = {
            "status": status,
            "reason": reason,
        }
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


def write_negative_media_cache(asset_dir: Path, cache: Dict[str, dict]) -> None:
    cache_path = media_negative_index_path(asset_dir)
    with IMAGE_CACHE_LOCK:
        with locked_media_index(asset_dir):
            disk_cache = load_negative_media_cache(asset_dir)
            merged_cache = _clean_negative_media_cache_entries(disk_cache)
            merged_cache.update(_clean_negative_media_cache_entries(cache))
            cache.clear()
            cache.update(merged_cache)
            payload = {
                "version": MEDIA_NEGATIVE_CACHE_VERSION,
                "entries": dict(sorted(merged_cache.items())),
            }
            tmp_path = cache_path.with_name(f"{cache_path.name}.{os.getpid()}.tmp")
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            tmp_path.replace(cache_path)


def persist_media_cache_entry(media_dir: Path, cache: Optional[Dict[str, str]]) -> None:
    if cache is None:
        return
    write_media_cache(media_dir.parent, cache)


def persist_negative_media_cache_entry(media_dir: Path, cache: Optional[Dict[str, dict]]) -> None:
    if cache is None:
        return
    write_negative_media_cache(media_dir.parent, cache)


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


def parse_env_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value


def load_env_file(path: Path, *, override: bool = False) -> int:
    if not path.exists() or not path.is_file():
        return 0

    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            continue
        if not override and key in os.environ:
            continue
        os.environ[key] = parse_env_value(value)
        loaded += 1
    return loaded


def get_with_retry(
    url: str,
    *,
    timeout: int,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> requests.Response:
    """
    Perform a GET request with a few retries and short exponential backoff.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        request_url = url
        try:
            while True:
                try:
                    resp = get_session().get(request_url, params=params, headers=headers, timeout=timeout)
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


def get_x_bearer_token(env_name: str = "X_BEARER_TOKEN") -> str:
    env_names = [env_name, "X_BEARER_TOKEN", "TWITTER_BEARER_TOKEN"]
    for name in env_names:
        token = os.environ.get(name, "").strip()
        if token:
            return token
    return ""


def _get_first_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def get_x_oauth1_credentials() -> Optional[XApiOAuth1Credentials]:
    credentials = XApiOAuth1Credentials(
        api_key=_get_first_env(
            "X_API_KEY",
            "X_CONSUMER_KEY",
            "X_CUSTOMER_KEY",
            "TWITTER_API_KEY",
            "TWITTER_CONSUMER_KEY",
            "TWITTER_CUSTOMER_KEY",
        ),
        api_key_secret=_get_first_env(
            "X_API_KEY_SECRET",
            "X_CONSUMER_SECRET",
            "X_CUSTOMER_KEY_SECRET",
            "TWITTER_API_KEY_SECRET",
            "TWITTER_CONSUMER_SECRET",
            "TWITTER_CUSTOMER_KEY_SECRET",
        ),
        access_token=_get_first_env("X_ACCESS_TOKEN", "X_OAUTH_TOKEN", "TWITTER_ACCESS_TOKEN", "TWITTER_OAUTH_TOKEN"),
        access_token_secret=_get_first_env(
            "X_ACCESS_TOKEN_SECRET",
            "X_OAUTH_TOKEN_SECRET",
            "TWITTER_ACCESS_TOKEN_SECRET",
            "TWITTER_OAUTH_TOKEN_SECRET",
        ),
    )
    return credentials if credentials.is_complete() else None


def get_x_api_auth(bearer_env_name: str = "X_BEARER_TOKEN") -> XApiAuth:
    return XApiAuth(
        bearer_token=get_x_bearer_token(bearer_env_name),
        oauth1=get_x_oauth1_credentials(),
    )


def normalize_x_api_auth(value) -> XApiAuth:
    if isinstance(value, XApiAuth):
        return value
    if isinstance(value, str):
        return XApiAuth(bearer_token=value.strip())
    if value is None:
        return XApiAuth()
    return XApiAuth(bearer_token=str(value).strip())


def _oauth_percent_encode(value) -> str:
    return quote(str(value), safe="~-._")


def build_oauth1_authorization_header(
    method: str,
    url: str,
    params: Optional[dict],
    credentials: XApiOAuth1Credentials,
    *,
    timestamp: Optional[int] = None,
    nonce: Optional[str] = None,
) -> str:
    parsed = urlparse(url)
    base_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
    oauth_params = {
        "oauth_consumer_key": credentials.api_key,
        "oauth_nonce": nonce or hashlib.sha256(os.urandom(32)).hexdigest(),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(timestamp if timestamp is not None else int(time.time())),
        "oauth_token": credentials.access_token,
        "oauth_version": "1.0",
    }

    signature_params: List[Tuple[str, str]] = []
    signature_params.extend((key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True))
    if params:
        for key, value in params.items():
            if isinstance(value, (list, tuple)):
                signature_params.extend((str(key), str(item)) for item in value)
            else:
                signature_params.append((str(key), str(value)))
    signature_params.extend(oauth_params.items())
    encoded_params = sorted((_oauth_percent_encode(key), _oauth_percent_encode(value)) for key, value in signature_params)
    parameter_string = "&".join(f"{key}={value}" for key, value in encoded_params)
    signature_base = "&".join(
        (
            method.upper(),
            _oauth_percent_encode(base_url),
            _oauth_percent_encode(parameter_string),
        )
    )
    signing_key = f"{_oauth_percent_encode(credentials.api_key_secret)}&{_oauth_percent_encode(credentials.access_token_secret)}"
    signature = base64.b64encode(
        hmac.new(signing_key.encode("utf-8"), signature_base.encode("utf-8"), hashlib.sha1).digest()
    ).decode("ascii")
    oauth_params["oauth_signature"] = signature

    header_params = ", ".join(
        f'{_oauth_percent_encode(key)}="{_oauth_percent_encode(value)}"'
        for key, value in sorted(oauth_params.items())
    )
    return f"OAuth {header_params}"


def format_api_error_payload(payload: dict) -> str:
    errors = payload.get("errors", []) if isinstance(payload, dict) else []
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict):
            detail = _coerce_string(first.get("detail")).strip()
            title = _coerce_string(first.get("title")).strip()
            if detail:
                return detail
            if title:
                return title
    return ""


def fetch_x_api_tweet_payload(tweet_id: str, bearer_token: str) -> Tuple[Optional[dict], str]:
    with X_API_LOOKUP_LOCK:
        cached = X_API_LOOKUP_CACHE.get(tweet_id)
        if cached is not None:
            return cached

    params = {
        "tweet.fields": X_API_TWEET_FIELDS,
        "expansions": X_API_EXPANSIONS,
        "user.fields": X_API_USER_FIELDS,
        "media.fields": X_API_MEDIA_FIELDS,
    }
    headers = {"Authorization": f"Bearer {bearer_token}"}
    try:
        resp = get_with_retry(f"{X_API_BASE_URL}/tweets/{tweet_id}", params=params, headers=headers, timeout=X_API_TIMEOUT)
        payload = resp.json()
    except Exception as exc:
        cause = exc.__cause__ if isinstance(exc, RuntimeError) and exc.__cause__ is not None else exc
        if isinstance(cause, requests.HTTPError) and cause.response is not None:
            try:
                payload = cause.response.json()
            except Exception:
                payload = {}
            error = format_api_error_payload(payload) or f"X API returned HTTP {cause.response.status_code}."
        else:
            error = f"X API lookup failed: {exc}"
        result = (None, error)
        with X_API_LOOKUP_LOCK:
            X_API_LOOKUP_CACHE[tweet_id] = result
        return result

    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        error = format_api_error_payload(payload) or "X API response did not include this post."
        result = (None, error)
        with X_API_LOOKUP_LOCK:
            X_API_LOOKUP_CACHE[tweet_id] = result
        return result

    result = (payload, "")
    with X_API_LOOKUP_LOCK:
        X_API_LOOKUP_CACHE[tweet_id] = result
    return result


def fetch_x_api_tweet_payload_oauth1(tweet_id: str, credentials: XApiOAuth1Credentials) -> Tuple[Optional[dict], str]:
    cache_key = ("oauth1", credentials.cache_identity(), tweet_id)
    with X_API_LOOKUP_LOCK:
        cached = X_API_LOOKUP_CACHE.get(cache_key)
        if cached is not None:
            return cached

    params = {
        "tweet.fields": X_API_TWEET_FIELDS,
        "expansions": X_API_EXPANSIONS,
        "user.fields": X_API_USER_FIELDS,
        "media.fields": X_API_MEDIA_FIELDS,
    }
    url = f"{X_API_BASE_URL}/tweets/{tweet_id}"
    headers = {"Authorization": build_oauth1_authorization_header("GET", url, params, credentials)}
    try:
        resp = get_with_retry(url, params=params, headers=headers, timeout=X_API_TIMEOUT)
        payload = resp.json()
    except Exception as exc:
        cause = exc.__cause__ if isinstance(exc, RuntimeError) and exc.__cause__ is not None else exc
        if isinstance(cause, requests.HTTPError) and cause.response is not None:
            try:
                payload = cause.response.json()
            except Exception:
                payload = {}
            error = format_api_error_payload(payload) or f"X API OAuth 1.0a returned HTTP {cause.response.status_code}."
        else:
            error = f"X API OAuth 1.0a lookup failed: {exc}"
        result = (None, error)
        with X_API_LOOKUP_LOCK:
            X_API_LOOKUP_CACHE[cache_key] = result
        return result

    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        error = format_api_error_payload(payload) or "X API OAuth 1.0a response did not include this post."
        result = (None, error)
        with X_API_LOOKUP_LOCK:
            X_API_LOOKUP_CACHE[cache_key] = result
        return result

    result = (payload, "")
    with X_API_LOOKUP_LOCK:
        X_API_LOOKUP_CACHE[cache_key] = result
    return result


def fetch_x_api_tweet_payload_with_auth(auth_value, tweet_id: str) -> Tuple[Optional[dict], str, str]:
    auth = normalize_x_api_auth(auth_value)
    errors: List[str] = []

    if auth.oauth1 is not None and auth.oauth1.is_complete():
        payload, error = fetch_x_api_tweet_payload_oauth1(tweet_id, auth.oauth1)
        if payload is not None:
            return payload, "", "x_api_oauth1"
        if error:
            errors.append(error)

    if auth.bearer_token:
        payload, error = fetch_x_api_tweet_payload(tweet_id, auth.bearer_token)
        if payload is not None:
            return payload, "", "x_api_bearer"
        if error:
            errors.append(error)

    return None, " ".join(errors) if errors else "X API credentials were not configured.", ""


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
        container.get("note_tweet", {}).get("text"),
        container.get("note_tweet_results", {}).get("result", {}).get("text"),
        container.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {}).get("text"),
    ]
    legacy = container.get("legacy")
    if isinstance(legacy, dict):
        candidates.extend(
            [
                legacy.get("note_tweet", {}).get("text"),
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


def _is_extractable_tweet_node(node: dict) -> bool:
    if not isinstance(node, dict):
        return False
    if _extract_tweet_text_from_node(node):
        return True
    legacy = node.get("legacy") if isinstance(node.get("legacy"), dict) else {}
    tweet_markers = (
        "author_id",
        "created_at",
        "conversation_id",
        "attachments",
        "entities",
        "extended_entities",
        "public_metrics",
        "note_tweet",
        "core",
    )
    legacy_markers = (
        "created_at",
        "full_text",
        "text",
        "user_id_str",
        "conversation_id_str",
        "entities",
        "extended_entities",
        "favorite_count",
        "retweet_count",
    )
    return any(marker in node for marker in tweet_markers) or any(marker in legacy for marker in legacy_markers)


def find_tweet_node_by_id(data: dict, tweet_id: str) -> Optional[dict]:
    if not tweet_id:
        return None
    for node in _iter_candidate_tweet_nodes(data):
        metadata = _extract_metadata_from_node(node)
        if metadata.get("tweet_id") == tweet_id and _is_extractable_tweet_node(node):
            return node
    return None


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


def _get_referenced_tweet_ids(node: dict, ref_type: str) -> List[str]:
    ids: List[str] = []
    seen = set()

    def add_id(value) -> None:
        ref_id = _coerce_string(value).strip()
        if ref_id and ref_id not in seen:
            ids.append(ref_id)
            seen.add(ref_id)

    for item in node.get("referenced_tweets", []):
        if not isinstance(item, dict) or item.get("type") != ref_type:
            continue
        add_id(item.get("id"))

    legacy = node.get("legacy") if isinstance(node.get("legacy"), dict) else {}
    if ref_type == "replied_to":
        for value in (
            node.get("in_reply_to_status_id_str"),
            node.get("in_reply_to_status_id"),
            legacy.get("in_reply_to_status_id_str"),
            legacy.get("in_reply_to_status_id"),
        ):
            add_id(value)
    elif ref_type == "quoted":
        for value in (
            node.get("quoted_status_id_str"),
            node.get("quoted_status_id"),
            legacy.get("quoted_status_id_str"),
            legacy.get("quoted_status_id"),
        ):
            add_id(value)
    return ids


def _find_reference_error(data: dict, tweet_id: str) -> str:
    for item in data.get("errors", []):
        if not isinstance(item, dict):
            continue
        resource_id = _coerce_string(item.get("resource_id") or item.get("value")).strip()
        if resource_id != tweet_id:
            continue
        detail = _coerce_string(item.get("detail")).strip()
        title = _coerce_string(item.get("title")).strip()
        if detail:
            return detail
        if title:
            return title
    return "Referenced tweet was not included in the archived payload."


def _find_user_info_by_id(data: dict, user_id: str) -> Tuple[str, str]:
    includes = data.get("includes", {})
    users = includes.get("users", []) if isinstance(includes, dict) else []
    for user in users:
        if not isinstance(user, dict):
            continue
        if _coerce_string(user.get("id")).strip() == user_id:
            return (
                _coerce_string(user.get("username") or user.get("screen_name")),
                _coerce_string(user.get("name")),
            )
    return "", ""


def _find_referenced_author_from_url_entities(node: dict, tweet_id: str) -> str:
    containers = [node]
    legacy = node.get("legacy")
    if isinstance(legacy, dict):
        containers.append(legacy)

    for container in containers:
        for entity_block_key in ("entities", "extended_entities"):
            entity_block = container.get(entity_block_key, {})
            if not isinstance(entity_block, dict):
                continue
            for url_entity in entity_block.get("urls", []):
                if not isinstance(url_entity, dict):
                    continue
                expanded = _coerce_string(url_entity.get("expanded_url") or url_entity.get("url"))
                if tweet_id not in expanded:
                    continue
                match = re.search(r"(?:twitter|x)\.com/([^/?#]+)/status/" + re.escape(tweet_id), expanded)
                if match:
                    return match.group(1)
    return ""


def _resolve_referenced_tweet_author_info(data: dict, primary_node: dict, ref_type: str, tweet_id: str) -> Tuple[str, str]:
    author = ""
    display_name = ""
    if ref_type == "replied_to":
        metadata = _extract_metadata_from_node(primary_node)
        reply_user_id = metadata.get("in_reply_to_user_id", "")
        if reply_user_id:
            author, display_name = _find_user_info_by_id(data, reply_user_id)
        if not author:
            author = metadata.get("in_reply_to_screen_name", "")
    if not author:
        author = _find_referenced_author_from_url_entities(primary_node, tweet_id)
    return author, display_name


def _build_missing_referenced_tweet_info(
    data: dict,
    primary_node: dict,
    ref_type: str,
    tweet_id: str,
    unavailable_reason: str = "",
) -> ReferencedTweetInfo:
    author, display_name = _resolve_referenced_tweet_author_info(data, primary_node, ref_type, tweet_id)
    url = f"https://twitter.com/{author}/status/{tweet_id}" if author else f"https://twitter.com/i/status/{tweet_id}"
    return ReferencedTweetInfo(
        kind=ref_type,
        text="",
        author=author,
        display_name=display_name,
        created_at="",
        url=url,
        unavailable_reason=unavailable_reason or _find_reference_error(data, tweet_id),
    )


def _build_referenced_tweet_info_from_payload(payload: dict, tweet_id: str, kind: str) -> Optional[ReferencedTweetInfo]:
    original_url = f"https://twitter.com/i/status/{tweet_id}"
    primary_node = find_tweet_node_by_id(payload, tweet_id)
    if primary_node is None:
        return None

    text, media_items, _, _ = extract_tweet_content(payload, original_url)
    author, display_name = _extract_user_info(payload, primary_node)
    metadata = _extract_metadata_from_node(primary_node)
    rendered_tweet_id = metadata.get("tweet_id") or tweet_id
    url = f"https://twitter.com/{author}/status/{rendered_tweet_id}" if author else f"https://twitter.com/i/status/{rendered_tweet_id}"
    return ReferencedTweetInfo(
        kind=kind,
        text=text,
        author=author,
        display_name=display_name,
        created_at=_coerce_string(primary_node.get("created_at")),
        url=url,
        media=tuple(media_items),
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _get_x_api_json_cache(data: dict, *, create: bool = False) -> dict:
    if not isinstance(data, dict):
        return {}
    cache = data.get(X_API_JSON_CACHE_KEY)
    if not isinstance(cache, dict):
        if not create:
            return {}
        cache = {}
        data[X_API_JSON_CACHE_KEY] = cache
    return cache


def _get_cached_x_api_reply_chain_entry(data: dict, tweet_id: str) -> dict:
    cache = _get_x_api_json_cache(data)
    chains = cache.get(X_API_REPLY_CHAINS_KEY, {}) if isinstance(cache, dict) else {}
    if not isinstance(chains, dict):
        return {}
    entry = chains.get(tweet_id, {})
    return entry if isinstance(entry, dict) else {}


def _load_cached_x_api_reply_chain(
    data: dict,
    tweet_id: str,
    max_depth: int,
) -> Tuple[List[ReferencedTweetInfo], int, str, str]:
    entry = _get_cached_x_api_reply_chain_entry(data, tweet_id)
    if not entry:
        return [], 0, "", ""

    posts = entry.get("posts", [])
    if not isinstance(posts, list):
        return [], 0, "", ""

    ref_tweets: List[ReferencedTweetInfo] = []
    for item in posts[:max_depth]:
        if not isinstance(item, dict):
            continue
        payload = item.get("payload")
        if not isinstance(payload, dict):
            continue
        cached_id = _coerce_string(item.get("id")).strip() or tweet_id
        tweet_info = _build_referenced_tweet_info_from_payload(payload, cached_id, "replied_to")
        if tweet_info is not None:
            ref_tweets.append(tweet_info)

    stored_depth = len(posts)
    try:
        stored_depth = max(stored_depth, int(entry.get("max_depth", 0)))
    except (TypeError, ValueError):
        pass

    terminal_error_id = ""
    terminal_error_message = ""
    terminal_error = entry.get("terminal_error", {})
    if isinstance(terminal_error, dict):
        terminal_error_id = _coerce_string(terminal_error.get("id")).strip()
        terminal_error_message = _coerce_string(terminal_error.get("message")).strip()
    return ref_tweets, stored_depth, terminal_error_id, terminal_error_message


def _get_cached_x_api_reply_chain_depth(data: dict, tweet_id: str) -> int:
    entry = _get_cached_x_api_reply_chain_entry(data, tweet_id)
    if not entry:
        return 0

    posts = entry.get("posts", [])
    post_count = len(posts) if isinstance(posts, list) else 0
    terminal_error = entry.get("terminal_error", {})
    has_terminal_error = isinstance(terminal_error, dict) and bool(_coerce_string(terminal_error.get("message")).strip())
    if not post_count and not has_terminal_error:
        return 0

    try:
        return max(post_count, int(entry.get("max_depth", 0)))
    except (TypeError, ValueError):
        return post_count or 1


def _store_x_api_reply_chain(
    data: dict,
    tweet_id: str,
    max_depth: int,
    payloads: list,
    terminal_error_id: str = "",
    terminal_error_message: str = "",
) -> bool:
    if not isinstance(data, dict) or not tweet_id:
        return False

    cache = _get_x_api_json_cache(data, create=True)
    chains = cache.get(X_API_REPLY_CHAINS_KEY)
    if not isinstance(chains, dict):
        chains = {}
        cache[X_API_REPLY_CHAINS_KEY] = chains

    entry = {
        "version": X_API_REPLY_CHAIN_CACHE_VERSION,
        "fetched_at": _utc_now_iso(),
        "max_depth": max_depth,
        "posts": [],
    }
    for item in payloads:
        if not isinstance(item, tuple) or len(item) < 2:
            continue
        cached_id = item[0]
        payload = item[1]
        source = item[2] if len(item) >= 3 else "x_api"
        if not isinstance(payload, dict):
            continue
        entry["posts"].append(
            {
                "id": cached_id,
                "source": source,
                "payload": payload,
            }
        )
    if terminal_error_message:
        entry["terminal_error"] = {
            "id": terminal_error_id or tweet_id,
            "message": terminal_error_message,
        }

    chains[tweet_id] = entry
    return True


def _parse_archive_timestamp(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S")
    except (TypeError, ValueError):
        return None


def _timestamp_distance(candidate_timestamp: str, target_timestamp: str) -> int:
    candidate = _parse_archive_timestamp(candidate_timestamp)
    target = _parse_archive_timestamp(target_timestamp)
    if candidate is None or target is None:
        return 0
    return int(abs((candidate - target).total_seconds()))


def _payload_contains_tweet(payload: dict, tweet_id: str) -> bool:
    if not isinstance(payload, dict) or not tweet_id:
        return False
    return find_tweet_node_by_id(payload, tweet_id) is not None


def build_local_tweet_snapshot_index(json_dir: Optional[Path]) -> Dict[str, List[Tuple[str, Path]]]:
    if json_dir is None or not json_dir.exists():
        return {}
    cache_key = str(json_dir.resolve())
    with LOCAL_TWEET_INDEX_LOCK:
        cached = LOCAL_TWEET_INDEX_CACHE.get(cache_key)
        if cached is not None:
            return cached

        index: Dict[str, List[Tuple[str, Path]]] = {}
        for json_path in json_dir.glob("*.json"):
            parsed = parse_snapshot_filename(json_path.name)
            timestamp = parsed[0] if parsed else ""
            payload = load_snapshot_json(json_path)
            if not isinstance(payload, dict):
                continue

            tweet_ids = set()
            for node in _iter_candidate_tweet_nodes(payload):
                if not _is_extractable_tweet_node(node):
                    continue
                tweet_id = _extract_metadata_from_node(node).get("tweet_id", "")
                if tweet_id:
                    tweet_ids.add(tweet_id)

            for tweet_id in tweet_ids:
                index.setdefault(tweet_id, []).append((timestamp, json_path))

        for entries in index.values():
            entries.sort(key=lambda item: (item[0], item[1].name))
        LOCAL_TWEET_INDEX_CACHE[cache_key] = index
        return index


def find_local_snapshot_payload_for_tweet(
    tweet_id: str,
    json_dir: Optional[Path],
    snapshot_timestamp: str = "",
) -> Optional[dict]:
    if not tweet_id or json_dir is None or not json_dir.exists():
        return None

    index = build_local_tweet_snapshot_index(json_dir)
    candidates = list(index.get(tweet_id, []))
    candidates.sort(key=lambda item: (_timestamp_distance(item[0], snapshot_timestamp), item[0], item[1].name))
    for _, json_path in candidates:
        payload = load_snapshot_json(json_path)
        if _payload_contains_tweet(payload, tweet_id):
            return payload
    return None


def _normalize_tweet_author_hint(author_hint: str) -> str:
    return author_hint.strip().lstrip("@")


def build_wayback_tweet_lookup_queries(tweet_id: str, author_hint: str = "") -> List[str]:
    queries: List[str] = []
    seen = set()
    author = _normalize_tweet_author_hint(author_hint)

    def add(value: str) -> None:
        if not value or value in seen:
            return
        seen.add(value)
        queries.append(value)

    if author:
        for host in ("twitter.com", "x.com", "mobile.twitter.com"):
            add(f"{host}/{author}/status/{tweet_id}")
    for host in ("twitter.com", "x.com", "mobile.twitter.com"):
        add(f"{host}/i/status/{tweet_id}")
    for host in ("twitter.com", "x.com", "mobile.twitter.com"):
        add(f"{host}/*/status/{tweet_id}")
    return queries


def query_wayback_tweet_captures(tweet_id: str, author_hint: str = "", snapshot_timestamp: str = "") -> List[Tuple[str, str]]:
    cache_key = (tweet_id, _normalize_tweet_author_hint(author_hint).lower(), snapshot_timestamp)
    with TWEET_CDX_LOOKUP_LOCK:
        cached = TWEET_CDX_LOOKUP_CACHE.get(cache_key)
        if cached is not None:
            return cached

    rows: List[Tuple[str, str]] = []
    seen = set()
    for query_url in build_wayback_tweet_lookup_queries(tweet_id, author_hint):
        params = {
            "url": query_url,
            "output": "json",
            "fl": "timestamp,original,statuscode,mimetype",
            "collapse": "digest",
            "limit": str(TWEET_CDX_FALLBACK_LIMIT),
        }
        if snapshot_timestamp:
            params["closest"] = snapshot_timestamp
        try:
            resp = get_with_retry(CDX_BASE_URL, params=params, timeout=JSON_TIMEOUT)
            payload = resp.json()
        except Exception:
            continue

        for row in payload[1:]:
            if len(row) < 2 or not row[0] or not row[1]:
                continue
            statuscode = _coerce_string(row[2]).strip() if len(row) >= 3 else ""
            if statuscode and statuscode not in {"200", "206", "302", "-"}:
                continue
            candidate = (row[0], row[1])
            if candidate in seen:
                continue
            seen.add(candidate)
            rows.append(candidate)

    rows.sort(key=lambda item: (_timestamp_distance(item[0], snapshot_timestamp), item[0], item[1]))
    with TWEET_CDX_LOOKUP_LOCK:
        TWEET_CDX_LOOKUP_CACHE[cache_key] = rows
    return rows


def fetch_wayback_tweet_payload(
    tweet_id: str,
    author_hint: str = "",
    snapshot_timestamp: str = "",
) -> Tuple[Optional[dict], str, str]:
    cache_key = (tweet_id, _normalize_tweet_author_hint(author_hint).lower(), snapshot_timestamp)
    with WAYBACK_TWEET_PAYLOAD_LOCK:
        cached = WAYBACK_TWEET_PAYLOAD_CACHE.get(cache_key)
        if cached is not None:
            return cached

    for capture_timestamp, original_url in query_wayback_tweet_captures(tweet_id, author_hint, snapshot_timestamp):
        raw_json = fetch_snapshot_json(capture_timestamp, original_url)
        if not raw_json:
            continue
        try:
            payload = json.loads(raw_json)
        except Exception:
            continue
        if not _payload_contains_tweet(payload, tweet_id):
            continue
        result = (payload, "", f"wayback:{capture_timestamp}:{original_url}")
        with WAYBACK_TWEET_PAYLOAD_LOCK:
            WAYBACK_TWEET_PAYLOAD_CACHE[cache_key] = result
        return result

    result = (None, f"Wayback fallback did not find archived JSON for tweet id {tweet_id}.", "wayback")
    with WAYBACK_TWEET_PAYLOAD_LOCK:
        WAYBACK_TWEET_PAYLOAD_CACHE[cache_key] = result
    return result


def _with_reply_chain_author_hint(context: Optional[ReplyChainLookupContext], author_hint: str) -> ReplyChainLookupContext:
    base = context or ReplyChainLookupContext()
    return ReplyChainLookupContext(
        json_dir=base.json_dir,
        snapshot_timestamp=base.snapshot_timestamp,
        author_hint=author_hint or base.author_hint,
        use_local_json=base.use_local_json,
        use_wayback=base.use_wayback,
    )


def fetch_reply_chain_payload(
    tweet_id: str,
    auth_value,
    lookup_context: Optional[ReplyChainLookupContext],
    author_hint: str = "",
) -> Tuple[Optional[dict], str, str]:
    context = lookup_context or ReplyChainLookupContext()
    effective_author_hint = author_hint or context.author_hint

    if context.use_local_json:
        payload = find_local_snapshot_payload_for_tweet(tweet_id, context.json_dir, context.snapshot_timestamp)
        if payload is not None:
            return payload, "", "local_json"

    x_api_error = ""
    auth = normalize_x_api_auth(auth_value)
    if auth.has_network_auth():
        payload, x_api_error, source = fetch_x_api_tweet_payload_with_auth(auth, tweet_id)
        if payload is not None:
            return payload, "", source

    wayback_error = ""
    if context.use_wayback:
        payload, wayback_error, source = fetch_wayback_tweet_payload(
            tweet_id,
            effective_author_hint,
            context.snapshot_timestamp,
        )
        if payload is not None:
            return payload, "", source

    if x_api_error and wayback_error:
        return None, f"{x_api_error} Wayback fallback: {wayback_error}", ""
    return None, x_api_error or wayback_error or "Reply-chain lookup did not find this post.", ""


def fetch_x_api_reply_chain(
    tweet_id: str,
    bearer_token: str,
    max_depth: int,
    payloads_out: Optional[list] = None,
    terminal_errors_out: Optional[List[Tuple[str, str]]] = None,
    lookup_context: Optional[ReplyChainLookupContext] = None,
) -> Tuple[List[ReferencedTweetInfo], str]:
    ref_tweets: List[ReferencedTweetInfo] = []
    current_id = tweet_id
    current_author_hint = lookup_context.author_hint if lookup_context is not None else ""
    seen = set()

    for depth_index in range(max_depth):
        if current_id in seen:
            break
        seen.add(current_id)

        payload, error, source = fetch_reply_chain_payload(current_id, bearer_token, lookup_context, current_author_hint)
        if payload is None:
            if terminal_errors_out is not None and error:
                terminal_errors_out.append((current_id, error))
            if ref_tweets:
                ref_tweets.append(
                    ReferencedTweetInfo(
                        kind="replied_to",
                        text="",
                        author="",
                        display_name="",
                        created_at="",
                        url=f"https://twitter.com/i/status/{current_id}",
                        unavailable_reason=error,
                    )
                )
                return ref_tweets, ""
            return [], error

        if payloads_out is not None:
            payloads_out.append((current_id, payload, source))

        tweet_info = _build_referenced_tweet_info_from_payload(payload, current_id, "replied_to")
        if tweet_info is None:
            error = "X API response did not include an extractable post."
            if terminal_errors_out is not None:
                terminal_errors_out.append((current_id, error))
            if ref_tweets:
                ref_tweets.append(
                    ReferencedTweetInfo(
                        kind="replied_to",
                        text="",
                        author="",
                        display_name="",
                        created_at="",
                        url=f"https://twitter.com/i/status/{current_id}",
                        unavailable_reason=error,
                    )
                )
                return ref_tweets, ""
            return [], error
        ref_tweets.append(tweet_info)

        primary_node = find_primary_tweet_node(payload, f"https://twitter.com/i/status/{current_id}")
        if primary_node is None:
            break
        parent_ids = _get_referenced_tweet_ids(primary_node, "replied_to")
        if not parent_ids:
            break
        current_author_hint, _ = _resolve_referenced_tweet_author_info(payload, primary_node, "replied_to", parent_ids[0])
        current_id = parent_ids[0]

    return ref_tweets, ""


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


def extract_tweet_content(
    data: dict,
    original_url: str,
    x_bearer_token: str = "",
    x_api_reply_chain_depth: int = 0,
    x_api_enrichment_changed: Optional[List[bool]] = None,
    x_api_use_json_cache: bool = True,
    x_api_retry_cached_errors: bool = False,
    reply_chain_lookup_context: Optional[ReplyChainLookupContext] = None,
) -> Tuple[str, List[TweetMediaItem], Dict[str, str], List[ReferencedTweetInfo]]:
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
            referenced_ids = _get_referenced_tweet_ids(primary_node, ref_type)
            rendered_ref_ids = set()

            def append_x_api_reply_chain(ref_id: str, context_node: dict, max_depth: int) -> bool:
                if not ref_id or max_depth <= 0:
                    return False

                author_hint, _ = _resolve_referenced_tweet_author_info(data, context_node, ref_type, ref_id)
                lookup_context = _with_reply_chain_author_hint(reply_chain_lookup_context, author_hint)
                has_reply_chain_lookup = (
                    bool(x_bearer_token)
                    or (lookup_context.use_local_json and lookup_context.json_dir is not None)
                    or lookup_context.use_wayback
                )

                def append_ref_infos(candidates: List[ReferencedTweetInfo]) -> bool:
                    appended = False
                    for x_ref in candidates:
                        match = TWEET_URL_ID_RE.search(x_ref.url)
                        x_ref_id = match.group(1) if match else ""
                        if x_ref_id and x_ref_id in rendered_ref_ids:
                            continue
                        if x_ref_id:
                            rendered_ref_ids.add(x_ref_id)
                        ref_tweets.append(x_ref)
                        appended = True
                    return appended

                if x_api_use_json_cache:
                    cached_refs, cached_depth, cached_error_id, cached_error = _load_cached_x_api_reply_chain(
                        data,
                        ref_id,
                        max_depth,
                    )
                    if cached_refs or cached_error:
                        retry_cached_error = x_api_retry_cached_errors and bool(cached_error)
                        use_cached = not retry_cached_error and (cached_depth >= max_depth or not has_reply_chain_lookup)
                        if use_cached:
                            appended = append_ref_infos(cached_refs)
                            if cached_error and len(cached_refs) < max_depth:
                                ref_tweets.append(
                                    ReferencedTweetInfo(
                                        kind="replied_to",
                                        text="",
                                        author="",
                                        display_name="",
                                        created_at="",
                                        url=f"https://twitter.com/i/status/{cached_error_id or ref_id}",
                                        unavailable_reason=cached_error,
                                    )
                                )
                                appended = True
                            return appended

                if not has_reply_chain_lookup:
                    return False

                payloads: list = []
                terminal_errors: List[Tuple[str, str]] = []
                x_api_refs, x_api_error = fetch_x_api_reply_chain(
                    ref_id,
                    x_bearer_token,
                    max_depth,
                    payloads_out=payloads,
                    terminal_errors_out=terminal_errors,
                    lookup_context=lookup_context,
                )
                terminal_error_id = terminal_errors[0][0] if terminal_errors else ""
                terminal_error = terminal_errors[0][1] if terminal_errors else x_api_error
                if payloads or terminal_error:
                    if _store_x_api_reply_chain(data, ref_id, max_depth, payloads, terminal_error_id, terminal_error):
                        if x_api_enrichment_changed is not None:
                            x_api_enrichment_changed.append(True)
                if x_api_refs:
                    return append_ref_infos(x_api_refs)
                if x_api_error:
                    ref_tweets.append(_build_missing_referenced_tweet_info(data, context_node, ref_type, ref_id, x_api_error))
                    return True
                return False

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
                    if rt_id:
                        rendered_ref_ids.add(rt_id)
                    ref_tweets.append(ReferencedTweetInfo(
                        kind=ref_type, text=rt_text, author=rt_author,
                        display_name=rt_display_name, created_at=rt_created,
                        url=rt_url, media=tuple(rt_media),
                    ))
                    if ref_type == "replied_to" and x_api_reply_chain_depth > 1:
                        for parent_ref_id in _get_referenced_tweet_ids(ref_node, "replied_to"):
                            append_x_api_reply_chain(parent_ref_id, ref_node, x_api_reply_chain_depth - 1)
                    elif ref_type == "replied_to" and x_api_use_json_cache:
                        for parent_ref_id in _get_referenced_tweet_ids(ref_node, "replied_to"):
                            cached_depth = _get_cached_x_api_reply_chain_depth(data, parent_ref_id)
                            if cached_depth:
                                append_x_api_reply_chain(parent_ref_id, ref_node, cached_depth)
            for ref_id in referenced_ids:
                if ref_id not in rendered_ref_ids:
                    effective_depth = x_api_reply_chain_depth
                    if ref_type == "replied_to" and effective_depth <= 0 and x_api_use_json_cache:
                        effective_depth = _get_cached_x_api_reply_chain_depth(data, ref_id)
                    if ref_type == "replied_to" and append_x_api_reply_chain(ref_id, primary_node, effective_depth):
                        continue
                    ref_tweets.append(_build_missing_referenced_tweet_info(data, primary_node, ref_type, ref_id))
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
    raw_json, _ = fetch_snapshot_json_with_status(snapshot_timestamp, original_url)
    return raw_json


def fetch_snapshot_json_with_status(snapshot_timestamp: str, original_url: str) -> Tuple[str, str]:
    """
    Retrieve the archived tweet JSON payload and return (raw_json, failure_status).
    """
    snapshot_url = f"https://web.archive.org/web/{snapshot_timestamp}id_/{original_url}"
    try:
        resp = get_with_retry(snapshot_url, timeout=JSON_TIMEOUT)
        resp.json()
    except Exception as exc:
        return "", extract_http_status_code(exc) or "unknown"
    return resp.text, ""


def load_snapshot_json(json_path: Path) -> Optional[dict]:
    try:
        return json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_snapshot_json_payload(json_path: Path, data: dict) -> None:
    tmp_path = json_path.with_name(f"{json_path.name}.tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(json_path)


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


def ensure_snapshot_json(
    record: SnapshotRecord,
    resume: bool = True,
    failure_statuses: Optional[Dict[SnapshotRecord, str]] = None,
) -> str:
    if resume and record.json_path.exists() and load_snapshot_json(record.json_path) is not None:
        return "reused"

    raw_json, failure_status = fetch_snapshot_json_with_status(record.timestamp, record.original_url)
    if not raw_json:
        if failure_statuses is not None:
            failure_statuses[record] = failure_status
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
    negative_cache: Optional[Dict[str, dict]] = None,
    failure_statuses: Optional[Dict[SnapshotRecord, str]] = None,
) -> str:
    json_status = ensure_snapshot_json(record, resume=resume, failure_statuses=failure_statuses)
    if json_status == "missing":
        return json_status
    if asset_dir_name and media_dir is not None:
        prefetch_snapshot_media(record, asset_dir_name, media_dir, image_cache, negative_cache)
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


def is_hard_missing_error(exc: Exception) -> bool:
    status_code = extract_http_status_code(exc)
    return status_code in {"404", "410"}


def clear_negative_media_cache_entry(media_url: str, media_dir: Path, negative_cache: Optional[Dict[str, dict]]) -> None:
    if negative_cache is None:
        return
    removed = False
    with IMAGE_CACHE_LOCK:
        if media_url in negative_cache:
            negative_cache.pop(media_url, None)
            removed = True
    if removed:
        persist_negative_media_cache_entry(media_dir, negative_cache)


def is_wayback_candidate_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc == "web.archive.org" and parsed.path.startswith("/web/")


def mark_negative_media_cache_entry(
    media_url: str,
    media_dir: Path,
    negative_cache: Optional[Dict[str, dict]],
    *,
    status: str,
    reason: str,
) -> None:
    if negative_cache is None:
        return
    with IMAGE_CACHE_LOCK:
        negative_cache[media_url] = {"status": status, "reason": reason}
    persist_negative_media_cache_entry(media_dir, negative_cache)


def download_image_asset(
    image_url: str,
    snapshot_timestamp: str,
    media_dir: Path,
    asset_dir_name: str,
    cache: Optional[Dict[str, str]] = None,
    negative_cache: Optional[Dict[str, dict]] = None,
    download_missing: bool = True,
    failure_statuses: Optional[Dict[str, str]] = None,
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
        if negative_cache is not None and image_url in negative_cache:
            if failure_statuses is not None:
                failure_statuses[image_url] = get_negative_media_status(negative_cache, image_url) or "unknown"
            return ""
    if not download_missing:
        return ""

    candidate_urls = build_image_candidate_urls(image_url, snapshot_timestamp)
    resp = None
    saw_archived_transient_error = False
    tried_archived_candidate = False
    latest_http_status = ""
    for candidate_url in candidate_urls:
        try:
            resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
            break
        except Exception as exc:
            latest_http_status = extract_http_status_code(exc) or latest_http_status
            if is_wayback_candidate_url(candidate_url):
                tried_archived_candidate = True
                if not is_hard_missing_error(exc):
                    saw_archived_transient_error = True
            resp = None
    closest_capture_candidates = []
    if resp is None:
        closest_capture_candidates = build_closest_capture_candidate_urls(image_url, snapshot_timestamp)
        for candidate_url in closest_capture_candidates:
            try:
                resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
                break
            except Exception as exc:
                latest_http_status = extract_http_status_code(exc) or latest_http_status
                if is_wayback_candidate_url(candidate_url):
                    tried_archived_candidate = True
                    if not is_hard_missing_error(exc):
                        saw_archived_transient_error = True
                resp = None
    if resp is None:
        if not latest_http_status and tried_archived_candidate and (not closest_capture_candidates or media_cdx_has_any_capture(image_url) is False):
            latest_http_status = "404"
            mark_negative_media_cache_entry(image_url, media_dir, negative_cache, status=latest_http_status, reason="no-capture")
        if failure_statuses is not None:
            failure_statuses[image_url] = latest_http_status or "unknown"
        if tried_archived_candidate and not saw_archived_transient_error:
            mark_negative_media_cache_entry(image_url, media_dir, negative_cache, status=latest_http_status or "404", reason="hard-missing")
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
        clear_negative_media_cache_entry(image_url, media_dir, negative_cache)
    return relative_path


def download_video_asset(
    video_url: str,
    snapshot_timestamp: str,
    media_dir: Path,
    asset_dir_name: str,
    cache: Optional[Dict[str, str]] = None,
    negative_cache: Optional[Dict[str, dict]] = None,
    download_missing: bool = True,
    failure_statuses: Optional[Dict[str, str]] = None,
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
        if negative_cache is not None and video_url in negative_cache:
            if failure_statuses is not None:
                failure_statuses[video_url] = get_negative_media_status(negative_cache, video_url) or "unknown"
            return ""
    if not download_missing:
        return ""

    candidate_urls = build_video_candidate_urls(video_url, snapshot_timestamp)
    resp = None
    saw_archived_transient_error = False
    tried_archived_candidate = False
    latest_http_status = ""
    for candidate_url in candidate_urls:
        try:
            resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
            break
        except Exception as exc:
            latest_http_status = extract_http_status_code(exc) or latest_http_status
            if is_wayback_candidate_url(candidate_url):
                tried_archived_candidate = True
                if not is_hard_missing_error(exc):
                    saw_archived_transient_error = True
            resp = None
    closest_capture_candidates = []
    if resp is None:
        closest_capture_candidates = build_closest_capture_candidate_urls(video_url, snapshot_timestamp)
        for candidate_url in closest_capture_candidates:
            try:
                resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
                break
            except Exception as exc:
                latest_http_status = extract_http_status_code(exc) or latest_http_status
                if is_wayback_candidate_url(candidate_url):
                    tried_archived_candidate = True
                    if not is_hard_missing_error(exc):
                        saw_archived_transient_error = True
                resp = None
    if resp is None:
        if not latest_http_status and tried_archived_candidate and (not closest_capture_candidates or media_cdx_has_any_capture(video_url) is False):
            latest_http_status = "404"
            mark_negative_media_cache_entry(video_url, media_dir, negative_cache, status=latest_http_status, reason="no-capture")
        if failure_statuses is not None:
            failure_statuses[video_url] = latest_http_status or "unknown"
        if tried_archived_candidate and not saw_archived_transient_error:
            mark_negative_media_cache_entry(video_url, media_dir, negative_cache, status=latest_http_status or "404", reason="hard-missing")
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
        clear_negative_media_cache_entry(video_url, media_dir, negative_cache)
    return relative_path


def prefetch_snapshot_media(
    record: SnapshotRecord,
    asset_dir_name: str,
    media_dir: Path,
    image_cache: Optional[Dict[str, str]] = None,
    negative_cache: Optional[Dict[str, dict]] = None,
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
            download_image_asset(media_item.url, record.timestamp, media_dir, asset_dir_name, image_cache, negative_cache)
        elif media_item.kind == "video":
            download_video_asset(media_item.url, record.timestamp, media_dir, asset_dir_name, image_cache, negative_cache)
            if media_item.poster_url:
                download_image_asset(media_item.poster_url, record.timestamp, media_dir, asset_dir_name, image_cache, negative_cache)


def snapshot_has_reply_chain_seed(data: dict, original_url: str, depth: int) -> bool:
    primary_node = find_primary_tweet_node(data, original_url)
    if primary_node is None:
        return False
    if _get_referenced_tweet_ids(primary_node, "replied_to"):
        return True
    if depth <= 1:
        return False
    for ref_node in find_replied_to_tweet_nodes(data, primary_node):
        if _get_referenced_tweet_ids(ref_node, "replied_to"):
            return True
    return False


def repair_reply_chain_for_snapshot_record(
    record: SnapshotRecord,
    bearer_token: str,
    depth: int,
    *,
    resume: bool = True,
) -> Tuple[bool, bool]:
    data = load_snapshot_json(record.json_path)
    if data is None:
        return False, False

    is_candidate = snapshot_has_reply_chain_seed(data, record.original_url, depth)
    changed: List[bool] = []
    extract_tweet_content(
        data,
        record.original_url,
        x_bearer_token=bearer_token,
        x_api_reply_chain_depth=depth,
        x_api_enrichment_changed=changed,
        x_api_use_json_cache=resume,
        x_api_retry_cached_errors=True,
        reply_chain_lookup_context=ReplyChainLookupContext(
            json_dir=record.json_path.parent,
            snapshot_timestamp=record.timestamp,
        ),
    )
    if changed:
        write_snapshot_json_payload(record.json_path, data)
        return is_candidate, True
    return is_candidate, False


def repair_reply_chains_from_snapshot_records(
    records: List[SnapshotRecord],
    bearer_token: str,
    depth: int,
    workers: int,
    *,
    resume: bool = True,
    reverse_resume: bool = False,
) -> Tuple[int, int, int, int]:
    repair_records = [
        record
        for record in order_snapshot_records_for_resume(records, resume=resume, reverse_resume=reverse_resume)
        if record.json_path.exists()
    ]
    total = len(repair_records)
    if total == 0:
        return 0, 0, 0, 0

    checked_count = 0
    candidate_count = 0
    updated_count = 0
    failed_count = 0
    next_to_submit = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {}
        while next_to_submit < total and len(pending) < workers * 4:
            record = repair_records[next_to_submit]
            future = executor.submit(repair_reply_chain_for_snapshot_record, record, bearer_token, depth, resume=resume)
            pending[future] = record
            next_to_submit += 1

        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                pending.pop(future)
                checked_count += 1
                try:
                    is_candidate, changed = future.result()
                except Exception:
                    failed_count += 1
                    is_candidate = False
                    changed = False

                if is_candidate:
                    candidate_count += 1
                if changed:
                    updated_count += 1

            while next_to_submit < total and len(pending) < workers * 4:
                record = repair_records[next_to_submit]
                future = executor.submit(repair_reply_chain_for_snapshot_record, record, bearer_token, depth, resume=resume)
                pending[future] = record
                next_to_submit += 1

            if checked_count % 25 == 0 or checked_count == total:
                print(
                    f"Reply-chain repair progress: {checked_count}/{total} checked, "
                    f"{candidate_count} reply-chain candidates, {updated_count} JSON updated, {failed_count} failed",
                    flush=True,
                )

    return total, candidate_count, updated_count, failed_count


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


def media_job_is_negatively_cached(negative_cache: Optional[Dict[str, dict]], url: str) -> bool:
    if negative_cache is None:
        return False
    with IMAGE_CACHE_LOCK:
        return url in negative_cache


def get_negative_media_status(negative_cache: Optional[Dict[str, dict]], url: str) -> str:
    if negative_cache is None:
        return ""
    with IMAGE_CACHE_LOCK:
        entry = negative_cache.get(url)
    if not isinstance(entry, dict):
        return ""
    return _coerce_string(entry.get("status")).strip()


def format_http_failure_detail(status_code: str) -> str:
    status_code = _coerce_string(status_code).strip()
    return f"http={status_code or 'unknown'}"


def repair_missing_media_from_snapshot_records(
    records: List[SnapshotRecord],
    asset_dir: Path,
    workers: int,
    resume: bool = True,
    reverse_resume: bool = False,
    verbose: bool = False,
) -> Tuple[int, int, int, int]:
    ensure_asset_directories(asset_dir)
    asset_dir_name = asset_dir.name
    media_dir = asset_dir / "media"
    image_cache = load_media_cache(asset_dir, asset_dir_name) if resume else {}
    negative_cache = load_negative_media_cache(asset_dir) if resume else {}
    media_failure_statuses: Dict[str, str] = {}

    unique_jobs: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
    record_jobs: Dict[SnapshotRecord, List[Tuple[str, str, str]]] = {}
    for record in order_snapshot_records_for_resume(records, resume=resume, reverse_resume=reverse_resume):
        jobs = collect_snapshot_media_jobs(record)
        if jobs:
            record_jobs[record] = jobs
        for kind, url, timestamp in jobs:
            unique_jobs.setdefault((kind, url), (kind, url, timestamp))

    total_jobs = len(unique_jobs)
    missing_jobs = [
        job for job in unique_jobs.values()
        if not media_job_is_cached(media_dir, image_cache, job[1])
    ]
    missing_before = len(missing_jobs)
    verbose_pending_by_record: Dict[SnapshotRecord, set] = {}
    verbose_records_by_job: Dict[Tuple[str, str], List[SnapshotRecord]] = {}
    verbose_reported_records = set()
    if verbose:
        for record, jobs in record_jobs.items():
            pending_keys = {
                (kind, url)
                for kind, url, _ in jobs
                if not media_job_is_cached(media_dir, image_cache, url)
            }
            if not pending_keys:
                continue
            verbose_pending_by_record[record] = pending_keys
            for key in pending_keys:
                verbose_records_by_job.setdefault(key, []).append(record)

    if not missing_jobs:
        write_media_cache(asset_dir, image_cache)
        return total_jobs, 0, 0, 0

    def record_http_failure_detail(record: SnapshotRecord) -> str:
        status_codes = []
        for _, url in verbose_pending_by_record.get(record, set()):
            status_code = get_negative_media_status(negative_cache, url) or media_failure_statuses.get(url, "")
            if status_code and status_code not in status_codes:
                status_codes.append(status_code)
        return format_http_failure_detail(",".join(status_codes))

    def run_job(job: Tuple[str, str, str]) -> bool:
        kind, url, timestamp = job
        if kind == "image":
            return bool(download_image_asset(url, timestamp, media_dir, asset_dir_name, image_cache, negative_cache, failure_statuses=media_failure_statuses))
        return bool(download_video_asset(url, timestamp, media_dir, asset_dir_name, image_cache, negative_cache, failure_statuses=media_failure_statuses))

    skipped_negative = sum(1 for _, url, _ in missing_jobs if media_job_is_negatively_cached(negative_cache, url))
    remaining_jobs = [job for job in missing_jobs if not media_job_is_negatively_cached(negative_cache, job[1])]
    if skipped_negative:
        print(f"Skipping {skipped_negative}/{missing_before} hard-missing targets from negative cache...", flush=True)
        if verbose:
            for record, pending_keys in verbose_pending_by_record.items():
                if record in verbose_reported_records:
                    continue
                if any(media_job_is_negatively_cached(negative_cache, url) for _, url in pending_keys):
                    print_verbose_status(record, False, record_http_failure_detail(record))
                    verbose_reported_records.add(record)

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
                        if verbose:
                            job_key = (job[0], job[1])
                            for record in verbose_records_by_job.get(job_key, []):
                                if record in verbose_reported_records:
                                    continue
                                pending_keys = verbose_pending_by_record.get(record)
                                if pending_keys is None:
                                    continue
                                pending_keys.discard(job_key)
                                if not pending_keys:
                                    print_verbose_status(record, True)
                                    verbose_reported_records.add(record)
                    else:
                        pass_failed += 1
                        if verbose and media_job_is_negatively_cached(negative_cache, job[1]):
                            job_key = (job[0], job[1])
                            for record in verbose_records_by_job.get(job_key, []):
                                if record in verbose_reported_records:
                                    continue
                                print_verbose_status(record, False, record_http_failure_detail(record))
                                verbose_reported_records.add(record)

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
        write_negative_media_cache(asset_dir, negative_cache)
        remaining_jobs = [
            job for job in remaining_jobs
            if not media_job_is_cached(media_dir, image_cache, job[1])
            and not media_job_is_negatively_cached(negative_cache, job[1])
        ]
        if remaining_jobs and pass_index < DEFAULT_MEDIA_REPAIR_PASSES:
            print(
                f"Retrying {len(remaining_jobs)}/{missing_before} still-missing media targets after a short backoff...",
                flush=True,
            )
            time.sleep(MEDIA_REPAIR_BACKOFF_SECONDS * pass_index)

    missing_after = sum(
        1 for _, url, _ in missing_jobs
        if not media_job_is_cached(media_dir, image_cache, url)
    )
    repaired_count = missing_before - missing_after
    write_media_cache(asset_dir, image_cache)
    write_negative_media_cache(asset_dir, negative_cache)
    if verbose:
        for record, pending_keys in verbose_pending_by_record.items():
            if record in verbose_reported_records:
                continue
            if pending_keys:
                print_verbose_status(record, False, record_http_failure_detail(record))
            else:
                print_verbose_status(record, True)
            verbose_reported_records.add(record)
    return total_jobs, missing_before, repaired_count, missing_after


def render_snapshot_entry(
    record: SnapshotRecord,
    index: int,
    asset_dir_name: str,
    media_dir: Path,
    image_cache: Optional[Dict[str, str]] = None,
    negative_cache: Optional[Dict[str, dict]] = None,
    download_missing_media: bool = True,
    x_bearer_token: str = "",
    x_api_reply_chain_depth: int = 0,
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

    x_api_enrichment_changed: List[bool] = []
    text, media_items, metadata, ref_tweets = extract_tweet_content(
        data,
        record.original_url,
        x_bearer_token=x_bearer_token,
        x_api_reply_chain_depth=x_api_reply_chain_depth,
        x_api_enrichment_changed=x_api_enrichment_changed,
        reply_chain_lookup_context=ReplyChainLookupContext(
            json_dir=record.json_path.parent,
            snapshot_timestamp=record.timestamp,
        ),
    )
    if x_api_enrichment_changed:
        try:
            write_snapshot_json_payload(record.json_path, data)
        except Exception as exc:
            print(f"Warning: failed to persist X API reply-chain cache to {record.json_path}: {exc}", flush=True)
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
            image_path = download_image_asset(
                media_item.url,
                record.timestamp,
                media_dir,
                asset_dir_name,
                image_cache,
                negative_cache,
                download_missing=download_missing_media,
            )
            if not image_path:
                continue
            media_html_parts.append(
                f"  <div class='tweet-media-item'><img src='{html.escape(image_path)}' alt='Archived media from tweet captured on {html.escape(dt)}' loading='lazy' decoding='async'/></div>\n"
            )
            continue
        if media_item.kind == "video":
            video_path = download_video_asset(
                media_item.url,
                record.timestamp,
                media_dir,
                asset_dir_name,
                image_cache,
                negative_cache,
                download_missing=download_missing_media,
            )
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
                    negative_cache,
                    download_missing=download_missing_media,
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

    quoted_html = ""
    reply_html = ""
    reply_count = 0
    for rt in ref_tweets:
        rt_safe_text = html.escape(rt.text).replace("\n", "<br/>\n")
        rt_missing_class = " tweet-ref-missing" if rt.unavailable_reason else ""
        # Author header: display name + @username
        rt_header_parts = []
        if rt.display_name:
            rt_header_parts.append(f"<span class='tweet-ref-name'>{html.escape(rt.display_name)}</span>")
        if rt.author:
            rt_header_parts.append(f"<span class='tweet-ref-handle'>@{html.escape(rt.author)}</span>")
        rt_header_html = f"    <div class='tweet-ref-header'>{' '.join(rt_header_parts)}</div>\n" if rt_header_parts else ""
        rt_text_html = f"    <p>{rt_safe_text}</p>\n" if rt_safe_text else ""
        if not rt_text_html and rt.unavailable_reason:
            rt_text_html = f"    <p>Referenced tweet unavailable: {html.escape(rt.unavailable_reason)}</p>\n"
        rt_media_parts = []
        for rm in rt.media:
            if rm.kind == "image":
                rm_path = download_image_asset(
                    rm.url,
                    record.timestamp,
                    media_dir,
                    asset_dir_name,
                    image_cache,
                    negative_cache,
                    download_missing=download_missing_media,
                )
                if rm_path:
                    rt_media_parts.append(
                        f"      <div class='tweet-media-item'><img src='{html.escape(rm_path)}' loading='lazy' decoding='async'/></div>\n"
                    )
            elif rm.kind == "video":
                rm_path = download_video_asset(
                    rm.url,
                    record.timestamp,
                    media_dir,
                    asset_dir_name,
                    image_cache,
                    negative_cache,
                    download_missing=download_missing_media,
                )
                if rm_path:
                    rm_poster = ""
                    if rm.poster_url:
                        pp = download_image_asset(
                            rm.poster_url,
                            record.timestamp,
                            media_dir,
                            asset_dir_name,
                            image_cache,
                            negative_cache,
                            download_missing=download_missing_media,
                        )
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
        if not rt_time_html and rt.url:
            rt_time_html = f"    <span class='tweet-ref-time'><a href='{html.escape(rt.url)}'>View referenced tweet</a></span>\n"
        blockquote = (
            f"  <blockquote class='tweet-ref tweet-ref-{rt.kind}{rt_missing_class}'>\n"
            f"{rt_header_html}"
            f"{rt_text_html}"
            f"{rt_media_html}"
            f"{rt_time_html}"
            "  </blockquote>\n"
        )
        if rt.kind == "quoted":
            quoted_html += blockquote
        else:
            reply_html += blockquote
            reply_count += 1

    comments_html = ""
    if reply_html:
        comments_html = (
            f"  <div class='tweet-comments' hidden>\n{reply_html}  </div>\n"
            f"  <button class='tweet-expand-btn' data-comment-count='{reply_count}'>"
            f"\u5c55\u5f00\u8bc4\u8bba ({reply_count})</button>\n"
        )

    block = (
        f"<div class='tweet' {tweet_attrs}>\n  <h3>{dt}</h3>\n{text_html}{media_html}{quoted_html}{comments_html}  <p><a href='{safe_url}'>View original tweet</a></p>\n</div>\n"
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
    panel_html = (
        "<div class='detail-panel' hidden>\n"
        "  <div class='detail-panel-backdrop'></div>\n"
        "  <div class='detail-panel-content'>\n"
        "    <div class='detail-panel-header'>\n"
        "      <div class='detail-panel-tabs'>\n"
        "        <button class='detail-tab active' data-tab='post'>\u5e16\u5b50\u8be6\u60c5</button>\n"
        "        <button class='detail-tab' data-tab='comments'>\u8bc4\u8bba\u533a</button>\n"
        "      </div>\n"
        "      <button class='detail-panel-close' aria-label='Close'>\u00d7</button>\n"
        "    </div>\n"
        "    <div class='detail-panel-body'>\n"
        "      <div class='detail-tab-pane' data-tab-content='post'></div>\n"
        "      <div class='detail-tab-pane' data-tab-content='comments' hidden></div>\n"
        "    </div>\n"
        "  </div>\n"
        "</div>\n"
    )
    return (
        f"{panel_html}"
        f"  <script src='{html.escape(asset_dir_name)}/archive.js'></script>\n"
        "</div>\n</body>\n</html>"
    )


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
    negative_cache: Optional[Dict[str, dict]] = None,
    reverse_resume: bool = False,
    verbose: bool = False,
) -> Tuple[int, int]:
    total = len(records)
    if total == 0:
        return 0, 0

    ready_records, missing_records = split_ready_and_missing_snapshot_records(records) if resume else ([], records[:])
    missing_records = order_snapshot_records_for_resume(
        missing_records,
        resume=resume,
        reverse_resume=reverse_resume,
    )
    downloaded_count = 0
    ready_count = len(ready_records)
    json_failure_statuses: Dict[SnapshotRecord, str] = {}

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
                    negative_cache,
                    json_failure_statuses,
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
                        if verbose:
                            print_verbose_status(record, True)
                    elif status == "reused":
                        if verbose:
                            print_verbose_status(record, True)
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
                        negative_cache,
                        json_failure_statuses,
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
        if verbose:
            for record in missing_records:
                print_verbose_status(record, False, format_http_failure_detail(json_failure_statuses.get(record, "")))

    return ready_count, total


def fetch_and_render_snapshot_entry(
    record: SnapshotRecord,
    index: int,
    asset_dir_name: str,
    media_dir: Path,
    image_cache: Optional[Dict[str, str]] = None,
    negative_cache: Optional[Dict[str, dict]] = None,
    resume: bool = True,
    x_bearer_token: str = "",
    x_api_reply_chain_depth: int = 0,
) -> Tuple[str, Optional[ArchiveEntry], str]:
    failure_statuses: Dict[SnapshotRecord, str] = {}
    json_status = prepare_snapshot_json_and_media(
        record,
        resume=resume,
        asset_dir_name=asset_dir_name,
        media_dir=media_dir,
        image_cache=image_cache,
        negative_cache=negative_cache,
        failure_statuses=failure_statuses,
    )
    if json_status == "missing":
        return json_status, None, failure_statuses.get(record, "")
    return (
        json_status,
        render_snapshot_entry(
            record,
            index,
            asset_dir_name,
            media_dir,
            image_cache,
            negative_cache,
            x_bearer_token=x_bearer_token,
            x_api_reply_chain_depth=x_api_reply_chain_depth,
        ),
        "",
    )


def build_archives_from_snapshot_records(
    records: List[SnapshotRecord],
    user: str,
    output_path: Path,
    asset_dir: Path,
    workers: int,
    resume: bool = True,
    download_missing_media: bool = True,
    reverse_resume: bool = False,
    x_bearer_token: str = "",
    x_api_reply_chain_depth: int = 0,
    verbose: bool = False,
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
    negative_cache = load_negative_media_cache(asset_dir) if resume else {}

    json_dir = asset_dir / "json"
    profile = extract_user_profile(user, json_dir)
    avatar_path = ""
    if profile and profile.avatar_url:
        avatar_path = download_image_asset(
            profile.avatar_url,
            "",
            media_dir,
            asset_dir_name,
            image_cache,
            negative_cache,
            download_missing=download_missing_media,
        )

    total = len(records)
    submission_items = order_indexed_snapshot_records_for_resume(
        records,
        resume=resume,
        reverse_resume=reverse_resume,
    )
    next_to_submit = 0
    next_to_finalize = 0
    rendered_count = 0
    last_reported_rendered = -1
    finalized_entries: List[ArchiveEntry] = []
    completed: Dict[int, Optional[ArchiveEntry]] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {}

        while next_to_submit < total and len(pending) < workers * 4:
            record_index, record = submission_items[next_to_submit]
            future = executor.submit(
                render_snapshot_entry,
                record,
                record_index,
                asset_dir_name,
                media_dir,
                image_cache,
                negative_cache,
                download_missing_media,
                x_bearer_token,
                x_api_reply_chain_depth,
            )
            pending[future] = record_index
            next_to_submit += 1

        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                index = pending.pop(future)
                try:
                    entry = future.result()
                except Exception:
                    entry = None
                if verbose:
                    print_verbose_status(records[index], entry is not None)
                completed[index] = entry

            while next_to_submit < total and len(pending) < workers * 4:
                record_index, record = submission_items[next_to_submit]
                future = executor.submit(
                    render_snapshot_entry,
                    record,
                    record_index,
                    asset_dir_name,
                    media_dir,
                    image_cache,
                    negative_cache,
                    download_missing_media,
                    x_bearer_token,
                    x_api_reply_chain_depth,
                )
                pending[future] = record_index
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
    write_negative_media_cache(asset_dir, negative_cache)
    print(f"Archive entries rendered: {rendered_count}/{total}")
    return write_all_archive_outputs(finalized_entries, user, output_path, asset_dir_name, profile=profile, avatar_path=avatar_path)


def build_archives(
    snapshots: List[Tuple[str, str]],
    user: str,
    output_path: Path,
    asset_dir: Path,
    workers: int,
    resume: bool = True,
    reverse_resume: bool = False,
    x_bearer_token: str = "",
    x_api_reply_chain_depth: int = 0,
    verbose: bool = False,
) -> List[Path]:
    ensure_asset_directories(asset_dir)
    records = build_snapshot_records(snapshots, asset_dir / "json")
    write_snapshot_manifest(asset_dir, user, records)
    ensure_static_files(asset_dir)
    media_dir = asset_dir / "media"
    asset_dir_name = asset_dir.name
    image_cache = load_media_cache(asset_dir, asset_dir_name) if resume else {}
    negative_cache = load_negative_media_cache(asset_dir) if resume else {}

    total = len(records)
    reused_json_count = count_existing_snapshot_json(records) if resume else 0
    ready_json_count = reused_json_count
    downloaded_json_count = 0
    submission_items = order_indexed_snapshot_records_for_resume(
        records,
        resume=resume,
        reverse_resume=reverse_resume,
    )
    next_to_submit = 0
    next_to_finalize = 0
    last_reported_ready_json = -1
    rendered_count = 0
    last_reported_rendered = -1
    finalized_entries: List[ArchiveEntry] = []
    completed: Dict[int, Tuple[str, Optional[ArchiveEntry], str]] = {}

    if reused_json_count:
        print(f"Reusing {reused_json_count}/{total} existing JSON snapshots...", flush=True)
    print("Rebuilding HTML from available JSON in chronological order...", flush=True)

    with output_path.open("w", encoding="utf-8") as base_file:
        base_file.write(render_document_start(user, total, asset_dir_name))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = {}

            while next_to_submit < total and len(pending) < workers * 4:
                record_index, record = submission_items[next_to_submit]
                future = executor.submit(
                    fetch_and_render_snapshot_entry,
                    record,
                    record_index,
                    asset_dir_name,
                    media_dir,
                    image_cache,
                    negative_cache,
                    resume,
                    x_bearer_token,
                    x_api_reply_chain_depth,
                )
                pending[future] = record_index
                next_to_submit += 1

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    index = pending.pop(future)
                    try:
                        result = future.result()
                    except Exception:
                        result = ("missing", None, "unknown")
                    if verbose:
                        print_verbose_status(records[index], result[1] is not None, format_http_failure_detail(result[2]))
                    completed[index] = result

                while next_to_submit < total and len(pending) < workers * 4:
                    record_index, record = submission_items[next_to_submit]
                    future = executor.submit(
                        fetch_and_render_snapshot_entry,
                        record,
                        record_index,
                        asset_dir_name,
                        media_dir,
                        image_cache,
                        negative_cache,
                        resume,
                        x_bearer_token,
                        x_api_reply_chain_depth,
                    )
                    pending[future] = record_index
                    next_to_submit += 1

                while next_to_finalize in completed:
                    json_status, entry, _ = completed.pop(next_to_finalize)
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
    write_negative_media_cache(asset_dir, negative_cache)

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
            negative_cache=negative_cache,
            reverse_resume=reverse_resume,
            verbose=verbose,
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
        reverse_resume=reverse_resume,
        x_bearer_token=x_bearer_token,
        x_api_reply_chain_depth=x_api_reply_chain_depth,
        verbose=verbose,
    )


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or a positive integer")
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
    mode_group.add_argument(
        "--repair-reply-chain-depth",
        "--repair-reply-chain-deepth",
        dest="repair_reply_chain_depth",
        type=positive_int,
        metavar="N",
        help="Scan local JSON snapshots, use X API lookup to fill replied-to parent chains to depth N, and write them back to JSON without regenerating HTML.",
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
    parser.add_argument(
        "--reverse-resume",
        action="store_true",
        help="When resume is enabled, process remaining snapshot/media work from newest to oldest while keeping final HTML output order unchanged.",
    )
    parser.add_argument(
        "--no-media-repair",
        action="store_true",
        help="With --html-from-json, do not download missing media during HTML rendering; only use already cached local media files.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-tweet status lines as tweet_id:success or tweet_id:failed:http=CODE.",
    )
    parser.add_argument(
        "--x-api-reply-chain-depth",
        type=non_negative_int,
        default=0,
        help="Use X API lookup to recursively fill missing replied-to posts up to this parent-chain depth.",
    )
    parser.add_argument(
        "--x-bearer-token-env",
        default="X_BEARER_TOKEN",
        help="Environment variable name containing the X API bearer token (default: X_BEARER_TOKEN; TWITTER_BEARER_TOKEN is also checked).",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Load X API credentials from this dotenv file before reading environment variables (default: .env).",
    )
    args = parser.parse_args()
    if args.no_media_repair and not args.html_from_json:
        parser.error("--no-media-repair can only be used with --html-from-json")
    if args.no_media_repair and args.no_resume:
        parser.error("--no-media-repair cannot be combined with --no-resume")
    if args.reverse_resume and args.no_resume:
        parser.error("--reverse-resume cannot be combined with --no-resume")
    if args.x_api_reply_chain_depth and (args.json_only or args.repair_media or args.repair_reply_chain_depth):
        parser.error("--x-api-reply-chain-depth only applies while rendering HTML")
    return args


def main():
    args = parse_args()
    load_env_file(Path(args.env_file))
    user = args.twitter_username.lstrip("@").strip()
    resume = not args.no_resume
    output_path, asset_dir, _, json_dir, _ = build_output_paths(user)
    x_bearer_token = XApiAuth()

    if args.reverse_resume:
        print("Resume order is set to newest-first for any remaining work.", flush=True)
    if args.x_api_reply_chain_depth or args.repair_reply_chain_depth:
        x_bearer_token = get_x_api_auth(args.x_bearer_token_env)
        if not x_bearer_token:
            flag_name = "--repair-reply-chain-depth" if args.repair_reply_chain_depth else "--x-api-reply-chain-depth"
            print(
                f"{flag_name}: no X API credentials found in {args.env_file} or environment; "
                "using local JSON and Wayback fallback only.",
                flush=True,
            )
    if args.repair_reply_chain_depth:
        print(f"X API reply-chain repair is enabled to depth {args.repair_reply_chain_depth} ({x_bearer_token.describe()}).", flush=True)
    elif args.x_api_reply_chain_depth:
        print(f"X API reply-chain lookup is enabled to depth {args.x_api_reply_chain_depth} ({x_bearer_token.describe()}).", flush=True)

    if args.html_from_json or args.repair_media or args.repair_reply_chain_depth:
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
                reverse_resume=args.reverse_resume,
                verbose=args.verbose,
            )
            print(f"Unique media targets: {total_jobs}")
            print(f"Missing before repair: {missing_before}")
            print(f"Repaired this run: {repaired_count}")
            print(f"Still missing after repair: {missing_after}")
            print("Finished media repair stage.")
            print(f"  - {media_index_path(asset_dir)}")
            print(f"  - {asset_dir / 'media'}")
            return
        if args.repair_reply_chain_depth:
            print(
                f"Found {len(available_records)} local JSON snapshots. Repairing reply chains to depth {args.repair_reply_chain_depth}...",
                flush=True,
            )
            total_records, candidate_count, updated_count, failed_count = repair_reply_chains_from_snapshot_records(
                available_records,
                x_bearer_token,
                args.repair_reply_chain_depth,
                args.workers,
                resume=resume,
                reverse_resume=args.reverse_resume,
            )
            print(f"JSON snapshots checked: {total_records}")
            print(f"Reply-chain candidates: {candidate_count}")
            print(f"JSON snapshots updated: {updated_count}")
            print(f"Failed snapshots: {failed_count}")
            print("Finished reply-chain repair stage.")
            print(f"  - {json_dir}")
            return
        else:
            print(f"Found {len(available_records)} local JSON snapshots. Rendering HTML...")
            if args.no_media_repair:
                print("Media repair during HTML rendering is disabled; using only already cached local media.", flush=True)
            written_paths = build_archives_from_snapshot_records(
                available_records,
                user,
                output_path,
                asset_dir,
                args.workers,
                resume=resume,
                download_missing_media=not args.no_media_repair,
                reverse_resume=args.reverse_resume,
                x_bearer_token=x_bearer_token,
                x_api_reply_chain_depth=args.x_api_reply_chain_depth,
                verbose=args.verbose,
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
            available_count, total = download_snapshot_records(
                records,
                args.workers,
                resume=resume,
                reverse_resume=args.reverse_resume,
                verbose=args.verbose,
            )
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
            reverse_resume=args.reverse_resume,
            x_bearer_token=x_bearer_token,
            x_api_reply_chain_depth=args.x_api_reply_chain_depth,
            verbose=args.verbose,
        )

    print("Finished. Wrote:")
    for path in written_paths:
        print(f"  - {path}")


if __name__ == "__main__":
    main()

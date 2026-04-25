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
    python download_archive.py <twitter_username> --html-from-json --no-any-repair
    X_BEARER_TOKEN=... python download_archive.py <twitter_username> --html-from-json --x-api-reply-chain-depth 8
    X_BEARER_TOKEN=... python download_archive.py <twitter_username> --html-from-json --x-api-reply-chain-depth 8 --x-api-only
    X_BEARER_TOKEN=... python download_archive.py <twitter_username> --repair-reply-chain-depth 8
    python download_archive.py <twitter_username> --reverse-resume

Example:

    python download_archive.py AnIncandescence
    python download_archive.py AnIncandescence --json-only --workers 24
    python download_archive.py AnIncandescence --html-from-json
    python download_archive.py AnIncandescence --html-from-json --no-any-repair
    X_BEARER_TOKEN=... python download_archive.py AnIncandescence --html-from-json --x-api-reply-chain-depth 8
    X_BEARER_TOKEN=... python download_archive.py AnIncandescence --html-from-json --x-api-reply-chain-depth 8 --x-api-only
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
from dataclasses import dataclass, replace
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
DEFAULT_X_API_MIN_INTERVAL_SECONDS = 1.2
X_API_RATE_LIMIT_RESET_PADDING_SECONDS = 2.0
DEFAULT_EFFORT_REPLY_CHAIN_DEPTH = 8
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
X_API_ACCESS_INDEX_LOCK = threading.RLock()
X_API_APP_BEARER_LOCK = threading.RLock()
X_API_APP_BEARER_CACHE: Dict[str, Tuple[str, str]] = {}
X_API_RATE_LIMIT_LOCK = threading.RLock()
X_API_MIN_INTERVAL_SECONDS = DEFAULT_X_API_MIN_INTERVAL_SECONDS
X_API_LAST_REQUEST_AT = 0.0
TWEET_CDX_LOOKUP_LOCK = threading.RLock()
TWEET_CDX_LOOKUP_CACHE: Dict[Tuple[str, str, str], List[Tuple[str, str]]] = {}
WAYBACK_TWEET_PAYLOAD_LOCK = threading.RLock()
WAYBACK_TWEET_PAYLOAD_CACHE: Dict[Tuple[str, str, str], Tuple[Optional[dict], str, str]] = {}
LOCAL_TWEET_INDEX_LOCK = threading.RLock()
LOCAL_TWEET_INDEX_CACHE: Dict[str, Dict[str, List[Tuple[str, Path]]]] = {}
LOCAL_CONVERSATION_INDEX_CACHE: Dict[str, Dict[str, List[dict]]] = {}
SNAPSHOT_MANIFEST_NAME = "snapshots.json"
MEDIA_INDEX_NAME = "media_index.json"
MEDIA_NEGATIVE_INDEX_NAME = "media_negative_index.json"
X_API_ACCESS_INDEX_NAME = "x_api_access_index.json"
MEDIA_NEGATIVE_CACHE_VERSION = 1
X_API_ACCESS_INDEX_VERSION = 1
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
DEFAULT_X_API_TIMELINE_PAGES = 1
DEFAULT_X_API_TIMELINE_PAGE_SIZE = 100


@dataclass(frozen=True)
class MediaFetchPolicy:
    name: str
    download_missing: bool
    include_images: bool = True
    include_videos: bool = True
    allow_wayback_fallback: bool = True
    allow_closest_cdx_lookup: bool = True

    def should_download_kind(self, kind: str) -> bool:
        if not self.download_missing:
            return False
        if kind == "image":
            return self.include_images
        if kind == "video":
            return self.include_videos
        return False


MEDIA_FETCH_NONE = MediaFetchPolicy(
    name="none",
    download_missing=False,
    include_images=False,
    include_videos=False,
    allow_wayback_fallback=False,
    allow_closest_cdx_lookup=False,
)
MEDIA_FETCH_EASY_IMAGES = MediaFetchPolicy(
    name="easy_image",
    download_missing=True,
    include_images=True,
    include_videos=False,
    allow_wayback_fallback=True,
    allow_closest_cdx_lookup=False,
)
MEDIA_FETCH_FULL = MediaFetchPolicy(
    name="full",
    download_missing=True,
    include_images=True,
    include_videos=True,
    allow_wayback_fallback=True,
    allow_closest_cdx_lookup=True,
)


@dataclass(frozen=True)
class EffortSettings:
    reply_chain_depth: int
    stream_media_policy: MediaFetchPolicy
    final_media_policy: MediaFetchPolicy
    run_final_media_repair: bool
    retry_forbidden_media_in_repair: bool

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
@media (max-width:600px){.archive-controls{gap:6px}.archive-toggle{width:100%;justify-content:space-between;padding:8px 12px}.archive-sticky-bar{position:static;background:none;backdrop-filter:none}.archive-tabs{position:sticky;top:0;z-index:10;background:rgba(255,255,255,.85);backdrop-filter:blur(12px)}}
@media (max-width:600px) and (prefers-color-scheme:dark){.archive-tabs{background:rgba(0,0,0,.85)}}
.archive-tabs{display:flex;border-bottom:1px solid var(--line)}
.archive-tab{flex:1;padding:14px 0;border:none;background:none;font-size:15px;font-weight:600;color:var(--muted);cursor:pointer;text-align:center;position:relative;transition:color .15s,background .15s}
.archive-tab:hover{background:var(--hover)}
.archive-tab.active{color:var(--ink)}
.archive-tab.active::after{content:"";position:absolute;bottom:0;left:50%;transform:translateX(-50%);width:56px;height:4px;border-radius:9999px;background:var(--accent)}
.archive-tab-meta{color:var(--muted);font-size:12px;font-weight:400;margin-left:4px}
.tweet-footer{display:flex;justify-content:space-between;align-items:baseline}
.tweet-expand-btn{border:none;background:none;color:var(--accent);font-size:inherit;font-weight:inherit;cursor:pointer;padding:0;line-height:inherit}
.tweet-expand-btn:hover{text-decoration:underline}
.tweet-comments{margin:4px 0}
.tweet-comment-node{position:relative;margin:0 0 8px}
.tweet-comment-node>.tweet-ref{margin:0 0 8px}
.tweet-comment-children{margin-left:9px;padding-left:5px;border-left:2px solid var(--line)}
.detail-panel{display:flex;flex-direction:column;background:var(--bg);overflow:hidden}
.detail-panel-backdrop{position:fixed;inset:0;z-index:99;background:rgba(0,0,0,.25)}
.detail-panel-backdrop[hidden]{display:none}
.detail-panel-header{display:flex;align-items:center;justify-content:space-between;padding:0 12px;border-bottom:1px solid var(--line);flex-shrink:0;min-height:48px}
.detail-panel-title{font-size:16px;font-weight:700;padding:12px 4px}
.detail-panel-close{width:36px;height:36px;border:none;background:none;font-size:22px;color:var(--muted);cursor:pointer;border-radius:50%;display:flex;align-items:center;justify-content:center;flex-shrink:0;transition:background .15s,color .15s}
.detail-panel-close:hover{background:var(--hover);color:var(--ink)}
.detail-panel-body{flex:1;overflow-y:auto;-webkit-overflow-scrolling:touch;padding:16px}
.detail-context{margin:0 0 12px;padding:0 0 12px 16px;border-left:2px solid var(--line)}
.detail-context .tweet-reply-chain{margin:0}
.detail-context .tweet-reply-chain>summary{display:none}
.detail-context .tweet-ref{margin:0 0 12px;padding:0;border:none;border-radius:0;background:none}
.detail-context .tweet-ref:last-child{margin-bottom:0}
.detail-context .tweet-ref p{font-size:15px;line-height:1.5;overflow-wrap:anywhere}
.detail-context .tweet-ref-name{font-size:15px}
.detail-context .tweet-ref-time{margin-top:4px}
.detail-context .tweet-media-unavailable{color:var(--muted)}
.detail-post h3{font-size:13px;font-weight:400;color:var(--muted)}
.detail-post p{margin:4px 0 0;font-size:15px;color:var(--ink);line-height:1.5;overflow-wrap:anywhere}
.detail-post .tweet-media{display:grid;grid-template-columns:1fr;gap:2px;margin:10px 0 4px;border-radius:16px;overflow:hidden}
.detail-post .tweet-media-item img{display:block;width:100%;height:auto;background:var(--line)}
.detail-post .tweet-media-item video{display:block;width:100%;height:auto;background:#000}
.detail-post .tweet-media-unavailable,.detail-comments .tweet-media-unavailable{color:var(--muted)}
.detail-post .tweet-ref{margin:10px 0 4px;padding:12px;border:1px solid var(--line);border-radius:12px;background:var(--bg)}
.detail-post .tweet-ref p{font-size:14px}
.detail-post a{color:var(--accent);text-decoration:none}
.detail-post a:hover{text-decoration:underline}
.detail-comments{margin:16px 0 0;padding:16px 0 0;border-top:1px solid var(--line)}
.detail-comments-header{font-size:15px;font-weight:700;color:var(--ink);margin:0 0 12px}
.detail-comments .tweet-ref{margin:0 0 12px;padding:12px;border:1px solid var(--line);border-radius:12px;background:var(--bg)}
.detail-comments .tweet-ref p{margin:4px 0 0;font-size:14px;color:var(--ink);line-height:1.45}
.detail-comments .tweet-ref-header{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap}
.detail-comments .tweet-ref-name{font-size:14px;font-weight:700;color:var(--ink)}
.detail-comments .tweet-ref-handle{font-size:13px;color:var(--muted)}
.detail-comments .tweet-ref-time{display:block;margin-top:6px;font-size:12px;color:var(--muted)}
.detail-comments .tweet-ref .tweet-media{display:grid;grid-template-columns:1fr;gap:2px;margin:8px 0 4px;border-radius:12px;overflow:hidden}
.detail-comments .tweet-ref .tweet-media-item{overflow:hidden;min-height:0}
.detail-comments .tweet-ref .tweet-media-item img{display:block;width:100%;height:auto;background:var(--line)}
.detail-comments .tweet-ref .tweet-media:has(> :nth-child(2)){grid-template-columns:1fr 1fr;height:200px}
.detail-comments .tweet-ref .tweet-media:has(> :nth-child(2)) img{height:100%;object-fit:cover}
.detail-comments .tweet-ref .tweet-media:has(> :nth-child(3):last-child){grid-template-rows:1fr 1fr}
.detail-comments .tweet-ref .tweet-media:has(> :nth-child(3):last-child)> :first-child{grid-row:1/-1}
.detail-comments .tweet-ref .tweet-media:has(> :nth-child(4):last-child){grid-template-rows:1fr 1fr}
.detail-empty{color:var(--muted);text-align:center;padding:32px 0;font-size:14px}
.detail-note{color:var(--muted);text-align:center;padding:8px 0;font-size:12px}
@media (min-width:601px){.detail-panel{position:sticky;top:0;height:100vh;width:600px;max-width:50vw;flex-shrink:0;border-left:1px solid var(--line)}.detail-panel-backdrop{display:none!important}}
@media (max-width:600px){.detail-panel{position:fixed;bottom:0;left:0;right:0;z-index:100;max-height:80vh;border-radius:16px 16px 0 0;box-shadow:0 -4px 24px rgba(0,0,0,.12);display:none;flex-direction:column}.detail-panel.is-open{display:flex}.detail-panel-body{overscroll-behavior:contain}body.detail-panel-open{overflow:hidden}}
"""
ARCHIVE_FILTER_JS = """
(() => {
  let deferredData = null;
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

  const stickyBar = document.createElement("div");
  stickyBar.className = "archive-sticky-bar";

  setupArchiveTabs(allTweets, stickyBar);
  setupTweetFilters(allTweets, controls);
  setupCommentExpansion(allTweets, controls);
  setupReplyChainExpansion(allTweets, controls);
  if (controls.children.length) stickyBar.appendChild(controls);

  const header = document.querySelector(".archive-header");
  const shell = document.querySelector(".archive-shell");
  if (header) header.hidden = true;
  if (shell && stickyBar.children.length) {
    if (header?.parentNode === shell) {
      shell.insertBefore(stickyBar, header.nextSibling);
    } else {
      shell.insertBefore(stickyBar, shell.firstChild);
    }
  }

  /* On mobile, move tabs out of stickyBar so they can be independently sticky */
  const mq = window.matchMedia("(max-width:600px)");
  function reflow() {
    const tabs = stickyBar.parentNode && stickyBar.querySelector(".archive-tabs");
    if (!tabs) return;
    if (mq.matches) {
      stickyBar.parentNode.insertBefore(tabs, stickyBar);
    } else {
      stickyBar.insertBefore(tabs, stickyBar.firstChild);
    }
  }
  reflow();
  mq.addEventListener("change", reflow);

  setupDetailPanel();

  function setupArchiveTabs(tweets, container) {
    const tabKey = `${getArchiveStorageNamespace()}-archive-tab`;
    const posts = tweets.filter((t) => t.dataset.isReply !== "true");
    const replyTweets = tweets.filter((t) => t.dataset.isReply === "true");
    const threadRoots = tweets.filter((t) => t.dataset.hasComments === "true" && t.dataset.isComment !== "true");
    const orphanReplies = replyTweets.filter((t) => t.dataset.isComment !== "true");
    const replyViewTweets = new Set([...threadRoots, ...orphanReplies]);
    if (!replyTweets.length && !threadRoots.length) return;

    const tabBar = document.createElement("div");
    tabBar.className = "archive-tabs";
    tabBar.innerHTML =
      `<button class="archive-tab active" data-archive-tab="posts">` +
      `\u5e16\u5b50<span class="archive-tab-meta">${posts.length}</span></button>` +
    `<button class="archive-tab" data-archive-tab="replies">` +
      `\u56de\u590d<span class="archive-tab-meta">${replyTweets.length}</span></button>`;

    container.appendChild(tabBar);

    const tabButtons = Array.from(tabBar.querySelectorAll(".archive-tab"));
    const scrollPositions = { posts: 0, replies: 0 };
    let currentTab = "posts";

    const saved = readTabState(tabKey);
    if (saved && saved !== "posts") {
      activateTab(saved, true);
    }

    tabButtons.forEach((btn) => {
      btn.addEventListener("click", () => activateTab(btn.dataset.archiveTab, false));
    });

    function activateTab(name, isInit) {
      if (!isInit) scrollPositions[currentTab] = window.scrollY;
      currentTab = name;
      tabButtons.forEach((b) => b.classList.toggle("active", b.dataset.archiveTab === name));
      writeTabState(tabKey, name);
      tweets.forEach((tweet) => {
        const isRepost = tweet.classList.contains("tweet-is-repost");
        const hiddenByFilter = isRepost && (document.querySelector("[data-filter-key=\\"repost\\"]")?.checked || false);
        tweet.hidden = hiddenByFilter || !tabAllowsTweet(tweet, name);
      });
      if (window._archiveSetCommentExpansionForTab) window._archiveSetCommentExpansionForTab();
      if (!isInit) window.scrollTo(0, scrollPositions[name]);
    }

    function tabAllowsTweet(tweet, name) {
      if (name === "posts") return tweet.dataset.isReply !== "true";
      return replyViewTweets.has(tweet);
    }

    function readTabState(key) {
      try { return window.localStorage.getItem(key) || "posts"; } catch { return "posts"; }
    }
    function writeTabState(key, value) {
      try { window.localStorage.setItem(key, value); } catch {}
    }

    // Expose for coordination with filters
    window._archiveActiveTab = () => {
      const active = tabBar.querySelector(".archive-tab.active");
      return active ? active.dataset.archiveTab : "posts";
    };
    window._archiveTabAllowsTweet = tabAllowsTweet;
    window._archiveRefreshTab = () => {
      const active = tabBar.querySelector(".archive-tab.active");
      if (active) activateTab(active.dataset.archiveTab, true);
    };
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
        `<span class="archive-toggle-label">${filter.label}</span>`;
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
    const tweetsWithComments = tweets.filter((t) => t.dataset.hasComments === "true" || t.querySelector(".tweet-comments"));
    if (!tweetsWithComments.length) return;

    const defaultExpand = readFilterState(expandKey);

    // Global toggle
    const toggle = document.createElement("label");
    toggle.className = "archive-toggle";
    toggle.innerHTML =
      `<input class="archive-toggle-input" type="checkbox" data-filter-key="expand-comments"/>` +
      `<span class="archive-toggle-switch" aria-hidden="true"></span>` +
      `<span class="archive-toggle-label">\u9ed8\u8ba4\u5c55\u5f00\u8bc4\u8bba</span>`;
    ctrls.appendChild(toggle);

    const globalCheckbox = toggle.querySelector("input");
    globalCheckbox.checked = defaultExpand;
    globalCheckbox.addEventListener("change", () => {
      writeFilterState(expandKey, globalCheckbox.checked);
      applyCommentExpansionForActiveTab();
    });

    // Per-tweet buttons
    tweetsWithComments.forEach((tweet) => {
      const btn = tweet.querySelector(".tweet-expand-btn");
      if (btn) {
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          const comments = ensureComments(tweet);
          if (comments) setCommentState(tweet, comments.hidden);
        });
      }
    });
    window._archiveSetCommentExpansionForTab = applyCommentExpansionForActiveTab;
    applyCommentExpansionForActiveTab();

    function applyCommentExpansionForActiveTab() {
      tweetsWithComments.forEach((tweet) => {
        setCommentState(tweet, globalCheckbox.checked);
      });
    }
  }

  function setCommentState(tweet, expanded) {
    const comments = expanded ? ensureComments(tweet) : tweet.querySelector(".tweet-comments");
    const btn = tweet.querySelector(".tweet-expand-btn");
    if (comments) comments.hidden = !expanded;
    if (btn) {
      const count = btn.dataset.commentCount;
      btn.textContent = expanded
        ? `\u6536\u8d77\u8bc4\u8bba (${count})`
        : `\u5c55\u5f00\u8bc4\u8bba (${count})`;
    }
  }

  function setupReplyChainExpansion(tweets, ctrls) {
    const expandKey = `${getArchiveStorageNamespace()}-default-expand-reply-chain`;
    const tweetsWithChain = tweets.filter((t) => t.querySelector(".tweet-reply-chain"));
    if (!tweetsWithChain.length) return;

    const defaultExpand = readFilterState(expandKey);

    const toggle = document.createElement("label");
    toggle.className = "archive-toggle";
    toggle.innerHTML =
      `<input class="archive-toggle-input" type="checkbox" data-filter-key="expand-reply-chain"/>` +
      `<span class="archive-toggle-switch" aria-hidden="true"></span>` +
      `<span class="archive-toggle-label">\u9ed8\u8ba4\u5c55\u5f00\u56de\u590d\u94fe</span>`;
    ctrls.appendChild(toggle);

    const globalCheckbox = toggle.querySelector("input");
    globalCheckbox.checked = defaultExpand;
    globalCheckbox.addEventListener("change", () => {
      writeFilterState(expandKey, globalCheckbox.checked);
      tweetsWithChain.forEach((tweet) => {
        const chain = tweet.querySelector(".tweet-reply-chain");
        if (chain) {
          if (globalCheckbox.checked) hydrateReplyChain(tweet, chain);
          chain.open = globalCheckbox.checked;
        }
      });
    });

    tweetsWithChain.forEach((tweet) => {
      const chain = tweet.querySelector(".tweet-reply-chain");
      if (chain) {
        chain.addEventListener("toggle", () => {
          if (chain.open) hydrateReplyChain(tweet, chain);
        });
        if (defaultExpand) hydrateReplyChain(tweet, chain);
        chain.open = defaultExpand;
      }
    });
  }

  function setupDetailPanel() {
    const panel = document.querySelector(".detail-panel");
    if (!panel) return;
    const backdrop = document.querySelector(".detail-panel-backdrop");
    const closeBtn = panel.querySelector(".detail-panel-close");
    const panelBody = panel.querySelector(".detail-panel-body");

    document.querySelector(".archive-shell")?.addEventListener("click", (e) => {
      const tweet = e.target.closest(".tweet");
      if (!tweet) return;
      if (e.target.closest("a, button, video, input, .lightbox")) return;
      openPanel(tweet);
    });

    function openPanel(tweet) {
      panelBody.innerHTML = "";

      // Referenced / replied-to content (shown above post)
      const replyChain = tweet.querySelector(".tweet-reply-chain");
      const record = getDeferredRecord(tweet);
      if (replyChain && record.r) {
        const ctx = document.createElement("div");
        ctx.className = "detail-context";
        const chainClone = replyChain.cloneNode(true);
        if (!chainClone.querySelector(".tweet-ref")) {
          chainClone.insertAdjacentHTML("beforeend", record.r);
        }
        chainClone.open = true;
        ctx.appendChild(chainClone);
        panelBody.appendChild(ctx);
      }

      // Post details (clone without comments, expand button, reply chain)
      const clone = tweet.cloneNode(true);
      const cd = clone.querySelector(".tweet-comments");
      const eb = clone.querySelector(".tweet-expand-btn");
      const rc = clone.querySelector(".tweet-reply-chain");
      if (cd) cd.remove();
      if (eb) eb.remove();
      if (rc) rc.remove();
      clone.removeAttribute("class");
      clone.className = "detail-post";
      panelBody.appendChild(clone);

      // Comments (shown below post)
      const comments = tweet.querySelector(".tweet-comments");
      const commentHtml = comments ? comments.innerHTML : extractDeferredCommentsInnerHtml(record.c || "");
      const commentCount = Number(record.cc || (comments ? comments.dataset.commentTotal || comments.children.length || 0 : 0));
      if (commentHtml && commentCount) {
        const section = document.createElement("div");
        section.className = "detail-comments";
        section.innerHTML =
          "<div class='detail-comments-header'>\u8bc4\u8bba (" + commentCount + ")</div>" +
          commentHtml +
          "<p class='detail-note'>\u4ec5\u5305\u542b\u672c\u5730\u5b58\u6863\u53ca\u5df2\u7f13\u5b58\u56de\u590d\u94fe\u4e2d\u53ef\u89c1\u7684\u8bc4\u8bba\uff0c\u5e76\u6309\u7236\u5b50\u56de\u590d\u5173\u7cfb\u5d4c\u5957\u663e\u793a</p>";
        panelBody.appendChild(section);
      }

      panelBody.scrollTop = 0;
      panel.classList.add("is-open");
      if (backdrop) backdrop.hidden = false;
      document.body.classList.add("detail-panel-open");
    }

    function closePanel() {
      panel.classList.remove("is-open");
      panelBody.innerHTML = "<p class='detail-empty'>\u70b9\u51fb\u5e16\u5b50\u67e5\u770b\u8be6\u60c5</p>";
      if (backdrop) backdrop.hidden = true;
      document.body.classList.remove("detail-panel-open");
    }

    if (backdrop) backdrop.addEventListener("click", closePanel);
    if (closeBtn) closeBtn.addEventListener("click", closePanel);

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && panel.classList.contains("is-open")) closePanel();
    });
  }

  function applyTweetFilters(tweets, filters, ctrls) {
    const enabledFilters = new Set(
      filters
        .filter((f) => ctrls.querySelector(`[data-filter-key="${f.key}"]`)?.checked)
        .map((f) => f.key),
    );
    const activeTab = window._archiveActiveTab ? window._archiveActiveTab() : null;
    tweets.forEach((tweet) => {
      const hiddenByFilter = filters.some(
        (f) => enabledFilters.has(f.key) && tweet.classList.contains(`tweet-is-${f.key}`),
      );
      if (hiddenByFilter) {
        tweet.hidden = true;
      } else if (activeTab) {
        const allowedByTab = window._archiveTabAllowsTweet
          ? window._archiveTabAllowsTweet(tweet, activeTab)
          : true;
        tweet.hidden = !allowedByTab;
      } else {
        tweet.hidden = false;
      }
    });
  }

  function getFirstSegmentText(body) {
    if (!body) return "";
    const [firstSegmentHtml = ""] = body.innerHTML.split(/<br\\s*\\/?>/i);
    const scratch = document.createElement("div");
    scratch.innerHTML = firstSegmentHtml;
    return (scratch.textContent || "").trim();
  }

  function readDeferredData() {
    if (deferredData) return deferredData;
    const node = document.getElementById("archive-deferred-data");
    if (!node) {
      deferredData = {};
      return deferredData;
    }
    try {
      deferredData = JSON.parse(node.textContent || "{}");
    } catch {
      deferredData = {};
    }
    return deferredData;
  }

  function getDeferredRecord(tweet) {
    const id = tweet?.dataset?.tweetId;
    const data = readDeferredData();
    return id && data[id] ? data[id] : {};
  }

  function ensureComments(tweet) {
    let comments = tweet.querySelector(".tweet-comments");
    if (comments) return comments;
    const record = getDeferredRecord(tweet);
    if (!record.c) return null;
    const footer = tweet.querySelector(".tweet-footer");
    if (footer) {
      footer.insertAdjacentHTML("beforebegin", record.c);
      comments = footer.previousElementSibling;
    } else {
      tweet.insertAdjacentHTML("beforeend", record.c);
      comments = tweet.lastElementChild;
    }
    return comments?.matches(".tweet-comments") ? comments : tweet.querySelector(".tweet-comments");
  }

  function hydrateReplyChain(tweet, chain) {
    if (!chain || chain.querySelector(".tweet-ref")) return;
    const record = getDeferredRecord(tweet);
    if (record.r) chain.insertAdjacentHTML("beforeend", record.r);
  }

  function extractDeferredCommentsInnerHtml(html) {
    if (!html) return "";
    const template = document.createElement("template");
    template.innerHTML = html;
    const comments = template.content.querySelector(".tweet-comments");
    return comments ? comments.innerHTML : "";
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
.archive-layout{display:flex;justify-content:center;min-height:100vh}
.archive-shell{max-width:600px;width:100%;border-left:1px solid var(--line);border-right:1px solid var(--line);min-height:100vh}
.archive-header{padding:12px 16px;border-bottom:1px solid var(--line)}
.archive-sticky-bar{position:sticky;top:0;z-index:10;background:rgba(255,255,255,.85);backdrop-filter:blur(12px)}
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
.tweet{padding:12px 16px;border-bottom:1px solid var(--line);transition:background .15s;cursor:pointer;content-visibility:auto;contain-intrinsic-size:auto 240px}
.tweet:hover{background:var(--hover)}
h3{font-size:13px;font-weight:400;color:var(--muted)}
p{margin:4px 0 0;font-size:15px;color:var(--ink);line-height:1.5;overflow-wrap:anywhere}
.tweet p:last-child{margin-bottom:4px}
.tweet-media{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:2px;margin:10px 0 4px;border-radius:16px;overflow:hidden}
.tweet-media-item{width:100%}
.tweet-media-item img{display:block;width:100%;height:auto;cursor:zoom-in;background:var(--line);transition:opacity .15s}
.tweet-media-item video{display:block;width:100%;height:auto;background:#000}
.tweet-media-item img:hover{opacity:.88}
.tweet-media-unavailable{color:var(--muted);font-size:14px}
.tweet-ref{margin:10px 0 4px;padding:12px;border:1px solid var(--line);border-radius:12px;background:var(--bg)}
.tweet-ref p{margin:4px 0 0;font-size:14px;color:var(--ink);line-height:1.45}
.tweet-ref-header{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap}
.tweet-ref-name{font-size:14px;font-weight:700;color:var(--ink)}
.tweet-ref-handle{font-size:13px;color:var(--muted)}
.tweet-ref-time{display:block;margin-top:6px;font-size:12px;color:var(--muted)}
.tweet-ref .tweet-media{margin:8px 0 4px;border-radius:12px}
.tweet-ref-missing{border-style:dashed;background:var(--hover)}
.tweet-ref-missing p{color:var(--muted)}
.tweet-reply-chain{margin:8px 0 4px}
.tweet-reply-chain summary{color:var(--muted);font-size:13px;cursor:pointer}
.tweet-reply-chain[open] summary{margin-bottom:6px}
.tweet-comment{background:var(--hover)}
.tweet-comment-node{position:relative;margin:0 0 8px}
.tweet-comment-node>.tweet-ref{margin:0 0 8px}
.tweet-comment-children{margin-left:9px;padding-left:5px;border-left:2px solid var(--line)}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
.lightbox{position:fixed;inset:0;z-index:1000;display:none;align-items:center;justify-content:center;padding:24px;background:rgba(0,0,0,.7);cursor:pointer}
.lightbox.is-open{display:flex}
.lightbox-image{display:block;max-width:min(100vw - 48px,1400px);max-height:calc(100vh - 48px);border-radius:12px;background:#000}
body.lightbox-open{overflow:hidden}
@media (max-width:600px){.archive-shell{border-left:none;border-right:none}.tweet{padding:10px 16px}.tweet-media{border-radius:12px}}
@media (prefers-color-scheme:dark){:root{--bg:#000;--hover:#080808;--ink:#f7f9f9;--muted:#8b98a5;--line:#2f3336;--accent:#1d9bf0}.archive-sticky-bar{background:rgba(0,0,0,.85)}.tweet-media-item img{background:var(--line)}.lightbox-image{background:var(--bg)}}
"""
ARCHIVE_JS = """
(() => {
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

  document.addEventListener("click", (event) => {
    const image = event.target.closest(".tweet-media-item img");
    if (!image) return;
    event.preventDefault();
    event.stopPropagation();
    openLightbox(image);
  }, true);

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && lightbox.classList.contains("is-open")) {
      closeLightbox();
      return;
    }
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    const image = event.target.closest(".tweet-media-item img");
    if (!image) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    openLightbox(image);
  }, true);
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
@media (prefers-color-scheme:dark){:root{--bg:#000;--hover:#080808;--ink:#f7f9f9;--muted:#8b98a5;--line:#2f3336;--accent:#1d9bf0}.hero{background:rgba(0,0,0,.85)}}
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
    tweet_id: str = ""
    conversation_id: str = ""
    in_reply_to_status_id: str = ""
    comment_html: str = ""
    comments_placeholder: str = ""
    expand_placeholder: str = ""
    comment_parent_entries: Tuple["ArchiveEntry", ...] = ()
    deferred_comments_html: str = ""
    deferred_comment_count: int = 0
    deferred_comment_latest_ts: int = 0
    deferred_comment_ids: Tuple[str, ...] = ()
    deferred_reply_chain_html: str = ""
    deferred_reply_chain_count: int = 0


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
    conversation_id_hint: str = ""
    before_created_at_hint: str = ""
    allow_conversation_wayback: bool = True


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
    label: str = ""

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
        description = " + ".join(methods) if methods else "no X API credentials"
        return f"{self.label}: {description}" if self.label else description


@dataclass(frozen=True)
class ReferencedTweetInfo:
    kind: str  # "quoted" or "replied_to"
    text: str
    author: str
    display_name: str
    created_at: str
    url: str
    tweet_id: str = ""
    conversation_id: str = ""
    in_reply_to_status_id: str = ""
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


def build_asset_candidate_urls(
    asset_url: str,
    snapshot_timestamp: str,
    allow_wayback_fallback: bool = True,
) -> List[str]:
    candidates: List[str] = []
    seen = set()

    def add(url: str) -> None:
        if not url or url in seen:
            return
        seen.add(url)
        candidates.append(url)

    for source_url in build_asset_source_variants(asset_url):
        if allow_wayback_fallback:
            for candidate in build_archived_urls_for_source_url(source_url, snapshot_timestamp):
                add(candidate)
        else:
            add(source_url)

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


def build_image_candidate_urls(
    image_url: str,
    snapshot_timestamp: str,
    allow_wayback_fallback: bool = True,
) -> List[str]:
    return build_asset_candidate_urls(image_url, snapshot_timestamp, allow_wayback_fallback=allow_wayback_fallback)


def build_video_candidate_urls(
    video_url: str,
    snapshot_timestamp: str,
    allow_wayback_fallback: bool = True,
) -> List[str]:
    return build_asset_candidate_urls(video_url, snapshot_timestamp, allow_wayback_fallback=allow_wayback_fallback)


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


def x_api_access_index_path(asset_dir: Path) -> Path:
    return asset_dir / X_API_ACCESS_INDEX_NAME


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


def replace_negative_media_cache(asset_dir: Path, cache: Dict[str, dict]) -> None:
    cache_path = media_negative_index_path(asset_dir)
    with IMAGE_CACHE_LOCK:
        with locked_media_index(asset_dir):
            cleaned_cache = _clean_negative_media_cache_entries(cache)
            cache.clear()
            cache.update(cleaned_cache)
            payload = {
                "version": MEDIA_NEGATIVE_CACHE_VERSION,
                "entries": dict(sorted(cleaned_cache.items())),
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


def parse_env_file_values(path: Path) -> Dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}

    values: Dict[str, str] = {}
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
        values[key] = parse_env_value(value)
    return values


def load_env_file(path: Path, *, override: bool = False) -> int:
    values = parse_env_file_values(path)
    loaded = 0
    for key, value in values.items():
        if not override and key in os.environ:
            continue
        os.environ[key] = value
        loaded += 1
    return loaded


def parse_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(0.0, value)


def set_x_api_min_interval(seconds: float) -> None:
    global X_API_MIN_INTERVAL_SECONDS
    X_API_MIN_INTERVAL_SECONDS = max(0.0, seconds)


def is_x_api_request_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return parsed.netloc.lower() in {"api.x.com", "api.twitter.com"}


def wait_for_x_api_rate_limit(url: str) -> None:
    global X_API_LAST_REQUEST_AT
    if X_API_MIN_INTERVAL_SECONDS <= 0 or not is_x_api_request_url(url):
        return
    with X_API_RATE_LIMIT_LOCK:
        now = time.monotonic()
        wait_seconds = X_API_LAST_REQUEST_AT + X_API_MIN_INTERVAL_SECONDS - now
        if wait_seconds > 0:
            time.sleep(wait_seconds)
            now = time.monotonic()
        X_API_LAST_REQUEST_AT = now


def parse_retry_after_seconds(value: str) -> Optional[float]:
    raw = _coerce_string(value).strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def rate_limit_sleep_seconds_from_response(response: Optional[requests.Response]) -> Optional[float]:
    if response is None:
        return None
    retry_after = parse_retry_after_seconds(response.headers.get("Retry-After", ""))
    reset_at = parse_retry_after_seconds(response.headers.get("x-rate-limit-reset", ""))
    reset_wait = None
    if reset_at is not None:
        reset_wait = max(0.0, reset_at - time.time() + X_API_RATE_LIMIT_RESET_PADDING_SECONDS)
    candidates = [value for value in (retry_after, reset_wait) if value is not None]
    if not candidates:
        return None
    return max(candidates)


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
                    wait_for_x_api_rate_limit(request_url)
                    resp = get_session().get(request_url, params=params, headers=headers, timeout=timeout)
                    resp.raise_for_status()
                    return resp
                except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as exc:
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
            retry_sleep = (0.4 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.2)
            if status_code == 429:
                retry_after_sleep = rate_limit_sleep_seconds_from_response(exc.response)
                if retry_after_sleep is not None:
                    retry_sleep = max(retry_sleep, retry_after_sleep)
            time.sleep(retry_sleep)
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


def _get_first_mapping_value(values: Dict[str, str], *names: str) -> str:
    for name in names:
        value = values.get(name, "").strip()
        if value:
            return value
    return ""


def _oauth1_credentials_from_values(values: Dict[str, str]) -> Optional[XApiOAuth1Credentials]:
    credentials = XApiOAuth1Credentials(
        api_key=_get_first_mapping_value(
            values,
            "X_API_KEY",
            "X_CONSUMER_KEY",
            "X_CUSTOMER_KEY",
            "TWITTER_API_KEY",
            "TWITTER_CONSUMER_KEY",
            "TWITTER_CUSTOMER_KEY",
        ),
        api_key_secret=_get_first_mapping_value(
            values,
            "X_API_KEY_SECRET",
            "X_CONSUMER_SECRET",
            "X_CUSTOMER_KEY_SECRET",
            "TWITTER_API_KEY_SECRET",
            "TWITTER_CONSUMER_SECRET",
            "TWITTER_CUSTOMER_KEY_SECRET",
        ),
        access_token=_get_first_mapping_value(
            values,
            "X_ACCESS_TOKEN",
            "X_OAUTH_TOKEN",
            "TWITTER_ACCESS_TOKEN",
            "TWITTER_OAUTH_TOKEN",
        ),
        access_token_secret=_get_first_mapping_value(
            values,
            "X_ACCESS_TOKEN_SECRET",
            "X_OAUTH_TOKEN_SECRET",
            "TWITTER_ACCESS_TOKEN_SECRET",
            "TWITTER_OAUTH_TOKEN_SECRET",
        ),
    )
    return credentials if credentials.is_complete() else None


def get_x_oauth1_credentials() -> Optional[XApiOAuth1Credentials]:
    return _oauth1_credentials_from_values(dict(os.environ))


def get_x_api_auth(bearer_env_name: str = "X_BEARER_TOKEN") -> XApiAuth:
    return XApiAuth(
        bearer_token=get_x_bearer_token(bearer_env_name),
        oauth1=get_x_oauth1_credentials(),
    )


def get_x_api_auth_from_values(
    values: Dict[str, str],
    bearer_env_name: str = "X_BEARER_TOKEN",
    label: str = "",
) -> XApiAuth:
    bearer_token = _get_first_mapping_value(values, bearer_env_name, "X_BEARER_TOKEN", "TWITTER_BEARER_TOKEN")
    return XApiAuth(
        bearer_token=bearer_token,
        oauth1=_oauth1_credentials_from_values(values),
        label=label,
    )


def get_x_api_auths_from_env_files(env_files: List[str], bearer_env_name: str = "X_BEARER_TOKEN") -> List[XApiAuth]:
    auths: List[XApiAuth] = []
    seen = set()
    for env_file in env_files:
        env_path = Path(env_file)
        values = parse_env_file_values(env_path)
        auth = get_x_api_auth_from_values(values, bearer_env_name, env_file)
        if not auth.has_network_auth():
            continue
        identity = x_api_auth_identity(auth)
        if identity in seen:
            continue
        seen.add(identity)
        auths.append(auth)
    return auths


def normalize_x_api_auth(value) -> XApiAuth:
    if isinstance(value, XApiAuth):
        return value
    if isinstance(value, str):
        return XApiAuth(bearer_token=value.strip())
    if value is None:
        return XApiAuth()
    return XApiAuth(bearer_token=str(value).strip())


def normalize_x_api_auths(value) -> List[XApiAuth]:
    if isinstance(value, (list, tuple)):
        auths: List[XApiAuth] = []
        for item in value:
            auths.extend(normalize_x_api_auths(item))
        return [auth for auth in auths if auth.has_network_auth()]
    auth = normalize_x_api_auth(value)
    return [auth] if auth.has_network_auth() else []


def _hash_for_log(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def x_api_auth_identity(auth: XApiAuth) -> str:
    if auth.oauth1 is not None and auth.oauth1.is_complete():
        return f"oauth1:{auth.oauth1.cache_identity()}"
    if auth.bearer_token:
        return f"bearer:{_hash_for_log(auth.bearer_token)}"
    return "none"


def new_x_api_access_index() -> dict:
    return {
        "version": X_API_ACCESS_INDEX_VERSION,
        "updated_at": "",
        "envs": {},
    }


def load_x_api_access_index(asset_dir: Path) -> dict:
    path = x_api_access_index_path(asset_dir)
    if not path.exists():
        return new_x_api_access_index()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return new_x_api_access_index()
    if not isinstance(payload, dict):
        return new_x_api_access_index()
    payload.setdefault("version", X_API_ACCESS_INDEX_VERSION)
    payload.setdefault("updated_at", "")
    if not isinstance(payload.get("envs"), dict):
        payload["envs"] = {}
    return payload


def write_x_api_access_index(asset_dir: Path, access_index: Optional[dict]) -> None:
    if not isinstance(access_index, dict):
        return
    access_index["version"] = X_API_ACCESS_INDEX_VERSION
    access_index["updated_at"] = _utc_now_iso()
    path = x_api_access_index_path(asset_dir)
    path.write_text(json.dumps(access_index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _x_api_payload_author_info(payload: Optional[dict]) -> Tuple[str, str, str]:
    if not isinstance(payload, dict):
        return "", "", ""
    primary_node = find_primary_tweet_node(payload, "")
    if primary_node is None:
        primary_node = payload.get("data") if isinstance(payload.get("data"), dict) else None
    if not isinstance(primary_node, dict):
        return "", "", ""
    username, display_name = _extract_user_info(payload, primary_node)
    metadata = _extract_metadata_from_node(primary_node)
    author_id = _coerce_string(primary_node.get("author_id")).strip()
    return username, display_name, author_id or metadata.get("author_id", "")


def record_x_api_access_result(
    access_index: Optional[dict],
    auth: XApiAuth,
    tweet_id: str,
    status: str,
    *,
    error: str = "",
    payload: Optional[dict] = None,
    author_hint: str = "",
) -> None:
    if not isinstance(access_index, dict) or not auth.has_network_auth():
        return

    username, display_name, author_id = _x_api_payload_author_info(payload)
    account_key = username or _normalize_tweet_author_hint(author_hint) or "unknown"
    timestamp = _utc_now_iso()
    env_key = auth.label or x_api_auth_identity(auth)

    with X_API_ACCESS_INDEX_LOCK:
        envs = access_index.setdefault("envs", {})
        env_entry = envs.setdefault(
            env_key,
            {
                "credential_id": x_api_auth_identity(auth),
                "auth": auth.describe(),
                "accounts": {},
                "tweet_results": {},
            },
        )
        env_entry["credential_id"] = x_api_auth_identity(auth)
        env_entry["auth"] = auth.describe()
        accounts = env_entry.setdefault("accounts", {})
        account = accounts.setdefault(
            account_key,
            {
                "username": account_key if account_key != "unknown" else "",
                "display_name": "",
                "user_id": "",
                "success_count": 0,
                "failure_count": 0,
                "last_success_at": "",
                "last_failure_at": "",
                "last_error": "",
                "tweet_ids": [],
            },
        )
        if username:
            account["username"] = username
        if display_name:
            account["display_name"] = display_name
        if author_id:
            account["user_id"] = author_id
        if status == "success":
            account["success_count"] = int(account.get("success_count", 0)) + 1
            account["last_success_at"] = timestamp
        else:
            account["failure_count"] = int(account.get("failure_count", 0)) + 1
            account["last_failure_at"] = timestamp
            account["last_error"] = error
        tweet_ids = account.setdefault("tweet_ids", [])
        if tweet_id and tweet_id not in tweet_ids:
            tweet_ids.append(tweet_id)
            if len(tweet_ids) > 50:
                del tweet_ids[:-50]
        tweet_results = env_entry.setdefault("tweet_results", {})
        tweet_results[tweet_id or f"unknown:{timestamp}"] = {
            "status": status,
            "account": account_key,
            "error": error,
            "updated_at": timestamp,
        }


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
    cache_key = ("bearer", _hash_for_log(bearer_token), tweet_id)
    with X_API_LOOKUP_LOCK:
        cached = X_API_LOOKUP_CACHE.get(cache_key)
        if cached is None:
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
            X_API_LOOKUP_CACHE[cache_key] = result
        return result

    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        error = format_api_error_payload(payload) or "X API response did not include this post."
        result = (None, error)
        with X_API_LOOKUP_LOCK:
            X_API_LOOKUP_CACHE[cache_key] = result
        return result

    result = (payload, "")
    with X_API_LOOKUP_LOCK:
        X_API_LOOKUP_CACHE[cache_key] = result
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


def fetch_x_api_tweet_payload_with_auths(
    auth_value,
    tweet_id: str,
    *,
    access_index: Optional[dict] = None,
    author_hint: str = "",
) -> Tuple[Optional[dict], str, str]:
    errors: List[str] = []
    auths = normalize_x_api_auths(auth_value)
    use_error_prefix = len(auths) > 1
    for auth in auths:
        payload, error, source = fetch_x_api_tweet_payload_with_auth(auth, tweet_id)
        if payload is not None:
            record_x_api_access_result(
                access_index,
                auth,
                tweet_id,
                "success",
                payload=payload,
                author_hint=author_hint,
            )
            source_label = auth.label or source
            return payload, "", f"{source}:{source_label}" if source_label != source else source
        if error:
            errors.append(f"{auth.label or x_api_auth_identity(auth)}: {error}" if use_error_prefix else error)
            record_x_api_access_result(
                access_index,
                auth,
                tweet_id,
                "failure",
                error=error,
                author_hint=author_hint,
            )
    return None, " ".join(errors) if errors else "X API credentials were not configured.", ""


def _format_x_api_endpoint_exception(exc: Exception, fallback_message: str) -> str:
    cause = exc.__cause__ if isinstance(exc, RuntimeError) and exc.__cause__ is not None else exc
    if isinstance(cause, requests.HTTPError) and cause.response is not None:
        try:
            payload = cause.response.json()
        except Exception:
            payload = {}
        return format_api_error_payload(payload) or f"{fallback_message} returned HTTP {cause.response.status_code}."
    return f"{fallback_message} failed: {exc}"


def fetch_x_api_endpoint_payload_with_auth(
    auth_value,
    url: str,
    params: Optional[dict],
    endpoint_label: str,
) -> Tuple[Optional[dict], str, str]:
    auth = normalize_x_api_auth(auth_value)
    errors: List[str] = []

    if auth.oauth1 is not None and auth.oauth1.is_complete():
        headers = {"Authorization": build_oauth1_authorization_header("GET", url, params or {}, auth.oauth1)}
        try:
            resp = get_with_retry(url, params=params, headers=headers, timeout=X_API_TIMEOUT)
            payload = resp.json()
        except Exception as exc:
            errors.append(_format_x_api_endpoint_exception(exc, f"X API OAuth 1.0a {endpoint_label}"))
        else:
            if isinstance(payload, dict):
                return payload, "", "x_api_oauth1"
            errors.append(f"X API OAuth 1.0a {endpoint_label} response was not a JSON object.")

    if auth.bearer_token:
        headers = {"Authorization": f"Bearer {auth.bearer_token}"}
        try:
            resp = get_with_retry(url, params=params, headers=headers, timeout=X_API_TIMEOUT)
            payload = resp.json()
        except Exception as exc:
            errors.append(_format_x_api_endpoint_exception(exc, f"X API bearer {endpoint_label}"))
        else:
            if isinstance(payload, dict):
                return payload, "", "x_api_bearer"
            errors.append(f"X API bearer {endpoint_label} response was not a JSON object.")

    return None, " ".join(errors) if errors else "X API credentials were not configured.", ""


def fetch_x_api_app_only_bearer(credentials: XApiOAuth1Credentials) -> Tuple[str, str]:
    if not credentials.api_key or not credentials.api_key_secret:
        return "", "X API consumer key/secret were not configured."

    cache_key = hashlib.sha256(
        f"{credentials.api_key}\0{credentials.api_key_secret}".encode("utf-8")
    ).hexdigest()[:16]
    with X_API_APP_BEARER_LOCK:
        cached = X_API_APP_BEARER_CACHE.get(cache_key)
        if cached is not None:
            return cached

    encoded_key = quote(credentials.api_key, safe="")
    encoded_secret = quote(credentials.api_key_secret, safe="")
    basic_token = base64.b64encode(f"{encoded_key}:{encoded_secret}".encode("utf-8")).decode("ascii")
    token_url = "https://api.twitter.com/oauth2/token"
    try:
        wait_for_x_api_rate_limit(token_url)
        resp = requests.post(
            token_url,
            headers={
                "Authorization": f"Basic {basic_token}",
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                "User-Agent": USER_AGENT,
            },
            data="grant_type=client_credentials",
            timeout=X_API_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        error = _format_x_api_endpoint_exception(exc, "X API app-only bearer exchange")
        result = ("", error)
        with X_API_APP_BEARER_LOCK:
            X_API_APP_BEARER_CACHE[cache_key] = result
        return result

    token = _coerce_string(payload.get("access_token")).strip() if isinstance(payload, dict) else ""
    token_type = _coerce_string(payload.get("token_type")).strip().lower() if isinstance(payload, dict) else ""
    if not token or token_type != "bearer":
        result = ("", "X API app-only bearer exchange did not return a bearer token.")
        with X_API_APP_BEARER_LOCK:
            X_API_APP_BEARER_CACHE[cache_key] = result
        return result

    result = (token, "")
    with X_API_APP_BEARER_LOCK:
        X_API_APP_BEARER_CACHE[cache_key] = result
    return result


def fetch_x_api_endpoint_payload_with_app_auth(
    auth_value,
    url: str,
    params: Optional[dict],
    endpoint_label: str,
) -> Tuple[Optional[dict], str, str]:
    auth = normalize_x_api_auth(auth_value)
    bearer_token = auth.bearer_token
    source = "x_api_bearer" if bearer_token else ""
    errors: List[str] = []

    if not bearer_token and auth.oauth1 is not None:
        bearer_token, bearer_error = fetch_x_api_app_only_bearer(auth.oauth1)
        if bearer_error:
            errors.append(bearer_error)
        elif bearer_token:
            source = "x_api_app_only"

    if not bearer_token:
        return None, " ".join(errors) if errors else "X API app-only bearer credentials were not configured.", ""

    headers = {"Authorization": f"Bearer {bearer_token}"}
    try:
        resp = get_with_retry(url, params=params, headers=headers, timeout=X_API_TIMEOUT)
        payload = resp.json()
    except Exception as exc:
        return None, _format_x_api_endpoint_exception(exc, f"X API {endpoint_label}"), source

    if not isinstance(payload, dict):
        return None, f"X API {endpoint_label} response was not a JSON object.", source
    return payload, "", source


def fetch_x_api_user_by_username_with_auth(auth_value, username: str) -> Tuple[Optional[dict], str, str]:
    url = f"{X_API_BASE_URL}/users/by/username/{username}"
    params = {"user.fields": X_API_USER_FIELDS}
    payload, error, source = fetch_x_api_endpoint_payload_with_auth(auth_value, url, params, "user lookup")
    if payload is None:
        return None, error, source
    if not isinstance(payload.get("data"), dict):
        return None, format_api_error_payload(payload) or "X API user lookup response did not include a user.", source
    return payload, "", source


def fetch_x_api_timeline_page_with_auth(
    auth_value,
    user_id: str,
    *,
    pagination_token: str = "",
    page_size: int = DEFAULT_X_API_TIMELINE_PAGE_SIZE,
) -> Tuple[Optional[dict], str, str]:
    url = f"{X_API_BASE_URL}/users/{user_id}/tweets"
    params = {
        "max_results": str(page_size),
        "tweet.fields": X_API_TWEET_FIELDS,
        "expansions": X_API_EXPANSIONS,
        "user.fields": X_API_USER_FIELDS,
        "media.fields": X_API_MEDIA_FIELDS,
    }
    if pagination_token:
        params["pagination_token"] = pagination_token
    payload, error, source = fetch_x_api_endpoint_payload_with_auth(auth_value, url, params, "timeline lookup")
    if payload is None:
        return None, error, source
    if "data" not in payload and not isinstance(payload.get("meta"), dict):
        return None, format_api_error_payload(payload) or "X API timeline response did not include tweets.", source
    return payload, "", source


def parse_x_api_created_at_timestamp(created_at: str) -> str:
    raw = _coerce_string(created_at).strip()
    if raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")
        except ValueError:
            pass
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def build_x_api_timeline_snapshot_payload(
    tweet: dict,
    includes: dict,
    *,
    username: str,
    source: str,
) -> dict:
    payload = {
        "data": tweet,
        "includes": includes if isinstance(includes, dict) else {},
    }
    cache = _get_x_api_json_cache(payload, create=True)
    cache["source"] = "x_api_timeline"
    cache["source_auth"] = source
    cache["timeline_user"] = username
    cache["fetched_at"] = _utc_now_iso()
    return payload


def write_x_api_timeline_snapshot_records(
    username: str,
    timeline_payloads: List[dict],
    json_dir: Path,
    *,
    resume: bool = True,
    source: str = "",
) -> Tuple[List[SnapshotRecord], int, int]:
    records: List[SnapshotRecord] = []
    written_count = 0
    reused_count = 0
    seen_tweet_ids = set()
    json_dir.mkdir(parents=True, exist_ok=True)

    for page_payload in timeline_payloads:
        if not isinstance(page_payload, dict):
            continue
        includes = page_payload.get("includes", {})
        tweets = page_payload.get("data", [])
        if isinstance(tweets, dict):
            tweets = [tweets]
        if not isinstance(tweets, list):
            continue
        for tweet in tweets:
            if not isinstance(tweet, dict):
                continue
            tweet_id = _coerce_string(tweet.get("id")).strip()
            if not tweet_id or tweet_id in seen_tweet_ids:
                continue
            seen_tweet_ids.add(tweet_id)
            timestamp = parse_x_api_created_at_timestamp(_coerce_string(tweet.get("created_at")))
            original_url = reconstruct_original_url(username, tweet_id)
            json_path = build_json_output_path(json_dir, timestamp, original_url, {"tweet_id": tweet_id})
            record = SnapshotRecord(timestamp=timestamp, original_url=original_url, json_path=json_path)
            records.append(record)
            if resume and json_path.exists() and load_snapshot_json(json_path) is not None:
                reused_count += 1
                continue
            snapshot_payload = build_x_api_timeline_snapshot_payload(
                tweet,
                includes,
                username=username,
                source=source,
            )
            write_snapshot_json_payload(json_path, snapshot_payload)
            written_count += 1

    return sorted(records, key=lambda record: (record.timestamp, record.original_url, record.json_path.name)), written_count, reused_count


def merge_snapshot_records_prefer_first(*record_groups: List[SnapshotRecord]) -> List[SnapshotRecord]:
    merged: Dict[str, SnapshotRecord] = {}
    for records in record_groups:
        for record in records:
            tweet_id = snapshot_record_tweet_id(record)
            key = f"tweet:{tweet_id}" if tweet_id else f"path:{record.original_url}:{record.json_path.name}"
            if key not in merged:
                merged[key] = record
    return sorted(merged.values(), key=lambda record: (record.timestamp, record.original_url, record.json_path.name))


def fetch_x_api_timeline_payloads_with_auths(
    username: str,
    auth_value,
    *,
    pages: int = DEFAULT_X_API_TIMELINE_PAGES,
    page_size: int = DEFAULT_X_API_TIMELINE_PAGE_SIZE,
    access_index: Optional[dict] = None,
) -> Tuple[List[dict], str, str]:
    auths = normalize_x_api_auths(auth_value)
    errors: List[str] = []
    use_error_prefix = len(auths) > 1
    for auth in auths:
        user_payload, user_error, user_source = fetch_x_api_user_by_username_with_auth(auth, username)
        source_label = auth.label or user_source
        full_source = f"{user_source}:{source_label}" if user_source and source_label != user_source else user_source
        if user_payload is None:
            if user_error:
                errors.append(f"{auth.label or x_api_auth_identity(auth)}: {user_error}" if use_error_prefix else user_error)
                record_x_api_access_result(
                    access_index,
                    auth,
                    f"user:{username}",
                    "failure",
                    error=user_error,
                    author_hint=username,
                )
            continue

        user_info = user_payload.get("data", {})
        user_id = _coerce_string(user_info.get("id")).strip()
        record_x_api_access_result(
            access_index,
            auth,
            f"user:{username}",
            "success",
            payload={"data": {"author_id": user_id}, "includes": {"users": [user_info]}},
            author_hint=username,
        )
        if not user_id:
            errors.append(f"{auth.label or x_api_auth_identity(auth)}: X API user lookup did not include a user id." if use_error_prefix else "X API user lookup did not include a user id.")
            continue

        payloads: List[dict] = []
        next_token = ""
        page_index = 0
        while pages == 0 or page_index < pages:
            page_payload, page_error, page_source = fetch_x_api_timeline_page_with_auth(
                auth,
                user_id,
                pagination_token=next_token,
                page_size=page_size,
            )
            if page_payload is None:
                record_x_api_access_result(
                    access_index,
                    auth,
                    f"timeline:{user_id}",
                    "failure",
                    error=page_error,
                    author_hint=username,
                )
                if not payloads:
                    errors.append(f"{auth.label or x_api_auth_identity(auth)}: {page_error}" if use_error_prefix else page_error)
                    break
                return payloads, page_error, full_source or page_source

            payloads.append(page_payload)
            for tweet in page_payload.get("data", []) or []:
                if isinstance(tweet, dict):
                    record_x_api_access_result(
                        access_index,
                        auth,
                        _coerce_string(tweet.get("id")).strip(),
                        "success",
                        payload={"data": tweet, "includes": page_payload.get("includes", {})},
                        author_hint=username,
                    )
            meta = page_payload.get("meta", {})
            next_token = _coerce_string(meta.get("next_token")).strip() if isinstance(meta, dict) else ""
            if not next_token:
                break
            page_index += 1

        if payloads:
            return payloads, "", full_source

    return [], " ".join(errors) if errors else "X API credentials were not configured.", ""


def fetch_x_api_conversation_page_with_auth(
    auth_value,
    conversation_id: str,
    *,
    search_mode: str = "all",
    next_token: str = "",
    page_size: int = DEFAULT_X_API_TIMELINE_PAGE_SIZE,
) -> Tuple[Optional[dict], str, str]:
    endpoint = "search/all" if search_mode == "all" else "search/recent"
    url = f"{X_API_BASE_URL}/tweets/{endpoint}"
    effective_page_size = min(page_size, 500 if search_mode == "all" else 100)
    params = {
        "query": f"conversation_id:{conversation_id} -is:retweet",
        "max_results": str(effective_page_size),
        "tweet.fields": X_API_TWEET_FIELDS,
        "expansions": X_API_EXPANSIONS,
        "user.fields": X_API_USER_FIELDS,
        "media.fields": X_API_MEDIA_FIELDS,
    }
    if next_token:
        params["next_token"] = next_token
    payload, error, source = fetch_x_api_endpoint_payload_with_app_auth(
        auth_value,
        url,
        params,
        f"conversation {endpoint} lookup",
    )
    if payload is None:
        return None, error, source
    if "data" not in payload and not isinstance(payload.get("meta"), dict):
        return None, format_api_error_payload(payload) or "X API conversation search response did not include posts.", source
    return payload, "", source


def fetch_x_api_conversation_payloads_with_auths(
    conversation_id: str,
    auth_value,
    *,
    search_mode: str = "all",
    pages: int = 0,
    page_size: int = DEFAULT_X_API_TIMELINE_PAGE_SIZE,
    access_index: Optional[dict] = None,
) -> Tuple[List[dict], str, str]:
    auths = normalize_x_api_auths(auth_value)
    errors: List[str] = []
    use_error_prefix = len(auths) > 1
    for auth in auths:
        payloads: List[dict] = []
        next_token = ""
        page_index = 0
        source = ""
        while pages == 0 or page_index < pages:
            page_payload, page_error, page_source = fetch_x_api_conversation_page_with_auth(
                auth,
                conversation_id,
                search_mode=search_mode,
                next_token=next_token,
                page_size=page_size,
            )
            source_label = auth.label or page_source
            source = f"{page_source}:{source_label}" if page_source and source_label != page_source else page_source
            if page_payload is None:
                record_x_api_access_result(
                    access_index,
                    auth,
                    f"conversation:{conversation_id}",
                    "failure",
                    error=page_error,
                    author_hint=conversation_id,
                )
                if not payloads:
                    errors.append(f"{auth.label or x_api_auth_identity(auth)}: {page_error}" if use_error_prefix else page_error)
                    break
                return payloads, page_error, source

            payloads.append(page_payload)
            for tweet in page_payload.get("data", []) or []:
                if isinstance(tweet, dict):
                    record_x_api_access_result(
                        access_index,
                        auth,
                        _coerce_string(tweet.get("id")).strip(),
                        "success",
                        payload={"data": tweet, "includes": page_payload.get("includes", {})},
                    )
            meta = page_payload.get("meta", {})
            next_token = _coerce_string(meta.get("next_token")).strip() if isinstance(meta, dict) else ""
            if not next_token:
                break
            page_index += 1

        if payloads:
            return payloads, "", source

    return [], " ".join(errors) if errors else "X API app-only credentials were not configured.", ""


def _included_users_by_id(includes: dict) -> Dict[str, dict]:
    users_by_id: Dict[str, dict] = {}
    if not isinstance(includes, dict):
        return users_by_id
    for user in includes.get("users", []) or []:
        if not isinstance(user, dict):
            continue
        user_id = _coerce_string(user.get("id")).strip()
        if user_id:
            users_by_id[user_id] = user
    return users_by_id


def _username_for_x_api_tweet(tweet: dict, includes: dict, fallback_username: str) -> str:
    author_id = _coerce_string(tweet.get("author_id")).strip()
    user = _included_users_by_id(includes).get(author_id, {})
    username = _coerce_string(user.get("username")).strip() if isinstance(user, dict) else ""
    return username or fallback_username


def build_x_api_conversation_snapshot_payload(
    tweet: dict,
    includes: dict,
    *,
    username: str,
    source: str,
    search_mode: str,
    conversation_id: str,
) -> dict:
    payload = {
        "data": tweet,
        "includes": includes if isinstance(includes, dict) else {},
    }
    cache = _get_x_api_json_cache(payload, create=True)
    cache["source"] = "x_api_conversation_search"
    cache["source_auth"] = source
    cache["conversation_search"] = search_mode
    cache["conversation_id"] = conversation_id
    cache["archive_user"] = username
    cache["fetched_at"] = _utc_now_iso()
    return payload


def write_x_api_conversation_snapshot_records(
    username: str,
    conversation_payloads: List[dict],
    json_dir: Path,
    *,
    resume: bool = True,
    source: str = "",
    search_mode: str = "all",
    existing_records_by_id: Optional[Dict[str, SnapshotRecord]] = None,
) -> Tuple[List[SnapshotRecord], int, int]:
    records: List[SnapshotRecord] = []
    written_count = 0
    reused_count = 0
    seen_tweet_ids = set()
    existing_records_by_id = existing_records_by_id or {}
    json_dir.mkdir(parents=True, exist_ok=True)

    for page_payload in conversation_payloads:
        if not isinstance(page_payload, dict):
            continue
        includes = page_payload.get("includes", {})
        tweets = page_payload.get("data", [])
        if isinstance(tweets, dict):
            tweets = [tweets]
        if not isinstance(tweets, list):
            continue
        for tweet in tweets:
            if not isinstance(tweet, dict):
                continue
            tweet_id = _coerce_string(tweet.get("id")).strip()
            if not tweet_id or tweet_id in seen_tweet_ids:
                continue
            seen_tweet_ids.add(tweet_id)

            existing_record = existing_records_by_id.get(tweet_id)
            if resume and existing_record is not None and existing_record.json_path.exists():
                records.append(existing_record)
                reused_count += 1
                continue

            timestamp = parse_x_api_created_at_timestamp(_coerce_string(tweet.get("created_at")))
            tweet_username = _username_for_x_api_tweet(tweet, includes, username)
            original_url = reconstruct_original_url(tweet_username, tweet_id)
            json_path = build_json_output_path(json_dir, timestamp, original_url, {"tweet_id": tweet_id})
            record = SnapshotRecord(timestamp=timestamp, original_url=original_url, json_path=json_path)
            records.append(record)
            if resume and json_path.exists() and load_snapshot_json(json_path) is not None:
                reused_count += 1
                continue
            snapshot_payload = build_x_api_conversation_snapshot_payload(
                tweet,
                includes,
                username=username,
                source=source,
                search_mode=search_mode,
                conversation_id=_coerce_string(tweet.get("conversation_id")).strip(),
            )
            write_snapshot_json_payload(json_path, snapshot_payload)
            written_count += 1

    return sorted(records, key=lambda record: (record.timestamp, record.original_url, record.json_path.name)), written_count, reused_count


def collect_root_conversation_ids(records: List[SnapshotRecord]) -> List[str]:
    conversation_ids = set()
    for record in records:
        if not record.json_path.exists():
            continue
        data = load_snapshot_json(record.json_path)
        if data is None:
            continue
        metadata = extract_tweet_metadata(data, record.original_url)
        tweet_id = metadata.get("tweet_id", "")
        if not tweet_id or metadata.get("is_repost") == "true":
            continue
        conversation_id = metadata.get("conversation_id") or tweet_id
        if metadata.get("is_reply") == "true" and conversation_id != tweet_id:
            continue
        conversation_ids.add(conversation_id)
    return sorted(conversation_ids, key=int)


def snapshot_records_by_tweet_id(records: List[SnapshotRecord]) -> Dict[str, SnapshotRecord]:
    indexed: Dict[str, SnapshotRecord] = {}
    for record in records:
        tweet_id = snapshot_record_tweet_id(record)
        if tweet_id and tweet_id not in indexed:
            indexed[tweet_id] = record
    return indexed


def repair_conversations_from_snapshot_records(
    records: List[SnapshotRecord],
    username: str,
    asset_dir: Path,
    auth_value,
    *,
    search_mode: str = "all",
    pages: int = 0,
    page_size: int = DEFAULT_X_API_TIMELINE_PAGE_SIZE,
    resume: bool = True,
    access_index: Optional[dict] = None,
) -> Tuple[int, int, int, int, List[SnapshotRecord]]:
    json_dir = asset_dir / "json"
    conversation_ids = collect_root_conversation_ids(records)
    existing_by_id = snapshot_records_by_tweet_id(records)
    conversation_records: List[SnapshotRecord] = []
    written_count = 0
    reused_count = 0
    failed_count = 0

    for index, conversation_id in enumerate(conversation_ids, 1):
        payloads, error, source = fetch_x_api_conversation_payloads_with_auths(
            conversation_id,
            auth_value,
            search_mode=search_mode,
            pages=pages,
            page_size=page_size,
            access_index=access_index,
        )
        if error and not payloads:
            failed_count += 1
            print(
                f"Conversation repair progress: {index}/{len(conversation_ids)} conversations, "
                f"{written_count} JSON written, {failed_count} failed; {conversation_id}: {error}",
                flush=True,
            )
            continue
        records_for_conversation, written, reused = write_x_api_conversation_snapshot_records(
            username,
            payloads,
            json_dir,
            resume=resume,
            source=source,
            search_mode=search_mode,
            existing_records_by_id=existing_by_id,
        )
        for record in records_for_conversation:
            tweet_id = snapshot_record_tweet_id(record)
            if tweet_id and tweet_id not in existing_by_id:
                existing_by_id[tweet_id] = record
        conversation_records.extend(records_for_conversation)
        written_count += written
        reused_count += reused
        if index % 25 == 0 or index == len(conversation_ids):
            print(
                f"Conversation repair progress: {index}/{len(conversation_ids)} conversations, "
                f"{written_count} JSON written, {reused_count} reused, {failed_count} failed",
                flush=True,
            )

    return len(conversation_ids), written_count, reused_count, failed_count, conversation_records


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
        if not _is_extractable_tweet_node(node):
            continue
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
        if not _is_extractable_tweet_node(node):
            continue
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
    referenced_reply_id = ""
    for item in node.get("referenced_tweets", []):
        if not isinstance(item, dict) or item.get("type") != "replied_to":
            continue
        referenced_reply_id = _coerce_string(item.get("id")).strip()
        if referenced_reply_id:
            break
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
        or referenced_reply_id
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

    for key, value in node.items():
        if key == X_API_JSON_CACHE_KEY:
            continue
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
        if not _is_extractable_tweet_node(node):
            continue
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


def _find_reply_target_author_from_text(node: dict) -> str:
    text = _extract_tweet_text_from_node(node)
    match = re.match(r"\s*@([A-Za-z0-9_]{1,15})\b", text)
    return match.group(1) if match else ""


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
            author = _find_reply_target_author_from_text(primary_node)
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
        tweet_id=tweet_id,
        unavailable_reason=unavailable_reason or _find_reference_error(data, tweet_id),
    )


def _build_referenced_tweet_info_from_payload(payload: dict, tweet_id: str, kind: str) -> Optional[ReferencedTweetInfo]:
    original_url = f"https://twitter.com/i/status/{tweet_id}"
    primary_node = find_tweet_node_by_id(payload, tweet_id)
    if primary_node is None:
        return None

    text, media_items, _, _ = extract_tweet_content(
        payload,
        original_url,
        x_api_use_json_cache=False,
        allow_reply_chain_media_lookup=False,
    )
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
        tweet_id=rendered_tweet_id,
        conversation_id=metadata.get("conversation_id", ""),
        in_reply_to_status_id=metadata.get("in_reply_to_status_id", ""),
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
    kind: str = "replied_to",
) -> Tuple[List[ReferencedTweetInfo], int, str, str]:
    ref_tweets: List[ReferencedTweetInfo] = []
    seen_ids = set()
    stored_depth = 0
    terminal_error_id = ""
    terminal_error_message = ""
    terminal_error_consumed = False

    def append_entry(source_data: dict, current_id: str) -> bool:
        nonlocal stored_depth, terminal_error_id, terminal_error_message, terminal_error_consumed
        if len(ref_tweets) >= max_depth:
            return False

        entry = _get_cached_x_api_reply_chain_entry(source_data, current_id)
        if not entry:
            return False

        posts = entry.get("posts", [])
        if isinstance(posts, list):
            stored_depth = max(stored_depth, len(posts))
        else:
            posts = []
        try:
            stored_depth = max(stored_depth, int(entry.get("max_depth", 0)))
        except (TypeError, ValueError):
            pass

        terminal_error = entry.get("terminal_error", {})
        if isinstance(terminal_error, dict) and not terminal_error_message:
            terminal_error_id = _coerce_string(terminal_error.get("id")).strip()
            terminal_error_message = _coerce_string(terminal_error.get("message")).strip()

        recovery = entry.get("recovery", {})
        recovery_method = _coerce_string(recovery.get("method")).strip() if isinstance(recovery, dict) else ""
        recovery_missing_id = _coerce_string(recovery.get("missing_id")).strip() if isinstance(recovery, dict) else ""
        prepend_terminal_error = (
            recovery_method == "conversation_id_timestamp"
            and bool(terminal_error_message)
            and (terminal_error_id or current_id) == (recovery_missing_id or current_id)
            and (terminal_error_id or current_id) == current_id
            and current_id not in seen_ids
        )
        if prepend_terminal_error and len(ref_tweets) < max_depth:
            seen_ids.add(current_id)
            ref_tweets.append(
                ReferencedTweetInfo(
                    kind=kind,
                    text="",
                    author="",
                    display_name="",
                    created_at="",
                    url=f"https://twitter.com/i/status/{current_id}",
                    tweet_id=current_id,
                    unavailable_reason=terminal_error_message,
                )
            )
            terminal_error_consumed = True

        appended = False
        for item in posts:
            if len(ref_tweets) >= max_depth:
                break
            if not isinstance(item, dict):
                continue
            payload = item.get("payload")
            if not isinstance(payload, dict):
                continue
            cached_id = _coerce_string(item.get("id")).strip() or current_id
            tweet_info = _build_referenced_tweet_info_from_payload(payload, cached_id, kind)
            if tweet_info is None:
                continue

            rendered_id = tweet_info.tweet_id or cached_id
            if rendered_id in seen_ids:
                continue
            seen_ids.add(rendered_id)
            ref_tweets.append(tweet_info)
            appended = True

            parent_id = tweet_info.in_reply_to_status_id
            if parent_id and len(ref_tweets) < max_depth:
                append_entry(payload, parent_id)

        return appended

    if not append_entry(data, tweet_id) and stored_depth == 0 and not terminal_error_message:
        return [], 0, "", ""

    if terminal_error_consumed:
        terminal_error_id = ""
        terminal_error_message = ""

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
    recovery_method: str = "",
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
    if recovery_method:
        entry["recovery"] = {
            "method": recovery_method,
            "missing_id": terminal_error_id or tweet_id,
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


def _parse_tweet_created_at(value: str) -> Optional[datetime]:
    raw = _coerce_string(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    except ValueError:
        pass
    try:
        return datetime.strptime(raw, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc).replace(tzinfo=None)
    except ValueError:
        return None


def _tweet_node_created_at(node: dict) -> str:
    if not isinstance(node, dict):
        return ""
    legacy = node.get("legacy") if isinstance(node.get("legacy"), dict) else {}
    return _coerce_string(node.get("created_at") or legacy.get("created_at")).strip()


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


def iter_local_snapshot_payloads_for_tweet(
    tweet_id: str,
    json_dir: Optional[Path],
    snapshot_timestamp: str = "",
) -> List[dict]:
    if not tweet_id or json_dir is None or not json_dir.exists():
        return []

    index = build_local_tweet_snapshot_index(json_dir)
    candidates = list(index.get(tweet_id, []))
    candidates.sort(key=lambda item: (_timestamp_distance(item[0], snapshot_timestamp), item[0], item[1].name))
    payloads: List[dict] = []
    for _, json_path in candidates:
        payload = load_snapshot_json(json_path)
        if _payload_contains_tweet(payload, tweet_id):
            payloads.append(payload)
    return payloads


def find_local_snapshot_payload_for_tweet(
    tweet_id: str,
    json_dir: Optional[Path],
    snapshot_timestamp: str = "",
) -> Optional[dict]:
    for payload in iter_local_snapshot_payloads_for_tweet(tweet_id, json_dir, snapshot_timestamp):
        return payload
    return None


def build_local_conversation_snapshot_index(json_dir: Optional[Path]) -> Dict[str, List[dict]]:
    if json_dir is None or not json_dir.exists():
        return {}
    cache_key = str(json_dir.resolve())
    with LOCAL_TWEET_INDEX_LOCK:
        cached = LOCAL_CONVERSATION_INDEX_CACHE.get(cache_key)
        if cached is not None:
            return cached

        index: Dict[str, List[dict]] = {}
        seen = set()
        for json_path in json_dir.glob("*.json"):
            parsed = parse_snapshot_filename(json_path.name)
            snapshot_timestamp = parsed[0] if parsed else ""
            payload = load_snapshot_json(json_path)
            if not isinstance(payload, dict):
                continue

            for node in _iter_candidate_tweet_nodes(payload):
                if not _is_extractable_tweet_node(node):
                    continue
                metadata = _extract_metadata_from_node(node)
                tweet_id = metadata.get("tweet_id", "")
                conversation_id = metadata.get("conversation_id", "")
                if not tweet_id or not conversation_id:
                    continue
                dedupe_key = (conversation_id, tweet_id, json_path)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                created_at = _tweet_node_created_at(node)
                created_dt = _parse_tweet_created_at(created_at) or _parse_archive_timestamp(snapshot_timestamp) or datetime.min
                index.setdefault(conversation_id, []).append(
                    {
                        "tweet_id": tweet_id,
                        "conversation_id": conversation_id,
                        "in_reply_to_status_id": metadata.get("in_reply_to_status_id", ""),
                        "created_at": created_at,
                        "created_dt": created_dt,
                        "snapshot_timestamp": snapshot_timestamp,
                        "json_path": json_path,
                    }
                )

        for entries in index.values():
            entries.sort(key=lambda item: (item["created_dt"], item["snapshot_timestamp"], item["tweet_id"]))
        LOCAL_CONVERSATION_INDEX_CACHE[cache_key] = index
        return index


def find_local_conversation_payloads_before(
    conversation_id: str,
    before_created_at: str,
    json_dir: Optional[Path],
    snapshot_timestamp: str = "",
    *,
    exclude_ids: Optional[set] = None,
    limit: int = 0,
) -> List[Tuple[str, dict, str]]:
    if not conversation_id or json_dir is None or not json_dir.exists() or limit <= 0:
        return []

    before_dt = _parse_tweet_created_at(before_created_at) or _parse_archive_timestamp(snapshot_timestamp)
    exclude_ids = exclude_ids or set()
    candidates = []
    for entry in build_local_conversation_snapshot_index(json_dir).get(conversation_id, []):
        tweet_id = entry.get("tweet_id", "")
        if not tweet_id or tweet_id in exclude_ids:
            continue
        created_dt = entry.get("created_dt")
        if before_dt is not None and isinstance(created_dt, datetime) and created_dt >= before_dt:
            continue
        candidates.append(entry)

    candidates.sort(
        key=lambda item: (
            item.get("created_dt") if isinstance(item.get("created_dt"), datetime) else datetime.min,
            item.get("snapshot_timestamp", ""),
            item.get("tweet_id", ""),
        ),
        reverse=True,
    )

    payloads: List[Tuple[str, dict, str]] = []
    seen_ids = set()
    for entry in candidates:
        tweet_id = entry.get("tweet_id", "")
        if not tweet_id or tweet_id in seen_ids:
            continue
        payload = load_snapshot_json(entry.get("json_path"))
        if not _payload_contains_tweet(payload, tweet_id):
            continue
        seen_ids.add(tweet_id)
        payloads.append((tweet_id, payload, "conversation_recovery:local_json"))
        if len(payloads) >= limit:
            break
    return payloads


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
    return replace(base, author_hint=author_hint or base.author_hint)


def _with_reply_chain_recovery_hints(
    context: Optional[ReplyChainLookupContext],
    conversation_id: str = "",
    before_created_at: str = "",
) -> ReplyChainLookupContext:
    base = context or ReplyChainLookupContext()
    return replace(
        base,
        conversation_id_hint=conversation_id or base.conversation_id_hint,
        before_created_at_hint=before_created_at or base.before_created_at_hint,
    )


def fetch_reply_chain_payload(
    tweet_id: str,
    auth_value,
    lookup_context: Optional[ReplyChainLookupContext],
    author_hint: str = "",
    access_index: Optional[dict] = None,
) -> Tuple[Optional[dict], str, str]:
    context = lookup_context or ReplyChainLookupContext()
    effective_author_hint = author_hint or context.author_hint

    if context.use_local_json:
        payload = find_local_snapshot_payload_for_tweet(tweet_id, context.json_dir, context.snapshot_timestamp)
        if payload is not None:
            return payload, "", "local_json"

    x_api_error = ""
    if normalize_x_api_auths(auth_value):
        payload, x_api_error, source = fetch_x_api_tweet_payload_with_auths(
            auth_value,
            tweet_id,
            access_index=access_index,
            author_hint=effective_author_hint,
        )
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


def recover_reply_chain_payloads_by_conversation(
    missing_tweet_id: str,
    auth_value,
    lookup_context: Optional[ReplyChainLookupContext],
    remaining_depth: int,
    *,
    seen_ids: Optional[set] = None,
    access_index: Optional[dict] = None,
) -> List[Tuple[str, dict, str]]:
    context = lookup_context or ReplyChainLookupContext()
    conversation_id = _coerce_string(context.conversation_id_hint).strip()
    if not conversation_id or remaining_depth <= 0:
        return []

    seen_ids = seen_ids or set()
    excluded_ids = set(seen_ids)
    excluded_ids.add(missing_tweet_id)
    payloads: List[Tuple[str, dict, str]] = []
    payload_ids = set()

    def append_payload(tweet_id: str, payload: Optional[dict], source: str) -> None:
        if len(payloads) >= remaining_depth:
            return
        tweet_id = _coerce_string(tweet_id).strip()
        if not tweet_id or tweet_id in payload_ids:
            return
        if tweet_id in seen_ids and tweet_id != missing_tweet_id:
            return
        if not isinstance(payload, dict) or not _payload_contains_tweet(payload, tweet_id):
            return
        payload_ids.add(tweet_id)
        payloads.append((tweet_id, payload, source or "conversation_recovery"))

    for tweet_id, payload, source in find_local_conversation_payloads_before(
        conversation_id,
        context.before_created_at_hint,
        context.json_dir,
        context.snapshot_timestamp,
        exclude_ids=excluded_ids,
        limit=remaining_depth,
    ):
        append_payload(tweet_id, payload, source)

    if conversation_id not in payload_ids and conversation_id not in seen_ids and len(payloads) < remaining_depth:
        # A conversation_id is itself the root post id. Even when the normal
        # parent lookup has Wayback disabled, this root lookup is a single,
        # explicit id recovered from conversation metadata, so allow the
        # archival fallback for it.
        root_context = replace(
            context,
            use_wayback=context.allow_conversation_wayback,
            author_hint=context.author_hint,
        )
        root_payload, _, root_source = fetch_reply_chain_payload(
            conversation_id,
            auth_value,
            root_context,
            context.author_hint,
            access_index=access_index,
        )
        append_payload(conversation_id, root_payload, f"conversation_recovery:{root_source}" if root_source else "conversation_recovery")

    payloads.sort(
        key=lambda item: (
            _parse_tweet_created_at(
                _tweet_node_created_at(find_tweet_node_by_id(item[1], item[0]) or {})
            )
            or datetime.min,
            item[0],
        ),
        reverse=True,
    )
    return payloads[:remaining_depth]


def fetch_x_api_reply_chain(
    tweet_id: str,
    bearer_token: str,
    max_depth: int,
    payloads_out: Optional[list] = None,
    terminal_errors_out: Optional[List[Tuple[str, str]]] = None,
    lookup_context: Optional[ReplyChainLookupContext] = None,
    x_api_access_index: Optional[dict] = None,
) -> Tuple[List[ReferencedTweetInfo], str]:
    ref_tweets: List[ReferencedTweetInfo] = []
    current_id = tweet_id
    base_lookup_context = lookup_context or ReplyChainLookupContext()
    current_author_hint = base_lookup_context.author_hint
    current_conversation_id = base_lookup_context.conversation_id_hint
    current_before_created_at = base_lookup_context.before_created_at_hint
    seen = set()

    for depth_index in range(max_depth):
        if current_id in seen:
            break
        seen.add(current_id)
        current_lookup_context = replace(
            base_lookup_context,
            author_hint=current_author_hint or base_lookup_context.author_hint,
            conversation_id_hint=current_conversation_id or base_lookup_context.conversation_id_hint,
            before_created_at_hint=current_before_created_at or base_lookup_context.before_created_at_hint,
        )

        payload, error, source = fetch_reply_chain_payload(
            current_id,
            bearer_token,
            current_lookup_context,
            current_author_hint,
            access_index=x_api_access_index,
        )
        if payload is None:
            recovered_payloads = recover_reply_chain_payloads_by_conversation(
                current_id,
                bearer_token,
                current_lookup_context,
                max_depth - len(ref_tweets),
                seen_ids=seen,
                access_index=x_api_access_index,
            )
            if recovered_payloads:
                recovered_current = any(recovered_id == current_id for recovered_id, _, _ in recovered_payloads)
                if not recovered_current and len(ref_tweets) < max_depth:
                    ref_tweets.append(
                        ReferencedTweetInfo(
                            kind="replied_to",
                            text="",
                            author="",
                            display_name="",
                            created_at="",
                            url=f"https://twitter.com/i/status/{current_id}",
                            tweet_id=current_id,
                            unavailable_reason=error,
                        )
                    )
                if terminal_errors_out is not None and error and not recovered_current:
                    terminal_errors_out.append((current_id, error))
                for recovered_id, recovered_payload, recovered_source in recovered_payloads:
                    if len(ref_tweets) >= max_depth:
                        break
                    if payloads_out is not None:
                        payloads_out.append((recovered_id, recovered_payload, recovered_source))
                    recovered_info = _build_referenced_tweet_info_from_payload(
                        recovered_payload,
                        recovered_id,
                        "replied_to",
                    )
                    if recovered_info is not None:
                        ref_tweets.append(recovered_info)
                if ref_tweets:
                    return ref_tweets, ""
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
                        tweet_id=current_id,
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
                        tweet_id=current_id,
                        unavailable_reason=error,
                    )
                )
                return ref_tweets, ""
            return [], error
        ref_tweets.append(tweet_info)

        primary_node = find_primary_tweet_node(payload, f"https://twitter.com/i/status/{current_id}")
        if primary_node is None:
            break
        current_conversation_id = tweet_info.conversation_id or current_conversation_id
        current_before_created_at = tweet_info.created_at or current_before_created_at
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
    allow_reply_chain_media_lookup: bool = True,
    x_api_access_index: Optional[dict] = None,
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

    def infer_media_kind_from_key(media_key: str) -> str:
        prefix = media_key.split("_", 1)[0]
        return "video" if prefix in {"7", "13", "16"} else "image"

    def attachment_media_keys_from_node(node: dict) -> List[str]:
        keys: List[str] = []
        seen = set()
        containers = [node]
        legacy = node.get("legacy")
        if isinstance(legacy, dict):
            containers.append(legacy)
        for container in containers:
            attachments = container.get("attachments", {})
            media_keys = attachments.get("media_keys", []) if isinstance(attachments, dict) else []
            for media_key in media_keys:
                media_key = _coerce_string(media_key).strip()
                if media_key and media_key not in seen:
                    keys.append(media_key)
                    seen.add(media_key)
        return keys

    def unresolved_attachment_media_kinds(node: dict) -> List[str]:
        kinds: List[str] = []
        for media_key in attachment_media_keys_from_node(node):
            media = media_by_key.get(media_key)
            if not isinstance(media, dict):
                kinds.append(infer_media_kind_from_key(media_key))
                continue
            media_type = _coerce_string(media.get("type")).strip()
            if media_type == "photo":
                if not _coerce_string(media.get("url")).strip():
                    kinds.append("image")
            elif media_type in {"video", "animated_gif"}:
                if not select_best_video_variant(media) and not _coerce_string(media.get("preview_image_url")).strip():
                    kinds.append("video")
            else:
                kinds.append(infer_media_kind_from_key(media_key))
        return kinds

    def fetch_ref_info_for_unresolved_media(
        ref_id: str,
        context_node: dict,
        ref_type: str,
    ) -> Optional[ReferencedTweetInfo]:
        if not ref_id:
            return None

        cached_error = ""
        if x_api_use_json_cache:
            cached_refs, _, _, cached_error = _load_cached_x_api_reply_chain(data, ref_id, 1, ref_type)
            for cached_ref in cached_refs:
                if cached_ref.media:
                    return cached_ref

        def ref_info_from_payload(payload: Optional[dict], source: str) -> Optional[ReferencedTweetInfo]:
            if payload is None:
                return None
            enriched_ref = _build_referenced_tweet_info_from_payload(payload, ref_id, ref_type)
            if enriched_ref is None or not any(media.url for media in enriched_ref.media):
                return None
            if _store_x_api_reply_chain(data, ref_id, 1, [(ref_id, payload, source or "media_lookup")]):
                if x_api_enrichment_changed is not None:
                    x_api_enrichment_changed.append(True)
            return enriched_ref

        author_hint, _ = _resolve_referenced_tweet_author_info(data, context_node, ref_type, ref_id)
        lookup_context = _with_reply_chain_author_hint(reply_chain_lookup_context, author_hint)
        terminal_error = ""
        if lookup_context.use_local_json:
            for payload in iter_local_snapshot_payloads_for_tweet(
                ref_id,
                lookup_context.json_dir,
                lookup_context.snapshot_timestamp,
            ):
                enriched_ref = ref_info_from_payload(payload, "local_json")
                if enriched_ref is not None:
                    return enriched_ref

        if cached_error and not x_api_retry_cached_errors:
            return None

        if not allow_reply_chain_media_lookup:
            return None

        if normalize_x_api_auths(x_bearer_token):
            payload, x_api_error, source = fetch_x_api_tweet_payload_with_auths(
                x_bearer_token,
                ref_id,
                access_index=x_api_access_index,
                author_hint=lookup_context.author_hint,
            )
            enriched_ref = ref_info_from_payload(payload, source)
            if enriched_ref is not None:
                return enriched_ref
            terminal_error = x_api_error

        if lookup_context.use_wayback:
            payload, wayback_error, source = fetch_wayback_tweet_payload(
                ref_id,
                lookup_context.author_hint,
                lookup_context.snapshot_timestamp,
            )
            enriched_ref = ref_info_from_payload(payload, source)
            if enriched_ref is not None:
                return enriched_ref
            if wayback_error:
                terminal_error = (
                    f"{terminal_error} Wayback fallback: {wayback_error}"
                    if terminal_error
                    else wayback_error
                )
        if terminal_error and _store_x_api_reply_chain(data, ref_id, 1, [], ref_id, terminal_error):
            if x_api_enrichment_changed is not None:
                x_api_enrichment_changed.append(True)
        return None

    quote_tco_urls: set = set()
    primary_node = find_primary_tweet_node(data, original_url)
    if primary_node is not None:
        quote_tco_urls = _find_quote_tco_urls(primary_node)
        collect_text_from_node(primary_node)
        collect_media_from_node(primary_node)
        for unresolved_kind in unresolved_attachment_media_kinds(primary_node):
            media_items.append(TweetMediaItem(kind=unresolved_kind, url=""))
        for ref_type in ("replied_to", "quoted"):
            finder = find_replied_to_tweet_nodes if ref_type == "replied_to" else find_quoted_tweet_nodes
            referenced_ids = _get_referenced_tweet_ids(primary_node, ref_type)
            rendered_ref_ids = set()

            def append_x_api_reply_chain(ref_id: str, context_node: dict, max_depth: int) -> bool:
                if not ref_id or max_depth <= 0:
                    return False

                author_hint, _ = _resolve_referenced_tweet_author_info(data, context_node, ref_type, ref_id)
                lookup_context = _with_reply_chain_author_hint(reply_chain_lookup_context, author_hint)
                context_metadata = _extract_metadata_from_node(context_node)
                lookup_context = _with_reply_chain_recovery_hints(
                    lookup_context,
                    context_metadata.get("conversation_id", ""),
                    _tweet_node_created_at(context_node),
                )
                has_reply_chain_lookup = (
                    bool(x_bearer_token)
                    or (lookup_context.use_local_json and lookup_context.json_dir is not None)
                    or lookup_context.use_wayback
                    or (lookup_context.allow_conversation_wayback and bool(lookup_context.conversation_id_hint))
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
                                        tweet_id=cached_error_id or ref_id,
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
                    x_api_access_index=x_api_access_index,
                )
                terminal_error_id = terminal_errors[0][0] if terminal_errors else ""
                terminal_error = terminal_errors[0][1] if terminal_errors else x_api_error
                if payloads or terminal_error:
                    recovered_after_gap = bool(
                        terminal_error
                        and terminal_error_id
                        and any(
                            isinstance(item, tuple)
                            and len(item) >= 3
                            and _coerce_string(item[2]).startswith("conversation_recovery:")
                            for item in payloads
                        )
                    )
                    if _store_x_api_reply_chain(
                        data,
                        ref_id,
                        max_depth,
                        payloads,
                        terminal_error_id,
                        terminal_error,
                        recovery_method="conversation_id_timestamp" if recovered_after_gap else "",
                    ):
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
                rt_unresolved_media_kinds = unresolved_attachment_media_kinds(ref_node)
                rt_text = _extract_tweet_text_from_node(ref_node)
                rt_author, rt_display_name = _extract_user_info(data, ref_node)
                rt_meta = _extract_metadata_from_node(ref_node)
                rt_id = rt_meta.get("tweet_id", "")
                rt_url = f"https://twitter.com/{rt_author}/status/{rt_id}" if rt_author and rt_id else ""
                rt_created = _coerce_string(ref_node.get("created_at"))
                if rt_unresolved_media_kinds:
                    enriched_ref = fetch_ref_info_for_unresolved_media(rt_id, ref_node, ref_type)
                    if enriched_ref is not None:
                        rt_text = enriched_ref.text or rt_text
                        rt_author = enriched_ref.author or rt_author
                        rt_display_name = enriched_ref.display_name or rt_display_name
                        rt_created = enriched_ref.created_at or rt_created
                        rt_url = enriched_ref.url or rt_url
                        rt_id = enriched_ref.tweet_id or rt_id
                        rt_media = list(enriched_ref.media)
                        rt_meta = {
                            **rt_meta,
                            "conversation_id": enriched_ref.conversation_id or rt_meta.get("conversation_id", ""),
                            "in_reply_to_status_id": enriched_ref.in_reply_to_status_id or rt_meta.get("in_reply_to_status_id", ""),
                        }
                    else:
                        for unresolved_kind in rt_unresolved_media_kinds:
                            rt_media.append(TweetMediaItem(kind=unresolved_kind, url=""))
                if rt_text or rt_media:
                    if rt_id:
                        rendered_ref_ids.add(rt_id)
                    ref_tweets.append(ReferencedTweetInfo(
                        kind=ref_type, text=rt_text, author=rt_author,
                        display_name=rt_display_name, created_at=rt_created,
                        url=rt_url,
                        tweet_id=rt_id,
                        conversation_id=rt_meta.get("conversation_id", ""),
                        in_reply_to_status_id=rt_meta.get("in_reply_to_status_id", ""),
                        media=tuple(rt_media),
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
        if dedupe_key not in seen:
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
    retry_forbidden_media: bool = False,
    media_fetch_policy: Optional[MediaFetchPolicy] = None,
) -> str:
    json_status = ensure_snapshot_json(record, resume=resume, failure_statuses=failure_statuses)
    if json_status == "missing":
        return json_status
    if asset_dir_name and media_dir is not None:
        prefetch_snapshot_media(
            record,
            asset_dir_name,
            media_dir,
            image_cache,
            negative_cache,
            retry_forbidden_media=retry_forbidden_media,
            media_fetch_policy=media_fetch_policy,
        )
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


def is_negative_media_status(status_code: str) -> bool:
    return status_code in {"403", "404", "410"}


def negative_media_reason_for_status(status_code: str) -> str:
    if status_code == "403":
        return "forbidden"
    return "hard-missing"


def clear_negative_media_cache_entry(media_url: str, media_dir: Path, negative_cache: Optional[Dict[str, dict]]) -> None:
    if negative_cache is None:
        return
    removed = False
    with IMAGE_CACHE_LOCK:
        if media_url in negative_cache:
            negative_cache.pop(media_url, None)
            removed = True
    if removed:
        replace_negative_media_cache(media_dir.parent, negative_cache)


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
    retry_forbidden_media: bool = False,
    allow_wayback_fallback: bool = True,
    allow_closest_cdx_lookup: bool = True,
) -> str:
    """
    Download an image once and return its relative asset path.
    """
    removed_forbidden_negative = False
    with IMAGE_CACHE_LOCK:
        if cache is not None and image_url in cache:
            cached_relative_path = cache[image_url]
            if (media_dir / Path(cached_relative_path).name).exists():
                return cached_relative_path
            cache.pop(image_url, None)
        if negative_cache is not None and image_url in negative_cache:
            negative_status = get_negative_media_status(negative_cache, image_url)
            if retry_forbidden_media and negative_status == "403":
                negative_cache.pop(image_url, None)
                removed_forbidden_negative = True
            else:
                if failure_statuses is not None:
                    failure_statuses[image_url] = negative_status or "unknown"
                return ""
    if removed_forbidden_negative:
        replace_negative_media_cache(media_dir.parent, negative_cache)
    if not download_missing:
        return ""

    candidate_urls = build_image_candidate_urls(
        image_url,
        snapshot_timestamp,
        allow_wayback_fallback=allow_wayback_fallback,
    )
    resp = None
    saw_archived_transient_error = False
    saw_forbidden_error = False
    tried_archived_candidate = False
    latest_http_status = ""
    for candidate_url in candidate_urls:
        try:
            resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
            break
        except Exception as exc:
            status_code = extract_http_status_code(exc)
            latest_http_status = status_code or latest_http_status
            if is_wayback_candidate_url(candidate_url):
                tried_archived_candidate = True
                if not is_negative_media_status(status_code):
                    saw_archived_transient_error = True
            if status_code == "403":
                saw_forbidden_error = True
            resp = None
    closest_capture_candidates = []
    if resp is None and allow_wayback_fallback and allow_closest_cdx_lookup:
        closest_capture_candidates = build_closest_capture_candidate_urls(image_url, snapshot_timestamp)
        for candidate_url in closest_capture_candidates:
            try:
                resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
                break
            except Exception as exc:
                status_code = extract_http_status_code(exc)
                latest_http_status = status_code or latest_http_status
                if is_wayback_candidate_url(candidate_url):
                    tried_archived_candidate = True
                    if not is_negative_media_status(status_code):
                        saw_archived_transient_error = True
                if status_code == "403":
                    saw_forbidden_error = True
                resp = None
    if resp is None:
        if saw_forbidden_error:
            latest_http_status = "403"
            if failure_statuses is not None:
                failure_statuses[image_url] = latest_http_status
            mark_negative_media_cache_entry(
                image_url,
                media_dir,
                negative_cache,
                status=latest_http_status,
                reason=negative_media_reason_for_status(latest_http_status),
            )
            return ""
        if (
            allow_closest_cdx_lookup
            and not latest_http_status
            and tried_archived_candidate
            and (not closest_capture_candidates or media_cdx_has_any_capture(image_url) is False)
        ):
            latest_http_status = "404"
            mark_negative_media_cache_entry(image_url, media_dir, negative_cache, status=latest_http_status, reason="no-capture")
        if failure_statuses is not None:
            failure_statuses[image_url] = latest_http_status or "unknown"
        if tried_archived_candidate and not saw_archived_transient_error:
            negative_status = latest_http_status or "404"
            mark_negative_media_cache_entry(
                image_url,
                media_dir,
                negative_cache,
                status=negative_status,
                reason=negative_media_reason_for_status(negative_status),
            )
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
    retry_forbidden_media: bool = False,
    allow_wayback_fallback: bool = True,
    allow_closest_cdx_lookup: bool = True,
) -> str:
    """
    Download one archived video variant and return its relative asset path.
    """
    removed_forbidden_negative = False
    with IMAGE_CACHE_LOCK:
        if cache is not None and video_url in cache:
            cached_relative_path = cache[video_url]
            if (media_dir / Path(cached_relative_path).name).exists():
                return cached_relative_path
            cache.pop(video_url, None)
        if negative_cache is not None and video_url in negative_cache:
            negative_status = get_negative_media_status(negative_cache, video_url)
            if retry_forbidden_media and negative_status == "403":
                negative_cache.pop(video_url, None)
                removed_forbidden_negative = True
            else:
                if failure_statuses is not None:
                    failure_statuses[video_url] = negative_status or "unknown"
                return ""
    if removed_forbidden_negative:
        replace_negative_media_cache(media_dir.parent, negative_cache)
    if not download_missing:
        return ""

    candidate_urls = build_video_candidate_urls(
        video_url,
        snapshot_timestamp,
        allow_wayback_fallback=allow_wayback_fallback,
    )
    resp = None
    saw_archived_transient_error = False
    saw_forbidden_error = False
    tried_archived_candidate = False
    latest_http_status = ""
    for candidate_url in candidate_urls:
        try:
            resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
            break
        except Exception as exc:
            status_code = extract_http_status_code(exc)
            latest_http_status = status_code or latest_http_status
            if is_wayback_candidate_url(candidate_url):
                tried_archived_candidate = True
                if not is_negative_media_status(status_code):
                    saw_archived_transient_error = True
            if status_code == "403":
                saw_forbidden_error = True
            resp = None
    closest_capture_candidates = []
    if resp is None and allow_wayback_fallback and allow_closest_cdx_lookup:
        closest_capture_candidates = build_closest_capture_candidate_urls(video_url, snapshot_timestamp)
        for candidate_url in closest_capture_candidates:
            try:
                resp = get_with_retry(candidate_url, timeout=IMAGE_TIMEOUT)
                break
            except Exception as exc:
                status_code = extract_http_status_code(exc)
                latest_http_status = status_code or latest_http_status
                if is_wayback_candidate_url(candidate_url):
                    tried_archived_candidate = True
                    if not is_negative_media_status(status_code):
                        saw_archived_transient_error = True
                if status_code == "403":
                    saw_forbidden_error = True
                resp = None
    if resp is None:
        if saw_forbidden_error:
            latest_http_status = "403"
            if failure_statuses is not None:
                failure_statuses[video_url] = latest_http_status
            mark_negative_media_cache_entry(
                video_url,
                media_dir,
                negative_cache,
                status=latest_http_status,
                reason=negative_media_reason_for_status(latest_http_status),
            )
            return ""
        if (
            allow_closest_cdx_lookup
            and not latest_http_status
            and tried_archived_candidate
            and (not closest_capture_candidates or media_cdx_has_any_capture(video_url) is False)
        ):
            latest_http_status = "404"
            mark_negative_media_cache_entry(video_url, media_dir, negative_cache, status=latest_http_status, reason="no-capture")
        if failure_statuses is not None:
            failure_statuses[video_url] = latest_http_status or "unknown"
        if tried_archived_candidate and not saw_archived_transient_error:
            negative_status = latest_http_status or "404"
            mark_negative_media_cache_entry(
                video_url,
                media_dir,
                negative_cache,
                status=negative_status,
                reason=negative_media_reason_for_status(negative_status),
            )
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
    retry_forbidden_media: bool = False,
    media_fetch_policy: Optional[MediaFetchPolicy] = None,
) -> None:
    data = load_snapshot_json(record.json_path)
    if data is None:
        return

    policy = media_fetch_policy or MEDIA_FETCH_FULL
    _, media_items, _, ref_tweets = extract_tweet_content(
        data,
        record.original_url,
        reply_chain_lookup_context=ReplyChainLookupContext(
            json_dir=record.json_path.parent,
            snapshot_timestamp=record.timestamp,
            use_wayback=policy.download_missing and policy.allow_wayback_fallback,
        ),
        allow_reply_chain_media_lookup=policy.download_missing,
    )
    all_media = list(media_items)
    for qt in ref_tweets:
        all_media.extend(qt.media)
    for media_item in all_media:
        if media_item.kind == "image":
            download_image_asset(
                media_item.url,
                record.timestamp,
                media_dir,
                asset_dir_name,
                image_cache,
                negative_cache,
                download_missing=policy.should_download_kind("image"),
                retry_forbidden_media=retry_forbidden_media,
                allow_wayback_fallback=policy.allow_wayback_fallback,
                allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
            )
        elif media_item.kind == "video":
            download_video_asset(
                media_item.url,
                record.timestamp,
                media_dir,
                asset_dir_name,
                image_cache,
                negative_cache,
                download_missing=policy.should_download_kind("video"),
                retry_forbidden_media=retry_forbidden_media,
                allow_wayback_fallback=policy.allow_wayback_fallback,
                allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
            )
            if media_item.poster_url:
                download_image_asset(
                    media_item.poster_url,
                    record.timestamp,
                    media_dir,
                    asset_dir_name,
                    image_cache,
                    negative_cache,
                    download_missing=policy.should_download_kind("video"),
                    retry_forbidden_media=retry_forbidden_media,
                    allow_wayback_fallback=policy.allow_wayback_fallback,
                    allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
                )


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
    reply_chain_use_wayback: bool = True,
    x_api_access_index: Optional[dict] = None,
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
            use_wayback=reply_chain_use_wayback,
        ),
        x_api_access_index=x_api_access_index,
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
    reply_chain_use_wayback: bool = True,
    x_api_access_index: Optional[dict] = None,
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
            future = executor.submit(
                repair_reply_chain_for_snapshot_record,
                record,
                bearer_token,
                depth,
                resume=resume,
                reply_chain_use_wayback=reply_chain_use_wayback,
                x_api_access_index=x_api_access_index,
            )
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
                future = executor.submit(
                    repair_reply_chain_for_snapshot_record,
                    record,
                    bearer_token,
                    depth,
                    resume=resume,
                    reply_chain_use_wayback=reply_chain_use_wayback,
                    x_api_access_index=x_api_access_index,
                )
                pending[future] = record
                next_to_submit += 1

            if checked_count % 25 == 0 or checked_count == total:
                print(
                    f"Reply-chain repair progress: {checked_count}/{total} checked, "
                    f"{candidate_count} reply-chain candidates, {updated_count} JSON updated, {failed_count} failed",
                    flush=True,
                )

    return total, candidate_count, updated_count, failed_count


def collect_snapshot_media_jobs(
    record: SnapshotRecord,
    x_bearer_token: str = "",
    allow_reply_chain_media_lookup: bool = True,
    allow_wayback_ref_media_lookup: bool = True,
) -> List[Tuple[str, str, str]]:
    data = load_snapshot_json(record.json_path)
    if data is None:
        return []

    x_api_enrichment_changed: List[bool] = []
    _, media_items, _, ref_tweets = extract_tweet_content(
        data,
        record.original_url,
        x_bearer_token=x_bearer_token,
        x_api_enrichment_changed=x_api_enrichment_changed,
        reply_chain_lookup_context=ReplyChainLookupContext(
            json_dir=record.json_path.parent,
            snapshot_timestamp=record.timestamp,
            use_wayback=allow_wayback_ref_media_lookup,
        ),
        allow_reply_chain_media_lookup=allow_reply_chain_media_lookup,
    )
    if x_api_enrichment_changed:
        try:
            write_snapshot_json_payload(record.json_path, data)
        except Exception as exc:
            print(f"Warning: failed to persist reply-chain media cache to {record.json_path}: {exc}", flush=True)
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


def _iter_dict_nodes(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_dict_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_dict_nodes(child)


def is_twitter_media_asset_url(url: str) -> bool:
    raw = _coerce_string(url).strip()
    if not raw:
        return False
    parsed = urlparse(raw)
    return parsed.netloc.endswith("pbs.twimg.com") or parsed.netloc.endswith("video.twimg.com")


def is_tweet_media_payload_node(node: dict) -> bool:
    if not isinstance(node, dict) or not _coerce_string(node.get("media_key")).strip():
        return False
    if "display_url" in node or "expanded_url" in node:
        return False
    if _coerce_string(node.get("type")).strip() in {"photo", "video", "animated_gif"}:
        return True
    if "height" in node or "width" in node or isinstance(node.get("variants"), list):
        return True
    return any(
        is_twitter_media_asset_url(node.get(field, ""))
        for field in ("url", "media_url_https", "media_url", "preview_image_url")
    )


def collect_local_media_urls_by_key(records: List[SnapshotRecord]) -> Dict[str, List[str]]:
    urls_by_key: Dict[str, List[str]] = {}
    seen_by_key: Dict[str, set] = {}

    def add(media_key: str, url: str) -> None:
        media_key = _coerce_string(media_key).strip()
        url = _coerce_string(url).strip()
        if not media_key or not is_twitter_media_asset_url(url):
            return
        seen = seen_by_key.setdefault(media_key, set())
        if url in seen:
            return
        seen.add(url)
        urls_by_key.setdefault(media_key, []).append(url)

    for record in records:
        data = load_snapshot_json(record.json_path)
        if data is None:
            continue
        for node in _iter_dict_nodes(data):
            if not is_tweet_media_payload_node(node):
                continue
            media_key = _coerce_string(node.get("media_key")).strip()
            for field in ("url", "media_url_https", "media_url", "preview_image_url"):
                add(media_key, node.get(field, ""))
            variants = node.get("variants")
            if isinstance(variants, list):
                for variant in variants:
                    if isinstance(variant, dict):
                        add(media_key, variant.get("url", ""))
    return urls_by_key


def media_url_replacement_is_compatible(old_url: str, candidate_url: str) -> bool:
    old = urlparse(old_url)
    candidate = urlparse(candidate_url)
    if old.netloc.endswith("pbs.twimg.com") != candidate.netloc.endswith("pbs.twimg.com"):
        return False
    if old.netloc.endswith("video.twimg.com") != candidate.netloc.endswith("video.twimg.com"):
        return False
    old_suffix = Path(old.path).suffix.lower()
    candidate_suffix = Path(candidate.path).suffix.lower()
    if old_suffix and candidate_suffix and old_suffix != candidate_suffix:
        return False
    return True


def choose_local_media_replacement_url(
    media_key: str,
    old_url: str,
    local_media_urls_by_key: Dict[str, List[str]],
    negative_cache: Optional[Dict[str, dict]],
    media_dir: Path,
    image_cache: Optional[Dict[str, str]],
) -> str:
    candidates = []
    for candidate_url in local_media_urls_by_key.get(media_key, []):
        if candidate_url == old_url:
            continue
        if get_negative_media_status(negative_cache, candidate_url) in {"403", "404", "410"}:
            continue
        if not media_url_replacement_is_compatible(old_url, candidate_url):
            continue
        candidates.append(candidate_url)
    if not candidates:
        return ""
    cached_candidates = [
        candidate_url
        for candidate_url in candidates
        if media_job_is_cached(media_dir, image_cache, candidate_url)
    ]
    return (cached_candidates or candidates)[0]


def refresh_snapshot_media_urls_from_local_index(
    record: SnapshotRecord,
    stale_urls: set,
    local_media_urls_by_key: Dict[str, List[str]],
    negative_cache: Optional[Dict[str, dict]],
    media_dir: Path,
    image_cache: Optional[Dict[str, str]],
) -> bool:
    if not stale_urls:
        return False
    data = load_snapshot_json(record.json_path)
    if data is None:
        return False

    changed = False
    for node in _iter_dict_nodes(data):
        if not is_tweet_media_payload_node(node):
            continue
        media_key = _coerce_string(node.get("media_key")).strip()
        for field in ("url", "media_url_https", "media_url", "preview_image_url"):
            current_url = _coerce_string(node.get(field)).strip()
            if current_url not in stale_urls:
                continue
            replacement_url = choose_local_media_replacement_url(
                media_key,
                current_url,
                local_media_urls_by_key,
                negative_cache,
                media_dir,
                image_cache,
            )
            if replacement_url:
                node[field] = replacement_url
                changed = True
        variants = node.get("variants")
        if isinstance(variants, list):
            for variant in variants:
                if not isinstance(variant, dict):
                    continue
                current_url = _coerce_string(variant.get("url")).strip()
                if current_url not in stale_urls:
                    continue
                replacement_url = choose_local_media_replacement_url(
                    media_key,
                    current_url,
                    local_media_urls_by_key,
                    negative_cache,
                    media_dir,
                    image_cache,
                )
                if replacement_url:
                    variant["url"] = replacement_url
                    changed = True

    if changed:
        cache = _get_x_api_json_cache(data, create=True)
        cache["media_url_refreshed_at"] = _utc_now_iso()
        cache["media_url_refresh_source"] = "local_media_key_index"
        write_snapshot_json_payload(record.json_path, data)
    return changed


def refresh_snapshot_media_urls_from_x_api(record: SnapshotRecord, auth_value) -> bool:
    if not normalize_x_api_auths(auth_value):
        return False
    data = load_snapshot_json(record.json_path)
    if data is None:
        return False
    metadata = extract_tweet_metadata(data, record.original_url)
    tweet_id = metadata.get("tweet_id", "")
    if not tweet_id:
        return False

    payload, _, _ = fetch_x_api_tweet_payload_with_auths(auth_value, tweet_id)
    if payload is None:
        return False
    fresh_media_by_key: Dict[str, dict] = {}
    includes = payload.get("includes", {})
    if isinstance(includes, dict):
        for media in includes.get("media", []) or []:
            if isinstance(media, dict):
                media_key = _coerce_string(media.get("media_key")).strip()
                if media_key:
                    fresh_media_by_key[media_key] = media
    if not fresh_media_by_key:
        return False

    changed = False
    for node in _iter_dict_nodes(data):
        if not is_tweet_media_payload_node(node):
            continue
        media_key = _coerce_string(node.get("media_key")).strip()
        fresh_media = fresh_media_by_key.get(media_key)
        if not fresh_media:
            continue
        for field in ("type", "url", "preview_image_url"):
            fresh_value = fresh_media.get(field)
            if fresh_value and node.get(field) != fresh_value:
                node[field] = fresh_value
                changed = True
        fresh_variants = fresh_media.get("variants")
        if isinstance(fresh_variants, list) and fresh_variants and node.get("variants") != fresh_variants:
            node["variants"] = fresh_variants
            changed = True

    if changed:
        cache = _get_x_api_json_cache(data, create=True)
        cache["media_url_refreshed_at"] = _utc_now_iso()
        cache["media_url_refresh_source"] = "x_api_lookup"
        write_snapshot_json_payload(record.json_path, data)
    return changed


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


def media_job_is_blocked_by_negative_cache(
    negative_cache: Optional[Dict[str, dict]],
    url: str,
    *,
    retry_forbidden_media: bool = False,
) -> bool:
    if negative_cache is None:
        return False
    with IMAGE_CACHE_LOCK:
        entry = negative_cache.get(url)
    if not isinstance(entry, dict):
        return False
    status = _coerce_string(entry.get("status")).strip()
    return not (retry_forbidden_media and status == "403")


def get_negative_media_status(negative_cache: Optional[Dict[str, dict]], url: str) -> str:
    if negative_cache is None:
        return ""
    with IMAGE_CACHE_LOCK:
        entry = negative_cache.get(url)
    if not isinstance(entry, dict):
        return ""
    return _coerce_string(entry.get("status")).strip()


def media_jobs_have_hard_negative_cache(jobs: List[Tuple[str, str, str]], negative_cache: Optional[Dict[str, dict]]) -> bool:
    for _, url, _ in jobs:
        if get_negative_media_status(negative_cache, url) in {"404", "410"}:
            return True
    return False


def hard_negative_media_urls_from_jobs(jobs: List[Tuple[str, str, str]], negative_cache: Optional[Dict[str, dict]]) -> set:
    urls = set()
    for _, url, _ in jobs:
        if get_negative_media_status(negative_cache, url) in {"404", "410"}:
            urls.add(url)
    return urls


def drop_forbidden_negative_media_cache_entries(asset_dir: Path, negative_cache: Optional[Dict[str, dict]]) -> int:
    if negative_cache is None:
        return 0
    removed = 0
    with IMAGE_CACHE_LOCK:
        for media_url, entry in list(negative_cache.items()):
            if isinstance(entry, dict) and _coerce_string(entry.get("status")).strip() == "403":
                negative_cache.pop(media_url, None)
                removed += 1
    if removed:
        replace_negative_media_cache(asset_dir, negative_cache)
    return removed


def format_http_failure_detail(status_code: str) -> str:
    status_code = _coerce_string(status_code).strip()
    return f"http={status_code or 'unknown'}"


def repair_missing_media_from_snapshot_records(
    records: List[SnapshotRecord],
    asset_dir: Path,
    workers: int,
    x_bearer_token: str = "",
    resume: bool = True,
    reverse_resume: bool = False,
    verbose: bool = False,
    retry_forbidden_media: bool = False,
    retry_forbidden_each_pass: bool = False,
    allow_wayback_media_fallback: bool = True,
    media_fetch_policy: Optional[MediaFetchPolicy] = None,
) -> Tuple[int, int, int, int]:
    ensure_asset_directories(asset_dir)
    asset_dir_name = asset_dir.name
    media_dir = asset_dir / "media"
    image_cache = load_media_cache(asset_dir, asset_dir_name) if resume else {}
    negative_cache = load_negative_media_cache(asset_dir) if resume else {}
    if retry_forbidden_media:
        retry_count = drop_forbidden_negative_media_cache_entries(asset_dir, negative_cache)
        if retry_count:
            print(f"Retrying {retry_count} cached HTTP 403 media targets...", flush=True)
        if not retry_forbidden_each_pass:
            retry_forbidden_media = False
    media_failure_statuses: Dict[str, str] = {}
    policy = media_fetch_policy or MEDIA_FETCH_FULL

    unique_jobs: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
    record_jobs: Dict[SnapshotRecord, List[Tuple[str, str, str]]] = {}
    job_records_by_key: Dict[Tuple[str, str], List[SnapshotRecord]] = {}

    def register_record_jobs(record: SnapshotRecord, jobs: List[Tuple[str, str, str]]) -> None:
        record_jobs[record] = jobs
        for kind, url, timestamp in jobs:
            unique_jobs.setdefault((kind, url), (kind, url, timestamp))
            records_for_job = job_records_by_key.setdefault((kind, url), [])
            if record not in records_for_job:
                records_for_job.append(record)

    ordered_records = order_snapshot_records_for_resume(records, resume=resume, reverse_resume=reverse_resume)
    local_media_urls_by_key = collect_local_media_urls_by_key(ordered_records)
    allow_ref_media_lookup = bool(normalize_x_api_auths(x_bearer_token))
    for processed_count, record in enumerate(ordered_records, 1):
        jobs = collect_snapshot_media_jobs(
            record,
            x_bearer_token=x_bearer_token,
            allow_reply_chain_media_lookup=allow_ref_media_lookup,
            allow_wayback_ref_media_lookup=False,
        )
        hard_negative_urls = hard_negative_media_urls_from_jobs(jobs, negative_cache)
        if hard_negative_urls:
            if refresh_snapshot_media_urls_from_local_index(
                record,
                hard_negative_urls,
                local_media_urls_by_key,
                negative_cache,
                media_dir,
                image_cache,
            ):
                jobs = collect_snapshot_media_jobs(
                    record,
                    x_bearer_token=x_bearer_token,
                    allow_reply_chain_media_lookup=allow_ref_media_lookup,
                    allow_wayback_ref_media_lookup=False,
                )
                hard_negative_urls = hard_negative_media_urls_from_jobs(jobs, negative_cache)
            if hard_negative_urls and refresh_snapshot_media_urls_from_x_api(record, x_bearer_token):
                jobs = collect_snapshot_media_jobs(
                    record,
                    x_bearer_token=x_bearer_token,
                    allow_reply_chain_media_lookup=allow_ref_media_lookup,
                    allow_wayback_ref_media_lookup=False,
                )
        if jobs:
            register_record_jobs(record, jobs)
        if verbose and (processed_count % 250 == 0 or processed_count == len(ordered_records)):
            print(
                f"Media job collection: {processed_count}/{len(ordered_records)} snapshots scanned, "
                f"{len(unique_jobs)} unique targets found...",
                flush=True,
            )

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

    def register_verbose_pending_jobs(record: SnapshotRecord, jobs: List[Tuple[str, str, str]]) -> None:
        if not verbose or record in verbose_reported_records:
            return
        pending_keys = verbose_pending_by_record.setdefault(record, set())
        for kind, url, _ in jobs:
            if media_job_is_cached(media_dir, image_cache, url):
                continue
            key = (kind, url)
            pending_keys.add(key)
            records_for_job = verbose_records_by_job.setdefault(key, [])
            if record not in records_for_job:
                records_for_job.append(record)

    def refresh_records_for_hard_failed_jobs(failed_jobs: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
        replacement_jobs: List[Tuple[str, str, str]] = []
        replacement_seen = set()
        for kind, url, _ in failed_jobs:
            if get_negative_media_status(negative_cache, url) not in {"404", "410"}:
                continue
            for record in job_records_by_key.get((kind, url), []):
                current_jobs = record_jobs.get(record, [])
                hard_negative_urls = hard_negative_media_urls_from_jobs(current_jobs, negative_cache)
                if not hard_negative_urls:
                    continue
                changed = refresh_snapshot_media_urls_from_local_index(
                    record,
                    hard_negative_urls,
                    local_media_urls_by_key,
                    negative_cache,
                    media_dir,
                    image_cache,
                )
                if not changed and refresh_snapshot_media_urls_from_x_api(record, x_bearer_token):
                    changed = True
                if not changed:
                    continue
                new_jobs = collect_snapshot_media_jobs(
                    record,
                    x_bearer_token=x_bearer_token,
                    allow_reply_chain_media_lookup=allow_ref_media_lookup,
                    allow_wayback_ref_media_lookup=False,
                )
                register_record_jobs(record, new_jobs)
                if verbose and record not in verbose_reported_records:
                    pending_keys = verbose_pending_by_record.setdefault(record, set())
                    for stale_url in hard_negative_urls:
                        for stale_key in list(pending_keys):
                            if stale_key[1] == stale_url:
                                pending_keys.discard(stale_key)
                    register_verbose_pending_jobs(record, new_jobs)
                for new_job in new_jobs:
                    new_key = (new_job[0], new_job[1])
                    if new_key in replacement_seen:
                        continue
                    if media_job_is_cached(media_dir, image_cache, new_job[1]):
                        continue
                    if media_job_is_blocked_by_negative_cache(
                        negative_cache,
                        new_job[1],
                        retry_forbidden_media=retry_forbidden_media,
                    ):
                        continue
                    replacement_seen.add(new_key)
                    replacement_jobs.append(new_job)
        return replacement_jobs

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
            return bool(
                download_image_asset(
                    url,
                    timestamp,
                    media_dir,
                    asset_dir_name,
                    image_cache,
                    negative_cache,
                    failure_statuses=media_failure_statuses,
                    retry_forbidden_media=retry_forbidden_media,
                    allow_wayback_fallback=allow_wayback_media_fallback and policy.allow_wayback_fallback,
                    allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
                    download_missing=policy.should_download_kind("image"),
                )
            )
        return bool(
            download_video_asset(
                url,
                timestamp,
                media_dir,
                asset_dir_name,
                image_cache,
                negative_cache,
                failure_statuses=media_failure_statuses,
                retry_forbidden_media=retry_forbidden_media,
                allow_wayback_fallback=allow_wayback_media_fallback and policy.allow_wayback_fallback,
                allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
                download_missing=policy.should_download_kind("video"),
            )
        )

    skipped_negative = sum(
        1 for _, url, _ in missing_jobs
        if media_job_is_blocked_by_negative_cache(negative_cache, url, retry_forbidden_media=retry_forbidden_media)
    )
    remaining_jobs = [
        job for job in missing_jobs
        if not media_job_is_blocked_by_negative_cache(negative_cache, job[1], retry_forbidden_media=retry_forbidden_media)
    ]
    if skipped_negative:
        print(f"Skipping {skipped_negative}/{missing_before} targets from negative cache...", flush=True)
        if verbose:
            for record, pending_keys in verbose_pending_by_record.items():
                if record in verbose_reported_records:
                    continue
                if any(
                    media_job_is_blocked_by_negative_cache(negative_cache, url, retry_forbidden_media=retry_forbidden_media)
                    for _, url in pending_keys
                ):
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
        hard_failed_jobs: List[Tuple[str, str, str]] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = {}
            next_to_submit = 0
            last_wait_report = time.monotonic()

            while next_to_submit < len(remaining_jobs) and len(pending) < workers * 4:
                future = executor.submit(run_job, remaining_jobs[next_to_submit])
                pending[future] = remaining_jobs[next_to_submit]
                next_to_submit += 1

            completed_count = 0
            while pending:
                done, _ = wait(pending, timeout=5.0, return_when=FIRST_COMPLETED)
                if not done:
                    now = time.monotonic()
                    if verbose and now - last_wait_report >= 15:
                        print(
                            f"Media repair waiting: {completed_count}/{len(remaining_jobs)} checked, "
                            f"{len(pending)} active, {next_to_submit}/{len(remaining_jobs)} submitted...",
                            flush=True,
                        )
                        last_wait_report = now
                    continue
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
                        negative_status = get_negative_media_status(negative_cache, job[1])
                        if negative_status in {"404", "410"}:
                            hard_failed_jobs.append(job)
                        elif verbose and media_job_is_negatively_cached(negative_cache, job[1]):
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
        replacement_jobs = refresh_records_for_hard_failed_jobs(hard_failed_jobs)
        existing_remaining_keys = {(job[0], job[1]) for job in remaining_jobs}
        for job in replacement_jobs:
            key = (job[0], job[1])
            if key not in existing_remaining_keys:
                remaining_jobs.append(job)
                existing_remaining_keys.add(key)
        remaining_jobs = [
            job for job in remaining_jobs
            if not media_job_is_cached(media_dir, image_cache, job[1])
            and not media_job_is_blocked_by_negative_cache(
                negative_cache,
                job[1],
                retry_forbidden_media=retry_forbidden_media,
            )
        ]
        if remaining_jobs and pass_index < DEFAULT_MEDIA_REPAIR_PASSES:
            print(
                f"Retrying {len(remaining_jobs)}/{missing_before} still-missing media targets after a short backoff...",
                flush=True,
            )
            time.sleep(MEDIA_REPAIR_BACKOFF_SECONDS * pass_index)

    final_unique_jobs: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
    for jobs in record_jobs.values():
        for job in jobs:
            final_unique_jobs.setdefault((job[0], job[1]), job)
    total_jobs = len(final_unique_jobs)
    missing_after = sum(
        1 for _, url, _ in final_unique_jobs.values()
        if not media_job_is_cached(media_dir, image_cache, url)
    )
    repaired_count = max(0, missing_before - missing_after)
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


def render_compact_tweet_blockquote(
    *,
    kind: str,
    text: str,
    author: str,
    display_name: str,
    created_at: str,
    url: str,
    media_html: str = "",
    unavailable_reason: str = "",
    extra_class: str = "",
) -> str:
    safe_text = html.escape(text).replace("\n", "<br/>\n")
    header_parts = []
    if display_name:
        header_parts.append(f"<span class='tweet-ref-name'>{html.escape(display_name)}</span>")
    if author:
        header_parts.append(f"<span class='tweet-ref-handle'>@{html.escape(author)}</span>")
    header_html = f"    <div class='tweet-ref-header'>{' '.join(header_parts)}</div>\n" if header_parts else ""
    text_html = f"    <p>{safe_text}</p>\n" if safe_text else ""
    if not text_html and unavailable_reason:
        text_html = f"    <p>Referenced tweet unavailable: {html.escape(unavailable_reason)}</p>\n"
    time_html = ""
    if created_at and url:
        time_html = f"    <span class='tweet-ref-time'><a href='{html.escape(url)}'>{html.escape(created_at)}</a></span>\n"
    elif created_at:
        time_html = f"    <span class='tweet-ref-time'>{html.escape(created_at)}</span>\n"
    elif url:
        time_html = f"    <span class='tweet-ref-time'><a href='{html.escape(url)}'>View referenced tweet</a></span>\n"
    missing_class = " tweet-ref-missing" if unavailable_reason else ""
    classes = f"tweet-ref tweet-ref-{html.escape(kind)}{missing_class}"
    if extra_class:
        classes = f"{classes} {html.escape(extra_class)}"
    return (
        f"  <blockquote class='{classes}'>\n"
        f"{header_html}"
        f"{text_html}"
        f"{media_html}"
        f"{time_html}"
        "  </blockquote>\n"
    )


def media_unavailable_label(kind: str) -> str:
    return "[视频媒体不可用]" if kind == "video" else "[图片媒体不可用]"


def dedupe_media_unavailable_kinds(kinds: List[str]) -> List[str]:
    deduped: List[str] = []
    for kind in kinds:
        normalized = "video" if kind == "video" else "image"
        if normalized not in deduped:
            deduped.append(normalized)
    return deduped


def media_unavailable_text(kinds: List[str]) -> str:
    return "\n".join(media_unavailable_label(kind) for kind in dedupe_media_unavailable_kinds(kinds))


def render_media_unavailable_html(kinds: List[str], indent: str = "  ") -> str:
    return "".join(
        f"{indent}<p class='tweet-media-unavailable'>{html.escape(media_unavailable_label(kind))}</p>\n"
        for kind in dedupe_media_unavailable_kinds(kinds)
    )


def _tweet_id_from_url(url: str) -> str:
    match = TWEET_URL_ID_RE.search(url or "")
    return match.group(1) if match else ""


def _parse_tweet_datetime(value: str, fallback: datetime) -> datetime:
    raw = _coerce_string(value).strip()
    if not raw:
        return fallback

    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        pass

    try:
        parsed = datetime.strptime(raw, "%a %b %d %H:%M:%S %z %Y")
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    except ValueError:
        return fallback


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
    x_api_reply_chain_use_wayback: bool = True,
    retry_forbidden_media: bool = False,
    x_api_access_index: Optional[dict] = None,
    media_fetch_policy: Optional[MediaFetchPolicy] = None,
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

    policy = media_fetch_policy or (
        MEDIA_FETCH_FULL if download_missing_media else MEDIA_FETCH_NONE
    )
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
            use_wayback=x_api_reply_chain_depth > 0 and x_api_reply_chain_use_wayback,
        ),
        allow_reply_chain_media_lookup=policy.download_missing,
        x_api_access_index=x_api_access_index,
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
    missing_media_kinds: List[str] = []
    for media_item in media_items:
        if media_item.kind == "image":
            if not media_item.url:
                missing_media_kinds.append("image")
                continue
            image_path = download_image_asset(
                media_item.url,
                record.timestamp,
                media_dir,
                asset_dir_name,
                image_cache,
                negative_cache,
                download_missing=policy.should_download_kind("image"),
                retry_forbidden_media=retry_forbidden_media,
                allow_wayback_fallback=policy.allow_wayback_fallback,
                allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
            )
            if not image_path:
                missing_media_kinds.append("image")
                continue
            media_html_parts.append(
                f"  <div class='tweet-media-item'><img src='{html.escape(image_path)}' alt='Archived media from tweet captured on {html.escape(dt)}' loading='lazy' decoding='async'/></div>\n"
            )
            continue
        if media_item.kind == "video":
            if not media_item.url:
                missing_media_kinds.append("video")
                continue
            video_path = download_video_asset(
                media_item.url,
                record.timestamp,
                media_dir,
                asset_dir_name,
                image_cache,
                negative_cache,
                download_missing=policy.should_download_kind("video"),
                retry_forbidden_media=retry_forbidden_media,
                allow_wayback_fallback=policy.allow_wayback_fallback,
                allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
            )
            if not video_path:
                missing_media_kinds.append("video")
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
                    download_missing=policy.should_download_kind("video"),
                    retry_forbidden_media=retry_forbidden_media,
                    allow_wayback_fallback=policy.allow_wayback_fallback,
                    allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
                )
                if poster_path:
                    poster_attr = f" poster='{html.escape(poster_path)}'"
            mime_type = media_item.content_type or "video/mp4"
            media_html_parts.append(
                "  <div class='tweet-media-item'>"
                f"<video class='tweet-video' controls preload='none' playsinline{poster_attr}>"
                f"<source src='{html.escape(video_path)}' type='{html.escape(mime_type)}'/>"
                "Your browser does not support the video tag."
                "</video></div>\n"
            )

    media_html = ""
    if media_html_parts:
        media_html = "  <div class='tweet-media'>\n" + "".join(media_html_parts) + "  </div>\n"
    media_unavailable_html = render_media_unavailable_html(missing_media_kinds)
    display_body_text = text or media_unavailable_text(missing_media_kinds)

    tweet_id = metadata.get("tweet_id", "")
    in_reply_to_status_id = metadata.get("in_reply_to_status_id", "")
    primary_node = find_primary_tweet_node(data, record.original_url)
    comment_author = ""
    comment_display_name = ""
    if primary_node is not None:
        comment_author, comment_display_name = _extract_user_info(data, primary_node)
    comment_html = ""
    if tweet_id:
        comment_html = render_compact_tweet_blockquote(
            kind="comment",
            text=text,
            author=comment_author,
            display_name=comment_display_name,
            created_at=dt,
            url=record.original_url,
            media_html=media_html + media_unavailable_html,
            extra_class="tweet-comment",
        )

    quoted_html = ""
    reply_chain_html = ""
    reply_chain_count = 0
    comment_parent_entries: List[ArchiveEntry] = []
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
        rt_missing_media_kinds: List[str] = []
        for rm in rt.media:
            if rm.kind == "image":
                if not rm.url:
                    rt_missing_media_kinds.append("image")
                    continue
                rm_path = download_image_asset(
                    rm.url,
                    record.timestamp,
                    media_dir,
                    asset_dir_name,
                    image_cache,
                    negative_cache,
                    download_missing=policy.should_download_kind("image"),
                    retry_forbidden_media=retry_forbidden_media,
                    allow_wayback_fallback=policy.allow_wayback_fallback,
                    allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
                )
                if rm_path:
                    rt_media_parts.append(
                        f"      <div class='tweet-media-item'><img src='{html.escape(rm_path)}' loading='lazy' decoding='async'/></div>\n"
                    )
                else:
                    rt_missing_media_kinds.append("image")
            elif rm.kind == "video":
                if not rm.url:
                    rt_missing_media_kinds.append("video")
                    continue
                rm_path = download_video_asset(
                    rm.url,
                    record.timestamp,
                    media_dir,
                    asset_dir_name,
                    image_cache,
                    negative_cache,
                    download_missing=policy.should_download_kind("video"),
                    retry_forbidden_media=retry_forbidden_media,
                    allow_wayback_fallback=policy.allow_wayback_fallback,
                    allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
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
                            download_missing=policy.should_download_kind("video"),
                            retry_forbidden_media=retry_forbidden_media,
                            allow_wayback_fallback=policy.allow_wayback_fallback,
                            allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
                        )
                        if pp:
                            rm_poster = f" poster='{html.escape(pp)}'"
                    rm_mime = rm.content_type or "video/mp4"
                    rt_media_parts.append(
                        f"      <div class='tweet-media-item'><video class='tweet-video' controls preload='none' playsinline{rm_poster}>"
                        f"<source src='{html.escape(rm_path)}' type='{html.escape(rm_mime)}'/></video></div>\n"
                    )
                else:
                    rt_missing_media_kinds.append("video")
        rt_media_html = ""
        if rt_media_parts:
            rt_media_html = "    <div class='tweet-media'>\n" + "".join(rt_media_parts) + "    </div>\n"
        rt_media_unavailable_html = render_media_unavailable_html(rt_missing_media_kinds, indent="    ")
        rt_display_body_text = rt.text or media_unavailable_text(rt_missing_media_kinds)
        rt_time_html = ""
        if rt.created_at and rt.url:
            rt_time_html = f"    <span class='tweet-ref-time'><a href='{html.escape(rt.url)}'>{html.escape(rt.created_at)}</a></span>\n"
        elif rt.created_at:
            rt_time_html = f"    <span class='tweet-ref-time'>{html.escape(rt.created_at)}</span>\n"
        elif rt.url:
            rt_time_html = f"    <span class='tweet-ref-time'><a href='{html.escape(rt.url)}'>View referenced tweet</a></span>\n"
        blockquote = (
            f"  <blockquote class='tweet-ref tweet-ref-{rt.kind}{rt_missing_class}'>\n"
            f"{rt_header_html}"
            f"{rt_text_html}"
            f"{rt_media_html}"
            f"{rt_media_unavailable_html}"
            f"{rt_time_html}"
            "  </blockquote>\n"
        )
        if rt.kind == "quoted":
            quoted_html += blockquote
        else:
            reply_chain_html += blockquote
            reply_chain_count += 1
            rt_id = rt.tweet_id or _tweet_id_from_url(rt.url)
            if rt_id:
                parent_comment_html = render_compact_tweet_blockquote(
                    kind="comment",
                    text=rt.text,
                    author=rt.author,
                    display_name=rt.display_name,
                    created_at=rt.created_at,
                    url=rt.url,
                    media_html=rt_media_html + rt_media_unavailable_html,
                    unavailable_reason=rt.unavailable_reason,
                    extra_class="tweet-comment",
                )
                comment_parent_entries.append(
                    ArchiveEntry(
                        block="",
                        dt=_parse_tweet_datetime(rt.created_at, dt_obj),
                        time_text=rt.created_at,
                        body_text=rt_display_body_text,
                        body_length=len(rt_display_body_text),
                        entropy=calculate_entropy(normalize_text_for_entropy(rt_display_body_text)),
                        has_media=bool(rt_media_html or rt_missing_media_kinds),
                        original_url=rt.url or f"https://twitter.com/i/status/{rt_id}",
                        index=index,
                        tweet_id=rt_id,
                        conversation_id=rt.conversation_id or metadata.get("conversation_id", ""),
                        in_reply_to_status_id=rt.in_reply_to_status_id,
                        comment_html=parent_comment_html,
                    )
                )

    deferred_reply_chain_html = ""
    deferred_reply_chain_count = 0
    reply_chain_context_html = ""
    if reply_chain_html and tweet_id:
        deferred_reply_chain_html = reply_chain_html
        deferred_reply_chain_count = reply_chain_count
        reply_chain_context_html = (
            f"  <details class='tweet-reply-chain' data-reply-chain-id='{html.escape(tweet_id)}'>\n"
            f"    <summary>\u4e0a\u7ea7\u56de\u590d\u94fe ({reply_chain_count})</summary>\n"
            f"  </details>\n"
        )
    elif reply_chain_html:
        reply_chain_context_html = (
            f"  <details class='tweet-reply-chain'>\n"
            f"    <summary>\u4e0a\u7ea7\u56de\u590d\u94fe ({reply_chain_count})</summary>\n"
            f"{reply_chain_html}"
            f"  </details>\n"
        )

    comments_placeholder = f"<!-- pale-fire-comments:{tweet_id} -->" if tweet_id else ""
    expand_placeholder = f"<!-- pale-fire-expand:{tweet_id} -->" if tweet_id else ""

    block = (
        f"<div class='tweet' {tweet_attrs}>\n  <h3>{dt}</h3>\n{text_html}{media_html}{media_unavailable_html}{quoted_html}{reply_chain_context_html}{comments_placeholder}  <p class='tweet-footer'><a href='{safe_url}'>View original tweet</a>{expand_placeholder}</p>\n</div>\n"
    )
    return ArchiveEntry(
        block=block,
        dt=dt_obj,
        time_text=dt,
        body_text=display_body_text,
        body_length=len(display_body_text),
        entropy=calculate_entropy(normalize_text_for_entropy(display_body_text)),
        has_media=bool(media_html_parts or missing_media_kinds),
        original_url=record.original_url,
        index=index,
        tweet_id=tweet_id,
        conversation_id=metadata.get("conversation_id", ""),
        in_reply_to_status_id=in_reply_to_status_id,
        comment_html=comment_html,
        comments_placeholder=comments_placeholder,
        expand_placeholder=expand_placeholder,
        comment_parent_entries=tuple(comment_parent_entries),
        deferred_reply_chain_html=deferred_reply_chain_html,
        deferred_reply_chain_count=deferred_reply_chain_count,
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
        "<div class='detail-panel-backdrop' hidden></div>\n"
        "<div class='archive-layout'>\n"
        "<div class='archive-shell'>\n"
        f"{profile_html}"
        "  <header class='archive-header'>\n"
        f"    <h1 class='archive-title'>{html.escape(title)}</h1>\n"
        f"    <p class='archive-subtitle'>{html.escape(subtitle)}</p>\n"
        "  </header>\n"
    )


def inline_json_script_payload(data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return re.sub(r"</", r"<\\/", payload, flags=re.IGNORECASE)


def extract_comment_ids_from_html(comments_html: str) -> Tuple[str, ...]:
    if not comments_html:
        return ()
    return tuple(dict.fromkeys(re.findall(r"data-comment-id=['\"]([^'\"]+)['\"]", comments_html)))


def add_tweet_data_attribute(block: str, name: str, value: str) -> str:
    if not block.startswith("<div class='tweet'"):
        return block
    attr = f" {name}='{html.escape(value)}'"
    end = block.find(">")
    if end == -1:
        return block
    return f"{block[:end]}{attr}{block[end:]}"


def build_deferred_archive_data(entries: List[ArchiveEntry]) -> dict:
    data = {}
    for entry in entries:
        if not entry.tweet_id:
            continue
        record = {}
        if entry.deferred_comments_html:
            record["c"] = entry.deferred_comments_html
            record["cc"] = entry.deferred_comment_count
            record["cl"] = entry.deferred_comment_latest_ts
        if entry.deferred_reply_chain_html:
            record["r"] = entry.deferred_reply_chain_html
            record["rc"] = entry.deferred_reply_chain_count
        if record:
            data[entry.tweet_id] = record
    return data


def render_deferred_archive_data(entries: List[ArchiveEntry]) -> str:
    data = build_deferred_archive_data(entries)
    if not data:
        return ""
    return (
        "<script type='application/json' id='archive-deferred-data'>"
        f"{inline_json_script_payload(data)}"
        "</script>\n"
    )


def render_document_end(asset_dir_name: str, deferred_data_html: str = "") -> str:
    panel_html = (
        "<div class='detail-panel'>\n"
        "  <div class='detail-panel-header'>\n"
        "    <span class='detail-panel-title'>\u8be6\u60c5</span>\n"
        "    <button class='detail-panel-close' aria-label='Close'>\u00d7</button>\n"
        "  </div>\n"
        "  <div class='detail-panel-body'>\n"
        "    <p class='detail-empty'>\u70b9\u51fb\u5e16\u5b50\u67e5\u770b\u8be6\u60c5</p>\n"
        "  </div>\n"
        "</div>\n"
    )
    return (
        "</div>\n"
        f"{panel_html}"
        "</div>\n"
        f"{deferred_data_html}"
        f"<script src='{html.escape(asset_dir_name)}/archive.js'></script>\n"
        "</body>\n</html>"
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


def _render_comment_node(
    comment: ArchiveEntry,
    children_by_parent: Dict[str, List[ArchiveEntry]],
    seen_ids: set[str],
) -> Tuple[str, int, datetime]:
    if not comment.tweet_id or not comment.comment_html or comment.tweet_id in seen_ids:
        return "", 0, datetime.min
    seen_ids.add(comment.tweet_id)

    child_blocks = []
    child_count = 0
    latest_dt = comment.dt
    for child in children_by_parent.get(comment.tweet_id, []):
        child_html, nested_count, nested_latest_dt = _render_comment_node(child, children_by_parent, seen_ids)
        if child_html:
            child_blocks.append(child_html)
            child_count += nested_count
            if nested_latest_dt > latest_dt:
                latest_dt = nested_latest_dt

    nested_html = ""
    if child_blocks:
        nested_html = "    <div class='tweet-comment-children'>\n" + "".join(child_blocks) + "    </div>\n"

    return (
        f"    <div class='tweet-comment-node' data-comment-id='{html.escape(comment.tweet_id)}'>\n"
        f"{comment.comment_html}"
        f"{nested_html}"
        "    </div>\n",
        1 + child_count,
        latest_dt,
    )


def build_comment_section_html(parent_id: str, children_by_parent: Dict[str, List[ArchiveEntry]]) -> Tuple[str, str]:
    if not parent_id:
        return "", ""

    comment_blocks = []
    count = 0
    latest_dt = datetime.min
    seen_ids: set[str] = set()
    for comment in children_by_parent.get(parent_id, []):
        comment_html, nested_count, nested_latest_dt = _render_comment_node(comment, children_by_parent, seen_ids)
        if comment_html:
            comment_blocks.append(comment_html)
            count += nested_count
            if nested_latest_dt > latest_dt:
                latest_dt = nested_latest_dt

    if not count:
        return "", ""
    latest_ts = int(latest_dt.replace(tzinfo=timezone.utc).timestamp()) if latest_dt != datetime.min else 0
    comments_div = (
        f"  <div class='tweet-comments' data-comment-total='{count}' data-comment-latest-ts='{latest_ts}' hidden>\n"
        f"{''.join(comment_blocks)}"
        "  </div>\n"
    )
    expand_btn = (
        f"<button class='tweet-expand-btn' data-comment-count='{count}'>"
        f"\u5c55\u5f00\u8bc4\u8bba ({count})</button>"
    )
    return comments_div, expand_btn


def attach_local_comment_sections(entries: List[ArchiveEntry]) -> List[ArchiveEntry]:
    local_entry_ids = {entry.tweet_id for entry in entries if entry.tweet_id}
    comment_entries = list(entries)
    for entry in entries:
        for parent_entry in entry.comment_parent_entries:
            if parent_entry.tweet_id and parent_entry.tweet_id not in local_entry_ids:
                comment_entries.append(parent_entry)

    entry_ids = {entry.tweet_id for entry in comment_entries if entry.tweet_id}
    children_by_parent: Dict[str, List[ArchiveEntry]] = {}
    for entry in comment_entries:
        if not entry.tweet_id or not entry.in_reply_to_status_id or not entry.comment_html:
            continue
        parent_id = entry.in_reply_to_status_id
        if parent_id not in entry_ids:
            conversation_id = entry.conversation_id
            if conversation_id and conversation_id != entry.tweet_id and conversation_id in entry_ids:
                parent_id = conversation_id
        children_by_parent.setdefault(parent_id, []).append(entry)

    for parent_id, children in list(children_by_parent.items()):
        deduped: List[ArchiveEntry] = []
        seen_child_ids = set()
        for child in sorted(children, key=lambda item: (item.dt, item.index)):
            if child.tweet_id in seen_child_ids:
                continue
            seen_child_ids.add(child.tweet_id)
            deduped.append(child)
        children_by_parent[parent_id] = deduped

    rendered_comment_ids = {
        child.tweet_id
        for children in children_by_parent.values()
        for child in children
        if child.tweet_id
    }
    updated_entries: List[ArchiveEntry] = []
    for entry in entries:
        if not entry.comments_placeholder:
            new_block = entry.block
            if entry.tweet_id in rendered_comment_ids:
                new_block = add_tweet_data_attribute(new_block, "data-is-comment", "true")
            updated_entries.append(replace(entry, block=new_block) if new_block != entry.block else entry)
            continue
        comments_div, expand_btn = build_comment_section_html(entry.tweet_id, children_by_parent)
        new_block = entry.block.replace(entry.comments_placeholder, "")
        if entry.expand_placeholder:
            new_block = new_block.replace(entry.expand_placeholder, expand_btn)
        comment_count = 0
        comment_latest_ts = 0
        if comments_div:
            count_match = re.search(r"data-comment-total='(\d+)'", comments_div)
            latest_match = re.search(r"data-comment-latest-ts='(\d+)'", comments_div)
            if count_match:
                comment_count = int(count_match.group(1))
            if latest_match:
                comment_latest_ts = int(latest_match.group(1))
        if comment_count:
            new_block = add_tweet_data_attribute(new_block, "data-has-comments", "true")
        if entry.tweet_id in rendered_comment_ids:
            new_block = add_tweet_data_attribute(new_block, "data-is-comment", "true")
        updated_entries.append(
            replace(
                entry,
                block=new_block,
                deferred_comments_html=comments_div,
                deferred_comment_count=comment_count,
                deferred_comment_latest_ts=comment_latest_ts,
                deferred_comment_ids=extract_comment_ids_from_html(comments_div),
            )
        )
    return updated_entries


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
        f.write(render_document_end(asset_dir_name, render_deferred_archive_data(entries)))


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
    entries = attach_local_comment_sections(entries)
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
    media_fetch_policy: Optional[MediaFetchPolicy] = None,
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
                    media_fetch_policy=media_fetch_policy,
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
                        media_fetch_policy=media_fetch_policy,
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
    x_api_reply_chain_use_wayback: bool = True,
    retry_forbidden_media: bool = False,
    x_api_access_index: Optional[dict] = None,
    media_fetch_policy: Optional[MediaFetchPolicy] = None,
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
        retry_forbidden_media=retry_forbidden_media,
        media_fetch_policy=media_fetch_policy,
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
            x_api_reply_chain_use_wayback=x_api_reply_chain_use_wayback,
            retry_forbidden_media=retry_forbidden_media,
            x_api_access_index=x_api_access_index,
            media_fetch_policy=media_fetch_policy,
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
    x_api_reply_chain_use_wayback: bool = True,
    verbose: bool = False,
    retry_forbidden_media: bool = False,
    x_api_access_index: Optional[dict] = None,
    media_fetch_policy: Optional[MediaFetchPolicy] = None,
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
    if retry_forbidden_media:
        retry_count = drop_forbidden_negative_media_cache_entries(asset_dir, negative_cache)
        if retry_count:
            print(f"Retrying {retry_count} cached HTTP 403 media targets...", flush=True)
        retry_forbidden_media = False
    policy = media_fetch_policy or (
        MEDIA_FETCH_FULL if download_missing_media else MEDIA_FETCH_NONE
    )

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
            download_missing=policy.should_download_kind("image"),
            retry_forbidden_media=retry_forbidden_media,
            allow_wayback_fallback=policy.allow_wayback_fallback,
            allow_closest_cdx_lookup=policy.allow_closest_cdx_lookup,
        )

    total = len(records)
    submission_items = order_indexed_snapshot_records_for_resume(
        records,
        resume=resume,
        reverse_resume=reverse_resume,
    )
    next_to_submit = 0
    processed_count = 0
    rendered_count = 0
    last_reported_processed = -1
    completed_entries: Dict[int, ArchiveEntry] = {}

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
                x_api_reply_chain_use_wayback,
                retry_forbidden_media,
                x_api_access_index,
                policy,
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
                processed_count += 1
                if entry is not None:
                    completed_entries[index] = entry
                    rendered_count += 1

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
                    x_api_reply_chain_use_wayback,
                    retry_forbidden_media,
                    x_api_access_index,
                    policy,
                )
                pending[future] = record_index
                next_to_submit += 1

            if (
                processed_count != last_reported_processed
                and processed_count
                and (processed_count % 25 == 0 or processed_count == total)
            ):
                print(
                    f"Processed {processed_count}/{total} snapshots "
                    f"(archive entries: {rendered_count})...",
                    flush=True,
                )
                last_reported_processed = processed_count

    write_media_cache(asset_dir, image_cache)
    write_negative_media_cache(asset_dir, negative_cache)
    finalized_entries = [entry for _, entry in sorted(completed_entries.items())]
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
    x_api_reply_chain_use_wayback: bool = True,
    verbose: bool = False,
    retry_forbidden_media: bool = False,
    x_api_access_index: Optional[dict] = None,
    media_fetch_policy: Optional[MediaFetchPolicy] = None,
    final_media_fetch_policy: Optional[MediaFetchPolicy] = None,
    final_media_repair: bool = False,
    final_media_repair_retry_forbidden: bool = False,
) -> List[Path]:
    ensure_asset_directories(asset_dir)
    records = build_snapshot_records(snapshots, asset_dir / "json")
    write_snapshot_manifest(asset_dir, user, records)
    ensure_static_files(asset_dir)
    media_dir = asset_dir / "media"
    asset_dir_name = asset_dir.name
    image_cache = load_media_cache(asset_dir, asset_dir_name) if resume else {}
    negative_cache = load_negative_media_cache(asset_dir) if resume else {}
    if retry_forbidden_media:
        retry_count = drop_forbidden_negative_media_cache_entries(asset_dir, negative_cache)
        if retry_count:
            print(f"Retrying {retry_count} cached HTTP 403 media targets...", flush=True)
        retry_forbidden_media = False
    stream_policy = media_fetch_policy or MEDIA_FETCH_FULL
    final_policy = final_media_fetch_policy or stream_policy

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
                    x_api_reply_chain_use_wayback,
                    retry_forbidden_media,
                    x_api_access_index,
                    stream_policy,
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
                        x_api_reply_chain_use_wayback,
                        retry_forbidden_media,
                        x_api_access_index,
                        stream_policy,
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
            media_fetch_policy=stream_policy,
        )
        ready_records, missing_records = split_ready_and_missing_snapshot_records(records)

    if final_media_repair and ready_records:
        print("Running hard media repair before final HTML rewrite...", flush=True)
        total_jobs, missing_before, repaired_count, missing_after = repair_missing_media_from_snapshot_records(
            ready_records,
            asset_dir,
            workers,
            x_bearer_token=x_bearer_token,
            resume=resume,
            reverse_resume=reverse_resume,
            verbose=verbose,
            retry_forbidden_media=final_media_repair_retry_forbidden,
            retry_forbidden_each_pass=final_media_repair_retry_forbidden,
            allow_wayback_media_fallback=final_policy.allow_wayback_fallback,
            media_fetch_policy=final_policy,
        )
        print(f"Hard media repair targets: {total_jobs}")
        print(f"Missing before hard repair: {missing_before}")
        print(f"Repaired during hard repair: {repaired_count}")
        print(f"Still missing after hard repair: {missing_after}")

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
        x_api_reply_chain_use_wayback=x_api_reply_chain_use_wayback,
        verbose=verbose,
        x_api_access_index=x_api_access_index,
        media_fetch_policy=final_policy,
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


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or a positive number")
    return parsed


def bounded_int(min_value: int, max_value: int):
    def parse(value: str) -> int:
        parsed = int(value)
        if parsed < min_value or parsed > max_value:
            raise argparse.ArgumentTypeError(f"must be between {min_value} and {max_value}")
        return parsed

    return parse


def cli_option_was_supplied(argv: List[str], *option_names: str) -> bool:
    for item in argv:
        for option_name in option_names:
            if item == option_name or item.startswith(f"{option_name}="):
                return True
    return False


def resolve_effort_settings(args: argparse.Namespace) -> EffortSettings:
    explicit_reply_depth = bool(getattr(args, "x_api_reply_chain_depth_explicit", False))
    if args.effort_level is None:
        return EffortSettings(
            reply_chain_depth=args.x_api_reply_chain_depth,
            stream_media_policy=MEDIA_FETCH_FULL,
            final_media_policy=MEDIA_FETCH_FULL,
            run_final_media_repair=False,
            retry_forbidden_media_in_repair=args.retry_403,
        )

    level = args.effort_level
    reply_chain_depth = (
        args.x_api_reply_chain_depth
        if explicit_reply_depth
        else DEFAULT_EFFORT_REPLY_CHAIN_DEPTH if level >= 1 else 0
    )
    if level <= 1:
        stream_policy = MEDIA_FETCH_NONE
    else:
        stream_policy = MEDIA_FETCH_EASY_IMAGES
    final_policy = MEDIA_FETCH_FULL if level >= 3 else stream_policy
    return EffortSettings(
        reply_chain_depth=reply_chain_depth,
        stream_media_policy=stream_policy,
        final_media_policy=final_policy,
        run_final_media_repair=level >= 3,
        retry_forbidden_media_in_repair=args.retry_403 or level >= 3,
    )


def should_load_x_api_auths(args: argparse.Namespace) -> bool:
    explicit_reply_depth = bool(getattr(args, "x_api_reply_chain_depth_explicit", False))
    return (
        (explicit_reply_depth and args.x_api_reply_chain_depth > 0)
        or args.repair_reply_chain_depth
        or args.x_api_timeline
        or args.x_api_conversations
    )


def parse_args() -> argparse.Namespace:
    raw_argv = sys.argv[1:]
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
    mode_group.add_argument(
        "--x-api-conversations",
        action="store_true",
        help="Use X API search to fetch replies for local root conversations and write them into local JSON without regenerating HTML.",
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
        "--no-any-repair",
        dest="no_any_repair",
        action="store_true",
        help="With --html-from-json, disable all network-backed repair/lookups; only local JSON and cached local media are used.",
    )
    parser.add_argument(
        "--no-media-repair",
        dest="no_any_repair",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-tweet status lines as tweet_id:success or tweet_id:failed:http=CODE.",
    )
    parser.add_argument(
        "--retry-403",
        action="store_true",
        help="Retry media URLs previously cached as HTTP 403 instead of skipping them from the negative cache.",
    )
    parser.add_argument(
        "--effort-level",
        type=bounded_int(0, 3),
        default=None,
        metavar="0-3",
        help=(
            "Beginner preset for the default full run: 0 text only, 1 adds local/Wayback reply chains "
            f"to depth {DEFAULT_EFFORT_REPLY_CHAIN_DEPTH}, 2 adds easy image media, 3 adds hard media repair."
        ),
    )
    parser.add_argument(
        "--x-api-reply-chain-depth",
        type=non_negative_int,
        default=0,
        help="Use X API lookup to recursively fill missing replied-to posts up to this parent-chain depth.",
    )
    parser.add_argument(
        "--x-api-timeline",
        action="store_true",
        help="Fetch the user's timeline from X API first and write those posts into local JSON before rendering.",
    )
    parser.add_argument(
        "--x-api-timeline-pages",
        type=non_negative_int,
        default=DEFAULT_X_API_TIMELINE_PAGES,
        metavar="N",
        help=f"Maximum number of X API timeline pages to fetch when --x-api-timeline is enabled; 0 means fetch until exhausted (default: {DEFAULT_X_API_TIMELINE_PAGES}).",
    )
    parser.add_argument(
        "--x-api-timeline-page-size",
        type=bounded_int(5, 100),
        default=DEFAULT_X_API_TIMELINE_PAGE_SIZE,
        metavar="N",
        help=f"Tweets per X API timeline page, 5-100 (default: {DEFAULT_X_API_TIMELINE_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--x-api-conversation-search",
        choices=("recent", "all"),
        default="all",
        help="Search endpoint to use with --x-api-conversations: recent for recent search, all for full-archive search (default: all).",
    )
    parser.add_argument(
        "--x-api-conversation-pages",
        type=non_negative_int,
        default=0,
        metavar="N",
        help="Maximum search pages per conversation when --x-api-conversations is enabled; 0 means fetch until exhausted (default: 0).",
    )
    parser.add_argument(
        "--x-api-conversation-page-size",
        type=bounded_int(10, 500),
        default=DEFAULT_X_API_TIMELINE_PAGE_SIZE,
        metavar="N",
        help=f"Posts per conversation search page, 10-500; recent search is capped at 100 by the script (default: {DEFAULT_X_API_TIMELINE_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--x-api-min-interval",
        type=non_negative_float,
        default=None,
        metavar="SECONDS",
        help=f"Minimum seconds between X API requests across all workers; set 0 to disable (default: env X_API_MIN_INTERVAL_SECONDS or {DEFAULT_X_API_MIN_INTERVAL_SECONDS}).",
    )
    parser.add_argument(
        "--x-api-only",
        "--no-wayback-fallback",
        dest="x_api_only",
        action="store_true",
        help="For X API-backed work, use local JSON and X API only for ordinary missing parents; conversation_id root recovery may still query Wayback.",
    )
    parser.add_argument(
        "--x-bearer-token-env",
        default="X_BEARER_TOKEN",
        help="Environment variable name containing the X API bearer token (default: X_BEARER_TOKEN; TWITTER_BEARER_TOKEN is also checked).",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        default=None,
        help="Load X API credentials from this dotenv file. Repeat to try multiple credential sets in order (default: .env).",
    )
    args = parser.parse_args()
    args.x_api_reply_chain_depth_explicit = cli_option_was_supplied(raw_argv, "--x-api-reply-chain-depth")
    args.env_files = args.env_file or [".env"]
    args.env_file = args.env_files[0]
    if args.effort_level is not None and (
        args.json_only
        or args.html_from_json
        or args.repair_media
        or args.repair_reply_chain_depth
        or args.x_api_conversations
        or args.x_api_timeline
    ):
        parser.error("--effort-level only applies to the default full run")
    if args.no_any_repair and not args.html_from_json:
        parser.error("--no-any-repair can only be used with --html-from-json")
    if args.no_any_repair and args.no_resume:
        parser.error("--no-any-repair cannot be combined with --no-resume")
    if args.no_any_repair and args.x_api_reply_chain_depth:
        parser.error("--no-any-repair cannot be combined with --x-api-reply-chain-depth")
    if args.reverse_resume and args.no_resume:
        parser.error("--reverse-resume cannot be combined with --no-resume")
    if args.x_api_reply_chain_depth and (args.json_only or args.repair_media or args.repair_reply_chain_depth or args.x_api_conversations):
        parser.error("--x-api-reply-chain-depth only applies while rendering HTML")
    if args.x_api_timeline and (args.html_from_json or args.repair_media or args.repair_reply_chain_depth or args.x_api_conversations):
        parser.error("--x-api-timeline can only be used with normal rendering or --json-only")
    if args.x_api_only and not (args.x_api_reply_chain_depth or args.repair_reply_chain_depth or args.x_api_timeline or args.repair_media or args.x_api_conversations):
        parser.error("--x-api-only only applies with X API reply-chain lookup/repair, --x-api-timeline, --x-api-conversations, or --repair-media")
    return args


def main():
    args = parse_args()
    load_env_file(Path(args.env_file))
    x_api_min_interval = (
        args.x_api_min_interval
        if args.x_api_min_interval is not None
        else parse_float_env("X_API_MIN_INTERVAL_SECONDS", DEFAULT_X_API_MIN_INTERVAL_SECONDS)
    )
    set_x_api_min_interval(x_api_min_interval)
    user = args.twitter_username.lstrip("@").strip()
    resume = not args.no_resume
    output_path, asset_dir, _, json_dir, _ = build_output_paths(user)
    effort_settings = resolve_effort_settings(args)
    x_api_auths: List[XApiAuth] = []
    x_bearer_token = XApiAuth()
    x_api_access_index = load_x_api_access_index(asset_dir)

    if args.reverse_resume:
        print("Resume order is set to newest-first for any remaining work.", flush=True)
    if args.effort_level is not None:
        print(
            f"Effort level {args.effort_level} enabled: reply-chain depth {effort_settings.reply_chain_depth}, "
            f"stream media policy {effort_settings.stream_media_policy.name}, "
            f"final media policy {effort_settings.final_media_policy.name}.",
            flush=True,
        )
    if should_load_x_api_auths(args):
        x_api_auths = get_x_api_auths_from_env_files(args.env_files, args.x_bearer_token_env)
        if not x_api_auths:
            fallback_auth = get_x_api_auth(args.x_bearer_token_env)
            if fallback_auth:
                x_api_auths = [fallback_auth]
        x_bearer_token = x_api_auths
        if not x_bearer_token:
            if args.x_api_timeline:
                flag_name = "--x-api-timeline"
            elif args.x_api_conversations:
                flag_name = "--x-api-conversations"
            else:
                flag_name = "--repair-reply-chain-depth" if args.repair_reply_chain_depth else "--x-api-reply-chain-depth"
            fallback_detail = (
                "using local JSON only; ordinary Wayback fallback is disabled."
                if args.x_api_only
                else "using local JSON and Wayback fallback only."
            )
            print(
                f"{flag_name}: no X API credentials found in {', '.join(args.env_files)} or environment; "
                f"{fallback_detail}",
                flush=True,
            )
    if args.repair_reply_chain_depth:
        auth_description = "; ".join(auth.describe() for auth in x_api_auths) if x_api_auths else x_bearer_token.describe()
        print(
            f"X API reply-chain repair is enabled to depth {args.repair_reply_chain_depth}, "
            f"min interval {X_API_MIN_INTERVAL_SECONDS:g}s ({auth_description}).",
            flush=True,
        )
    elif args.x_api_reply_chain_depth:
        auth_description = "; ".join(auth.describe() for auth in x_api_auths) if x_api_auths else x_bearer_token.describe()
        print(
            f"X API reply-chain lookup is enabled to depth {args.x_api_reply_chain_depth}, "
            f"min interval {X_API_MIN_INTERVAL_SECONDS:g}s ({auth_description}).",
            flush=True,
        )
    if args.x_api_timeline:
        auth_description = "; ".join(auth.describe() for auth in x_api_auths) if x_api_auths else x_bearer_token.describe()
        print(
            f"X API timeline fetch is enabled: {args.x_api_timeline_pages} page(s), "
            f"{args.x_api_timeline_page_size} tweets/page, min interval {X_API_MIN_INTERVAL_SECONDS:g}s ({auth_description}).",
            flush=True,
        )
    if args.x_api_conversations:
        auth_description = "; ".join(auth.describe() for auth in x_api_auths) if x_api_auths else x_bearer_token.describe()
        print(
            f"X API conversation search is enabled: {args.x_api_conversation_search}, "
            f"{args.x_api_conversation_pages} page(s)/conversation, "
            f"{args.x_api_conversation_page_size} posts/page, min interval {X_API_MIN_INTERVAL_SECONDS:g}s ({auth_description}).",
            flush=True,
        )
    if args.x_api_only:
        if args.repair_media and not (args.x_api_timeline or args.x_api_reply_chain_depth or args.repair_reply_chain_depth):
            print("Wayback media fallback is disabled for repair-media; only original media URLs and local cache will be used.", flush=True)
        else:
            print("Ordinary Wayback fallback is disabled for X API-backed work; conversation_id root recovery may still use it.", flush=True)

    if args.html_from_json or args.repair_media or args.repair_reply_chain_depth or args.x_api_conversations:
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
                x_bearer_token=x_bearer_token,
                resume=resume,
                reverse_resume=args.reverse_resume,
                verbose=args.verbose,
                retry_forbidden_media=args.retry_403,
                allow_wayback_media_fallback=not args.x_api_only,
            )
            print(f"Unique media targets: {total_jobs}")
            print(f"Missing before repair: {missing_before}")
            print(f"Repaired this run: {repaired_count}")
            print(f"Still missing after repair: {missing_after}")
            print("Finished media repair stage.")
            print(f"  - {media_index_path(asset_dir)}")
            print(f"  - {asset_dir / 'media'}")
            return
        if args.x_api_conversations:
            if not x_bearer_token:
                print(f"--x-api-conversations: no X API credentials found in {', '.join(args.env_files)} or environment.")
                sys.exit(1)
            print(
                f"Found {len(available_records)} local JSON snapshots. Repairing conversation comments via X API search...",
                flush=True,
            )
            total_conversations, written_count, reused_count, failed_count, conversation_records = (
                repair_conversations_from_snapshot_records(
                    available_records,
                    user,
                    asset_dir,
                    x_bearer_token,
                    search_mode=args.x_api_conversation_search,
                    pages=args.x_api_conversation_pages,
                    page_size=args.x_api_conversation_page_size,
                    resume=resume,
                    access_index=x_api_access_index,
                )
            )
            merged_records = merge_snapshot_records_prefer_first(conversation_records, available_records)
            write_snapshot_manifest(asset_dir, user, merged_records)
            write_x_api_access_index(asset_dir, x_api_access_index)
            print(f"Root conversations checked: {total_conversations}")
            print(f"Conversation JSON written: {written_count}")
            print(f"Conversation posts reused: {reused_count}")
            print(f"Conversation lookups failed: {failed_count}")
            print("Finished conversation repair stage.")
            print(f"  - {snapshot_manifest_path(asset_dir)}")
            print(f"  - {json_dir}")
            print(f"  - {x_api_access_index_path(asset_dir)}")
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
                reply_chain_use_wayback=not args.x_api_only,
                x_api_access_index=x_api_access_index,
            )
            write_x_api_access_index(asset_dir, x_api_access_index)
            print(f"JSON snapshots checked: {total_records}")
            print(f"Reply-chain candidates: {candidate_count}")
            print(f"JSON snapshots updated: {updated_count}")
            print(f"Failed snapshots: {failed_count}")
            print("Finished reply-chain repair stage.")
            print(f"  - {json_dir}")
            print(f"  - {x_api_access_index_path(asset_dir)}")
            return
        else:
            print(f"Found {len(available_records)} local JSON snapshots. Rendering HTML...")
            if args.no_any_repair:
                print("All network-backed repair during HTML rendering is disabled; using only local JSON and cached local media.", flush=True)
            written_paths = build_archives_from_snapshot_records(
                available_records,
                user,
                output_path,
                asset_dir,
                args.workers,
                resume=resume,
                download_missing_media=not args.no_any_repair,
                reverse_resume=args.reverse_resume,
                x_bearer_token=x_bearer_token,
                x_api_reply_chain_depth=args.x_api_reply_chain_depth,
                x_api_reply_chain_use_wayback=not args.x_api_only,
                verbose=args.verbose,
                retry_forbidden_media=args.retry_403,
                x_api_access_index=x_api_access_index,
            )
            if args.x_api_reply_chain_depth:
                write_x_api_access_index(asset_dir, x_api_access_index)
    else:
        if args.x_api_timeline:
            if not x_bearer_token:
                if args.x_api_only:
                    print(f"--x-api-timeline: no X API credentials found in {', '.join(args.env_files)} or environment.")
                    sys.exit(1)
                print("No X API credentials found; falling back to Wayback snapshot discovery.", flush=True)
            else:
                ensure_asset_directories(asset_dir)
                print(f"Fetching @{user} timeline from X API...", flush=True)
                timeline_payloads, timeline_error, timeline_source = fetch_x_api_timeline_payloads_with_auths(
                    user,
                    x_bearer_token,
                    pages=args.x_api_timeline_pages,
                    page_size=args.x_api_timeline_page_size,
                    access_index=x_api_access_index,
                )
                x_api_records, written_count, reused_count = write_x_api_timeline_snapshot_records(
                    user,
                    timeline_payloads,
                    json_dir,
                    resume=resume,
                    source=timeline_source,
                )
                write_x_api_access_index(asset_dir, x_api_access_index)
                if x_api_records:
                    local_records = []
                    if resume and not args.refresh_snapshots:
                        local_records = [
                            record for record in load_snapshot_records(asset_dir, json_dir, user)
                            if record.json_path.exists()
                        ]
                    records = merge_snapshot_records_prefer_first(x_api_records, local_records)
                    write_snapshot_manifest(asset_dir, user, records)
                    print(
                        f"X API timeline JSON ready: {len(x_api_records)} "
                        f"(written this run: {written_count}, reused: {reused_count}).",
                        flush=True,
                    )
                    if local_records:
                        print(f"Merged with {len(local_records)} existing local JSON records; X API records take precedence by tweet id.", flush=True)
                    if timeline_error:
                        print(f"X API timeline stopped early: {timeline_error}", flush=True)

                    if args.json_only:
                        print("Finished X API timeline JSON stage.")
                        print(f"  - {snapshot_manifest_path(asset_dir)}")
                        print(f"  - {json_dir}")
                        print(f"  - {x_api_access_index_path(asset_dir)}")
                        return

                    print(f"Rendering HTML from {len(records)} local JSON records...", flush=True)
                    written_paths = build_archives_from_snapshot_records(
                        records,
                        user,
                        output_path,
                        asset_dir,
                        args.workers,
                        resume=resume,
                        reverse_resume=args.reverse_resume,
                        x_bearer_token=x_bearer_token,
                        x_api_reply_chain_depth=args.x_api_reply_chain_depth,
                        x_api_reply_chain_use_wayback=not args.x_api_only,
                        verbose=args.verbose,
                        retry_forbidden_media=args.retry_403,
                        x_api_access_index=x_api_access_index,
                    )
                    if args.x_api_reply_chain_depth:
                        write_x_api_access_index(asset_dir, x_api_access_index)
                    print("Finished. Wrote:")
                    for path in written_paths:
                        print(f"  - {path}")
                    return

                if timeline_error:
                    print(f"X API timeline fetch did not return usable posts: {timeline_error}", flush=True)
                if args.x_api_only:
                    print("No X API timeline records available and Wayback fallback is disabled.")
                    sys.exit(1)
                print("Falling back to Wayback snapshot discovery.", flush=True)

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
        stream_media_policy = effort_settings.stream_media_policy
        final_media_policy = effort_settings.final_media_policy
        if args.effort_level is not None and args.x_api_only:
            stream_media_policy = replace(stream_media_policy, allow_wayback_fallback=False)
            final_media_policy = replace(final_media_policy, allow_wayback_fallback=False)
        written_paths = build_archives(
            snapshots,
            user,
            output_path,
            asset_dir,
            args.workers,
            resume=resume,
            reverse_resume=args.reverse_resume,
            x_bearer_token=x_bearer_token,
            x_api_reply_chain_depth=effort_settings.reply_chain_depth,
            x_api_reply_chain_use_wayback=not args.x_api_only,
            verbose=args.verbose,
            retry_forbidden_media=args.retry_403,
            x_api_access_index=x_api_access_index,
            media_fetch_policy=stream_media_policy,
            final_media_fetch_policy=final_media_policy,
            final_media_repair=effort_settings.run_final_media_repair,
            final_media_repair_retry_forbidden=effort_settings.retry_forbidden_media_in_repair,
        )
        if args.x_api_reply_chain_depth:
            write_x_api_access_index(asset_dir, x_api_access_index)

    print("Finished. Wrote:")
    for path in written_paths:
        print(f"  - {path}")


if __name__ == "__main__":
    main()

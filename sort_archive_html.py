#!/usr/bin/env python3
"""
Reorder an existing self-contained archive HTML without re-downloading data.
"""

from __future__ import annotations

import argparse
import html
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


TWEET_MARKER = "<div class='tweet'>"
TITLE_RE = re.compile(r"(<title>)(.*?)(</title>)", re.DOTALL)
H1_RE = re.compile(r"(<h1\b[^>]*>)(.*?)(</h1>)", re.DOTALL)
TIME_RE = re.compile(r"<h3>(.*?)</h3>", re.DOTALL)
BODY_RE = re.compile(r"<p>(.*?)</p>", re.DOTALL)
LINK_RE = re.compile(r"<a href='([^']+)'>View original tweet</a>", re.DOTALL)
BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
SORT_SUFFIX_RE = re.compile(
    r"_(time_desc|media_first_time_desc|text_length_desc|text_entropy_desc)$"
)


@dataclass(frozen=True)
class TweetEntry:
    block: str
    dt: datetime
    time_text: str
    body_text: str
    body_length: int
    entropy: float
    has_media: bool
    original_url: str
    index: int


def split_tweet_blocks(document: str) -> tuple[str, list[str], str]:
    first = document.find(TWEET_MARKER)
    if first == -1:
        raise ValueError("Could not find any tweet blocks in the input HTML.")

    prefix = document[:first]
    blocks: list[str] = []
    pos = first
    doc_len = len(document)

    while True:
        start = document.find(TWEET_MARKER, pos)
        if start == -1:
            break

        cursor = start
        depth = 0
        while True:
            next_open = document.find("<div", cursor)
            next_close = document.find("</div>", cursor)
            if next_close == -1:
                raise ValueError("Encountered an unterminated tweet block.")

            if next_open != -1 and next_open < next_close:
                depth += 1
                cursor = next_open + 4
                continue

            depth -= 1
            cursor = next_close + len("</div>")
            if depth == 0:
                while cursor < doc_len and document[cursor].isspace():
                    cursor += 1
                blocks.append(document[start:cursor])
                pos = cursor
                break

    suffix = document[pos:]
    return prefix, blocks, suffix


def extract_body_html(block: str) -> str:
    for match in BODY_RE.finditer(block):
        body_html = match.group(1)
        if "View original tweet" not in body_html:
            return body_html
    return ""


def html_fragment_to_text(fragment: str) -> str:
    text = BR_RE.sub("\n", fragment)
    text = TAG_RE.sub("", text)
    return html.unescape(text).strip()


def normalize_text_for_entropy(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def calculate_entropy(text: str) -> float:
    if not text:
        return 0.0
    counts = Counter(text)
    total = len(text)
    return -sum((count / total) * math.log2(count / total) for count in counts.values())


def parse_entry(block: str, index: int) -> TweetEntry:
    time_match = TIME_RE.search(block)
    if not time_match:
        raise ValueError(f"Tweet block #{index} is missing its <h3> timestamp.")
    time_text = html.unescape(time_match.group(1).strip())
    dt = datetime.strptime(time_text, "%Y-%m-%d %H:%M:%S UTC")

    body_html = extract_body_html(block)
    body_text = html_fragment_to_text(body_html)

    link_match = LINK_RE.search(block)
    original_url = html.unescape(link_match.group(1)) if link_match else ""

    return TweetEntry(
        block=block,
        dt=dt,
        time_text=time_text,
        body_text=body_text,
        body_length=len(body_text),
        entropy=calculate_entropy(normalize_text_for_entropy(body_text)),
        has_media=("class='tweet-media'" in block and "<img" in block),
        original_url=original_url,
        index=index,
    )


def retitle_document(prefix: str, title_text: str) -> str:
    prefix = TITLE_RE.sub(rf"\1{title_text}\3", prefix, count=1)
    prefix = H1_RE.sub(rf"\1{title_text}\3", prefix, count=1)
    return prefix


def extract_document_title(document: str, fallback: str) -> str:
    match = TITLE_RE.search(document)
    if not match:
        return fallback
    return html.unescape(match.group(2).strip())


def strip_sort_suffix(title_text: str) -> str:
    suffixes = (
        " (time desc)",
        " (media first, time desc)",
        " (text length desc)",
        " (text entropy desc)",
    )
    for suffix in suffixes:
        if title_text.endswith(suffix):
            return title_text[: -len(suffix)]
    return title_text


def sort_entries(entries: list[TweetEntry], mode: str) -> list[TweetEntry]:
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


def write_sorted_html(
    prefix: str,
    suffix: str,
    entries: list[TweetEntry],
    output_path: Path,
    title_text: str,
) -> None:
    new_prefix = retitle_document(prefix, title_text)
    output_path.write_text(
        new_prefix + "".join(entry.block for entry in entries) + suffix,
        encoding="utf-8",
    )


def build_outputs(input_path: Path) -> list[tuple[str, Path]]:
    document = input_path.read_text(encoding="utf-8")
    prefix, blocks, suffix = split_tweet_blocks(document)
    entries = [parse_entry(block, index) for index, block in enumerate(blocks)]
    base_title = strip_sort_suffix(extract_document_title(document, input_path.stem))
    base_stem = SORT_SUFFIX_RE.sub("", input_path.stem)

    jobs = [
        (
            f"{base_title} (time desc)",
            "time_desc",
            input_path.with_name(f"{base_stem}_time_desc{input_path.suffix}"),
        ),
        (
            f"{base_title} (media first, time desc)",
            "media_first_time_desc",
            input_path.with_name(f"{base_stem}_media_first_time_desc{input_path.suffix}"),
        ),
        (
            f"{base_title} (text length desc)",
            "text_length_desc",
            input_path.with_name(f"{base_stem}_text_length_desc{input_path.suffix}"),
        ),
        (
            f"{base_title} (text entropy desc)",
            "text_entropy_desc",
            input_path.with_name(f"{base_stem}_text_entropy_desc{input_path.suffix}"),
        ),
    ]

    outputs: list[tuple[str, Path]] = []
    for title_text, mode, output_path in jobs:
        sorted_entries = sort_entries(entries, mode)
        write_sorted_html(prefix, suffix, sorted_entries, output_path, title_text)
        outputs.append((mode, output_path))
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Reorder an existing archive HTML file.")
    parser.add_argument(
        "input_html",
        nargs="?",
        default="anincandescence_archive.html",
        help="Path to the existing archive HTML file.",
    )
    args = parser.parse_args()

    input_path = Path(args.input_html).resolve()
    outputs = build_outputs(input_path)
    for mode, output_path in outputs:
        print(f"{mode}: {output_path}")


if __name__ == "__main__":
    main()

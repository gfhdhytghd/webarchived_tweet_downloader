# webarchived_tweet_downloader

Download archived tweets for a Twitter/X account from the Internet Archive Wayback Machine and export them into a static archive bundle with shared HTML/CSS/JS/assets.

The exporter:

- queries the Wayback CDX API for archived tweet snapshots
- fetches archived tweet JSON payloads
- extracts tweet text from several Twitter payload shapes
- downloads tweet images into a shared asset folder
- writes mobile-friendly archive pages with built-in image tap-to-zoom
- saves raw archived JSON responses alongside the site assets
- outputs the chronological archive plus four sorted HTML variants

## Requirements

- Python 3.9+
- `requests`

Install the dependency with:

```bash
pip install requests
```

## Usage

Basic usage:

```bash
python download_archive.py <twitter_username>
```

Fetch only JSON snapshots:

```bash
python download_archive.py <twitter_username> --json-only
```

Regenerate HTML from local JSON snapshots:

```bash
python download_archive.py <twitter_username> --html-from-json
```

Set a custom concurrency level:

```bash
python download_archive.py <twitter_username> --workers 24
```

Force-refresh the Wayback snapshot list:

```bash
python download_archive.py <twitter_username> --refresh-snapshots
```

Examples:

```bash
python download_archive.py AnIncandescence
python download_archive.py AnIncandescence --json-only --workers 24
python download_archive.py AnIncandescence --html-from-json
```

The script always writes everything into a dedicated `<username>_archive/` folder.

## Output Layout

For a user like `AnIncandescence`, the downloader writes:

- `AnIncandescence_archive/index.html`
- `AnIncandescence_archive/AnIncandescence_archive.html`
- `AnIncandescence_archive/AnIncandescence_archive_time_desc.html`
- `AnIncandescence_archive/AnIncandescence_archive_media_first_time_desc.html`
- `AnIncandescence_archive/AnIncandescence_archive_text_length_desc.html`
- `AnIncandescence_archive/AnIncandescence_archive_text_entropy_desc.html`
- `AnIncandescence_archive/AnIncandescence_archive_assets/archive.css`
- `AnIncandescence_archive/AnIncandescence_archive_assets/archive.js`
- `AnIncandescence_archive/AnIncandescence_archive_assets/media/...`
- `AnIncandescence_archive/AnIncandescence_archive_assets/json/...`
- `AnIncandescence_archive/AnIncandescence_archive_assets/snapshots.json`
- `AnIncandescence_archive/AnIncandescence_archive_assets/media_index.json`

The generated archive pages include:

- responsive mobile-friendly layout
- larger card-style tweet blocks
- clickable / tap-to-zoom images via a built-in lightbox
- lazy-loaded external image assets
- built-in toggles for hiding replies and reposts
- hidden `data-*` reply metadata on each tweet block for later filtering
- an auto-generated `index.html` navigation page linking all five archive views

The hidden metadata currently includes:

- `data-tweet-id`
- `data-conversation-id`
- `data-in-reply-to-status-id`
- `data-in-reply-to-user-id`
- `data-in-reply-to-screen-name`
- `data-is-reply`
- `data-is-thread-root`
- `data-is-repost`
- `data-json-path`

Each JSON file is stored as one archived response per tweet capture, named with the capture timestamp and tweet id.

The downloader also writes:

- `snapshots.json`: manifest of timestamps, original tweet URLs, and expected JSON filenames
- `media_index.json`: cache of downloaded media URLs to local asset paths for resume runs

## What The Script Does

1. Calls the Wayback CDX API with a tweet URL prefix for the target account.
2. Deduplicates captures by content digest when listing snapshots.
3. Downloads archived tweet payloads with retry and backoff, or reuses local JSON files during resume runs.
4. Extracts text from multiple Twitter response formats, including legacy and newer nested structures.
5. Saves media, CSS, JS, raw JSON, and manifest/cache files into a shared asset folder.
6. Streams the main chronological archive while the default full run is downloading snapshots.
7. Writes the remaining four sorted HTML variants plus `index.html` after the full dataset is ready.

## Notes

- The output is no longer self-contained in a single file; keep each generated `<username>_archive/` directory intact.
- `index.html` is written inside that per-user output directory as the default entry page for static hosting.
- Resume is enabled by default. Existing valid JSON snapshots, downloaded media assets, and the local `snapshots.json` manifest are reused automatically. Pass `--no-resume` to force re-fetching JSON/media, or `--refresh-snapshots` to force a fresh CDX snapshot list lookup.
- Missing JSON snapshots are retried automatically across multiple passes in each run before the final HTML pages are finalized.
- In the default full run, tweet media is prefetched alongside snapshot JSON preparation so the streaming chronological HTML page is not waiting on media downloads at finalize time.
- `--json-only` stops after the JSON stage and does not write HTML files.
- `--html-from-json` skips snapshot JSON fetching and rebuilds the HTML pages from the local `<username>_archive/*_archive_assets/json/` directory.
- Wayback responses are inconsistent; some archived captures may still fail to parse.
- The script retries transient network failures, but very large exports can still take a while.
- The Internet Archive may throttle aggressive traffic. If necessary, rerun the command later.

## License

MIT. See [LICENSE](LICENSE).

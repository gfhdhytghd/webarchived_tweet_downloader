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

Regenerate HTML from local JSON snapshots without trying to repair missing media:

```bash
python download_archive.py <twitter_username> --html-from-json --no-media-repair
```

Repair only missing local media assets from existing JSON snapshots:

```bash
python download_archive.py <twitter_username> --repair-media
```

Repair only missing replied-to parent chains from existing JSON snapshots:

```bash
python download_archive.py <twitter_username> --repair-reply-chain-depth 8
```

Set a custom concurrency level:

```bash
python download_archive.py <twitter_username> --workers 24
```

Force-refresh the Wayback snapshot list:

```bash
python download_archive.py <twitter_username> --refresh-snapshots
```

Resume remaining snapshot/media work from newest to oldest:

```bash
python download_archive.py <twitter_username> --reverse-resume
```

Use X API lookup to fill missing replied-to parent chains while rendering HTML:

```bash
python download_archive.py <twitter_username> --html-from-json --x-api-reply-chain-depth 8
```

Use a non-default dotenv file:

```bash
python download_archive.py <twitter_username> --repair-reply-chain-depth 8 --env-file ./private.env
```

Examples:

```bash
python download_archive.py AnIncandescence
python download_archive.py AnIncandescence --json-only --workers 24
python download_archive.py AnIncandescence --json-only --reverse-resume
python download_archive.py AnIncandescence --html-from-json
python download_archive.py AnIncandescence --html-from-json --x-api-reply-chain-depth 8
python download_archive.py AnIncandescence --repair-reply-chain-depth 8
python download_archive.py AnIncandescence --html-from-json --reverse-resume
python download_archive.py AnIncandescence --html-from-json --no-media-repair
python download_archive.py AnIncandescence --repair-media --workers 12
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
- `--reverse-resume` changes resume-time processing priority to newest-first for any remaining JSON/media work, but does not change the final HTML output order.
- Missing JSON snapshots are retried automatically across multiple passes in each run before the final HTML pages are finalized.
- In the default full run, tweet media is prefetched alongside snapshot JSON preparation so the streaming chronological HTML page is not waiting on media downloads at finalize time.
- `--json-only` stops after the JSON stage and does not write HTML files.
- `--html-from-json` skips snapshot JSON fetching and rebuilds the HTML pages from the local `<username>_archive/*_archive_assets/json/` directory.
- `--html-from-json --no-media-repair` rebuilds HTML from local JSON but does not attempt to fetch any still-missing media; only already cached local media files are used.
- `.env` is loaded automatically before X API credentials are read. See `.env.schema` for the supported variables. Map the X Developer labels as: `Customer/Consumer Key` -> `X_API_KEY`, `Customer/Consumer Key Secret` -> `X_API_KEY_SECRET`, `OAuth 1.0a Access Token` -> `X_ACCESS_TOKEN`, and `OAuth 1.0a Access Token Secret` -> `X_ACCESS_TOKEN_SECRET`. Complete OAuth 1.0a user-context credentials are preferred for protected posts visible to that user. `X_BEARER_TOKEN` / `TWITTER_BEARER_TOKEN` remains supported for app-only lookups.
- `--x-api-reply-chain-depth N` uses X API v2 Post lookup to recursively fetch missing replied-to parent posts up to `N` levels while rendering HTML. Fetched reply-chain payloads are persisted into each snapshot JSON under `_pale_fire.x_api_reply_chains`, so later `--html-from-json` runs can render the cached chain without a token. Without X API credentials, lookup still tries matching local JSON snapshots and Wayback CDX captures.
- `--repair-reply-chain-depth N` does the same reply-chain enrichment as a repair-only stage: it scans local JSON snapshots, writes `_pale_fire.x_api_reply_chains`, and does not regenerate HTML. Existing complete caches are reused, but cached `terminal_error` failures from older runs are retried through local JSON, OAuth 1.0a / bearer X API when configured, and Wayback fallback. The misspelled alias `--repair-reply-chain-deepth` is also accepted.
- `--repair-media` scans local snapshot JSON files, finds media that is still missing from `<username>_archive/*_archive_assets/media/`, and tries to download only those assets without regenerating HTML.
- Wayback responses are inconsistent; some archived captures may still fail to parse.
- The script retries transient network failures, but very large exports can still take a while.
- The Internet Archive may throttle aggressive traffic. If necessary, rerun the command later.

## License

MIT. See [LICENSE](LICENSE).

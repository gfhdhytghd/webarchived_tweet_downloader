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

Example:

```bash
python download_archive.py AnIncandescence
```

The script always writes the base archive to `<username>_archive.html`.

## Output Layout

For a user like `AnIncandescence`, the downloader writes:

- `AnIncandescence_archive.html`
- `AnIncandescence_archive_time_desc.html`
- `AnIncandescence_archive_media_first_time_desc.html`
- `AnIncandescence_archive_text_length_desc.html`
- `AnIncandescence_archive_text_entropy_desc.html`
- `AnIncandescence_archive_assets/archive.css`
- `AnIncandescence_archive_assets/archive.js`
- `AnIncandescence_archive_assets/media/...`
- `AnIncandescence_archive_assets/json/...`

The generated archive pages include:

- responsive mobile-friendly layout
- larger card-style tweet blocks
- clickable / tap-to-zoom images via a built-in lightbox
- lazy-loaded external image assets
- hidden `data-*` reply metadata on each tweet block for later filtering

The hidden metadata currently includes:

- `data-tweet-id`
- `data-conversation-id`
- `data-in-reply-to-status-id`
- `data-in-reply-to-user-id`
- `data-in-reply-to-screen-name`
- `data-is-reply`
- `data-is-thread-root`
- `data-json-path`

Each JSON file is stored as one archived response per tweet capture, named with the capture timestamp and tweet id.

## What The Script Does

1. Calls the Wayback CDX API with a tweet URL prefix for the target account.
2. Deduplicates captures by content digest when listing snapshots.
3. Downloads archived tweet payloads with retry and backoff.
4. Extracts text from multiple Twitter response formats, including legacy and newer nested structures.
5. Saves media, CSS, JS, and raw JSON into a shared asset folder.
6. Writes the main chronological archive plus four sorted HTML variants.

## Notes

- The output is no longer self-contained in a single file; keep the HTML files together with the generated `*_archive_assets/` directory.
- Wayback responses are inconsistent; some archived captures may still fail to parse.
- The script retries transient network failures, but very large exports can still take a while.
- The Internet Archive may throttle aggressive traffic. If necessary, rerun the command later.

## License

MIT. See [LICENSE](LICENSE).

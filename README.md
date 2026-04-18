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

Regenerate HTML from local JSON snapshots without any network-backed repair:

```bash
python download_archive.py <twitter_username> --html-from-json --no-any-repair
```

Repair only missing local media assets from existing JSON snapshots:

```bash
python download_archive.py <twitter_username> --repair-media
```

Repair missing media without trying Wayback media fallback:

```bash
python download_archive.py <twitter_username> --repair-media --x-api-only
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

Fetch the user's own X API timeline into local JSON before rendering:

```bash
python download_archive.py <twitter_username> --x-api-timeline --env-file ./.env
```

Fetch all available X API timeline pages instead of the default first page:

```bash
python download_archive.py <twitter_username> --x-api-timeline --x-api-timeline-pages 0 --env-file ./.env
```

Fetch conversation replies for local root posts through X API search:

```bash
python download_archive.py <twitter_username> --x-api-conversations --x-api-conversation-search all --env-file ./.env
```

Disable Wayback fallback during reply-chain enrichment:

```bash
python download_archive.py <twitter_username> --repair-reply-chain-depth 8 --x-api-only
```

When a middle parent is unavailable, reply-chain repair also tries to recover earlier visible context from local posts in the same `conversation_id` that are older than the known child reply. The single root post identified by `conversation_id` may still use Wayback fallback, even with `--x-api-only`, because it is an explicit one-post lookup rather than a broad missing-parent fallback.

Use a non-default dotenv file:

```bash
python download_archive.py <twitter_username> --repair-reply-chain-depth 8 --env-file ./private.env
```

Try multiple dotenv files in order. This is useful when different authenticated X accounts can see different protected authors:

```bash
python download_archive.py <twitter_username> --repair-reply-chain-depth 8 --env-file ./a.env --env-file ./b.env
```

Tune X API pacing if you are close to rate limits:

```bash
python download_archive.py <twitter_username> --x-api-timeline --x-api-min-interval 2.5 --env-file ./.env
```

Repair reply chains for every local archive, using X API for ordinary missing parents and then rebuilding HTML:

```bash
for d in *_archive; do
  u="${d%_archive}"
  [ -d "$d/${u}_archive_assets/json" ] || continue
  python download_archive.py "$u" --repair-reply-chain-depth 64 --env-file ./.env --workers 4 --x-api-only
  python download_archive.py "$u" --html-from-json --env-file ./.env --workers 4
done
```

Examples:

```bash
python download_archive.py AnIncandescence
python download_archive.py AnIncandescence --json-only --workers 24
python download_archive.py AnIncandescence --json-only --reverse-resume
python download_archive.py AnIncandescence --html-from-json
python download_archive.py AnIncandescence --html-from-json --x-api-reply-chain-depth 8
python download_archive.py AnIncandescence --x-api-timeline --x-api-timeline-pages 0 --env-file ./.env
python download_archive.py AnIncandescence --x-api-conversations --x-api-conversation-search all --env-file ./.env
python download_archive.py AnIncandescence --repair-reply-chain-depth 8 --x-api-only
python download_archive.py AnIncandescence --repair-reply-chain-depth 8
python download_archive.py AnIncandescence --html-from-json --reverse-resume
python download_archive.py AnIncandescence --html-from-json --no-any-repair
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
- `AnIncandescence_archive/AnIncandescence_archive_assets/media_negative_index.json`
- `AnIncandescence_archive/AnIncandescence_archive_assets/x_api_access_index.json`

The generated archive pages include:

- responsive mobile-friendly layout
- larger card-style tweet blocks
- clickable / tap-to-zoom images via a built-in lightbox
- lazy-loaded external image assets
- built-in tabs for posts/replies plus toggles for replies, reposts, expanded comments, and reply chains
- clickable detail panel with reconstructed comment sections when local reply data is available
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
- `media_negative_index.json`: cache of hard-missing media URLs such as repeated HTTP 404/410 responses
- `x_api_access_index.json`: per-credential access summary for X API successes/failures by account and tweet id

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
- `--html-from-json --no-any-repair` rebuilds HTML from local JSON without network-backed repair or lookup. It does not fetch missing media, X API reply-chain data, or Wayback fallback payloads; only local JSON and already cached local media files are used. The older `--no-media-repair` spelling is still accepted as a compatibility alias.
- `.env` is loaded automatically before X API credentials are read. Use `--env-file` repeatedly to try multiple credential sets in order. See `.env.schema` for the supported variables. Map the X Developer labels as: `Customer/Consumer Key` -> `X_API_KEY`, `Customer/Consumer Key Secret` -> `X_API_KEY_SECRET`, `OAuth 1.0a Access Token` -> `X_ACCESS_TOKEN`, and `OAuth 1.0a Access Token Secret` -> `X_ACCESS_TOKEN_SECRET`. Complete OAuth 1.0a user-context credentials are preferred for protected posts visible to that user. `X_BEARER_TOKEN` / `TWITTER_BEARER_TOKEN` remains supported for app-only lookups.
- `--x-api-min-interval SECONDS` applies a process-wide minimum delay between X API requests across workers. It defaults to `X_API_MIN_INTERVAL_SECONDS` from the environment, or `1.2` seconds. HTTP 429 responses honor `Retry-After` and `x-rate-limit-reset` when present.
- `--x-api-reply-chain-depth N` uses X API v2 Post lookup to recursively fetch missing replied-to parent posts up to `N` levels while rendering HTML. Fetched reply-chain payloads are persisted into each snapshot JSON under `_pale_fire.x_api_reply_chains`, so later `--html-from-json` runs can render the cached chain without a token. Without X API credentials, lookup still tries matching local JSON snapshots and Wayback CDX captures.
- `--repair-reply-chain-depth N` does the same reply-chain enrichment as a repair-only stage: it scans local JSON snapshots, writes `_pale_fire.x_api_reply_chains`, and does not regenerate HTML. Existing complete caches are reused, but cached `terminal_error` failures from older runs are retried through local JSON, OAuth 1.0a / bearer X API when configured, and Wayback fallback. The misspelled alias `--repair-reply-chain-deepth` is also accepted.
- `--x-api-timeline` resolves the target user through X API and writes `/users/:id/tweets` results into local snapshot JSON before rendering. `--x-api-timeline-pages N` caps pagination; `0` means keep paging until X API stops returning `next_token`. `--x-api-timeline-page-size N` is clamped to the X API 5-100 tweet range.
- `--x-api-conversations` scans local root posts and searches `conversation_id:<id> -is:retweet` through X API, then writes returned replies into local snapshot JSON. `--x-api-conversation-search all` uses full-archive search when the X project has access; `recent` uses recent search. `--x-api-conversation-pages 0` keeps paging until exhausted. Search results still depend on X API indexing, plan access, and account visibility; it cannot guarantee a complete X-wide comment section.
- `--x-api-only` / `--no-wayback-fallback` can be combined with `--x-api-reply-chain-depth`, `--repair-reply-chain-depth`, `--x-api-timeline`, `--x-api-conversations`, or `--repair-media`. For reply-chain work it disables Wayback fallback for ordinary missing-parent lookups, while local JSON matches are still reused. If a chain breaks, the repair can recover older local posts from the same `conversation_id` by timestamp, and the single root post identified by `conversation_id` is still allowed to use Wayback fallback. For `--repair-media`, it disables Wayback media fallback and uses only local cache plus the original media URLs already stored in JSON.
- X API payloads written by timeline and conversation repair are normal snapshot JSON files with `_pale_fire.source` metadata. The script also records which env/auth entry succeeded or failed in `x_api_access_index.json`, keyed by the env-file label or credential hash.
- HTML comment sections are reconstructed from the local archive by scanning replies whose `in_reply_to_status_id` points at the current post or another visible local comment. Replied-to parent posts found in cached/repaired reply chains are also injected as lightweight comment nodes, so author replies to another user's comment can appear under that comment. Nested comment replies are rendered as threaded subtrees; if an intermediate parent is missing but the conversation root is present, the reply is attached to the root so it remains visible. This is not a complete X-wide comment section unless those replies are present in the local JSON archive or in cached reply-chain data. Replied-to ancestors are shown separately as `上级回复链`.
- `--repair-media` scans local snapshot JSON files, finds media that is still missing from `<username>_archive/*_archive_assets/media/`, and tries to download only those assets without regenerating HTML. It reuses local media URLs for the same `media_key`, can refresh stale top-level or referenced-tweet media URLs from X API when credentials are present, writes hard failures to `media_negative_index.json`, and renders explicit `[图片媒体不可用]` / `[视频媒体不可用]` placeholders for media that cannot be fetched.
- Wayback responses are inconsistent; some archived captures may still fail to parse.
- The script retries transient network failures, but very large exports can still take a while.
- The Internet Archive may throttle aggressive traffic. If necessary, rerun the command later.

## License

MIT. See [LICENSE](LICENSE).

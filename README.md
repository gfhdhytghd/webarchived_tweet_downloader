# webarchived_tweet_downloader

Download archived tweets for a Twitter/X account from the Internet Archive Wayback Machine and export them into a single self-contained HTML file.

The exporter:

- queries the Wayback CDX API for archived tweet snapshots
- fetches archived tweet JSON payloads
- extracts tweet text from several Twitter payload shapes
- downloads tweet images and embeds them into the output HTML as `data:` URLs
- writes a mobile-friendly archive page with built-in image tap-to-zoom

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
python download_archive.py <twitter_username> [output_html]
```

Example:

```bash
python download_archive.py AnIncandescence anincandescence_archive.html
```

If `output_html` is omitted, the script writes to `<username>_archive.html`.

The generated HTML now includes:

- responsive mobile-friendly layout
- larger card-style tweet blocks
- clickable / tap-to-zoom images via a built-in lightbox
- lazy-loaded embedded images

## Reordering An Existing Archive

You can also reorder an already-generated archive HTML without downloading data again:

```bash
python sort_archive_html.py path/to/account_archive.html
```

This writes four sibling files next to the input:

- `*_time_desc.html`
- `*_media_first_time_desc.html`
- `*_text_length_desc.html`
- `*_text_entropy_desc.html`

The sorter preserves the existing HTML structure and only changes tweet order.

## What The Script Does

1. Calls the Wayback CDX API with a tweet URL prefix for the target account.
2. Deduplicates captures by content digest when listing snapshots.
3. Downloads archived tweet payloads with retry and backoff.
4. Extracts text from multiple Twitter response formats, including legacy and newer nested structures.
5. Downloads media images and embeds them directly into the generated HTML.

## Notes

- The exported HTML can become large because images are embedded directly.
- Wayback responses are inconsistent; some archived captures may still fail to parse.
- The script retries transient network failures, but very large exports can still take a while.
- The Internet Archive may throttle aggressive traffic. If necessary, rerun the command later.

## Output

The generated HTML is self-contained:

- tweet text is included inline
- images are embedded as `data:image/...` URLs
- links back to the original tweet URL are preserved

That makes the result easy to move around without carrying a separate media folder.

## License

MIT. See [LICENSE](LICENSE).

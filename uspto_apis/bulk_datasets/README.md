# USPTO Bulk Datasets API

Scripts for discovering and downloading USPTO bulk files from product
catalogs such as `PTGRXML` and `APPXML`.

The implementation uses:

- one shared core downloader: `bulk_download.py`
- two thin product wrappers:
  - `download_ptgrxml.py`
  - `download_appxml.py`

Each run follows the same workflow:

1. Request the current file inventory from
   `https://api.uspto.gov/api/v1/datasets/products/<PRODUCT>`.
2. Download files serially with soft retry/backoff and throttling.
3. Track progress via manifest + checkpoint files for safe resume.

## Setup

Export your API key once per shell session:

```bash
export USPTO_API_KEY='your-api-key-here'
```

## Catalog request example

For `PTGRXML`, the list request is:

```bash
curl -sS 'https://api.uspto.gov/api/v1/datasets/products/PTGRXML' \
  -H "X-API-KEY: ${USPTO_API_KEY}"
```

The same pattern applies to `APPXML` by replacing the product code in the URL.

## Run the downloaders

Download `PTGRXML`:

```bash
python uspto_apis/bulk_datasets/download_ptgrxml.py
```

Download `APPXML`:

```bash
python uspto_apis/bulk_datasets/download_appxml.py
```

Both wrappers accept the same options from `bulk_download.py`, including:

- `--output-dir` destination directory for files,
- `--throttle` sleep between successful downloads (default `1.5` seconds),
- `--catalog-retries` and `--download-retries`,
- `--base-backoff` for exponential retry delay,
- `--max-files` for test runs.

## Recovery and tracking files

For each product run, the downloader writes:

- a catalog manifest JSONL (`<output-dir>/<product>_catalog.jsonl`),
- a checkpoint JSONL (`<output-dir>/<product>_checkpoint.jsonl`),
- a log file (`<output-dir>/download_<product>.log`).

Behavior on rerun:

- existing files with matching size are skipped,
- files marked done but missing/mismatched are re-downloaded,
- failed files are retried on subsequent runs,
- downloads stay soft (serial, throttled, and retry-aware).

## Example output layout

After running `download_ptgrxml.py` with default settings:

```text
ptgrxml/
  pg020101.zip
  pg020108.zip
  ...
  ptgrxml_catalog.jsonl
  ptgrxml_checkpoint.jsonl
  download_ptgrxml.log
```

After running `download_appxml.py` with default settings:

```text
appxml/
  pa220104.zip
  pa220111.zip
  ...
  appxml_catalog.jsonl
  appxml_checkpoint.jsonl
  download_appxml.log
```

## Tips

- Keep `--throttle` above `0` to avoid hammering the download endpoint.
- Use `--max-files` first when validating a new environment.
- If you need quieter output in automation, pass `--quiet`.
- If you need to disable file logging, pass `--log-file ''`.

## Reference

- Open Data Portal home: https://data.uspto.gov/
- Swagger UI: https://data.uspto.gov/swagger/index.html

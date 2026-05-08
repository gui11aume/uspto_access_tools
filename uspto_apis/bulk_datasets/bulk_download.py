#!/usr/bin/env python3
"""Shared soft-downloader for USPTO bulk dataset products.

This module contains the reusable logic for product catalogs like PTGRXML and
APPXML:

1. Fetch product file inventory from `/datasets/products/<PRODUCT>`.
2. Download listed files serially with conservative throttling.
3. Track progress in a checkpoint JSONL so reruns can recover cleanly.

Thin product-specific wrappers should call `main_for_product(...)`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, cast

import requests

CHUNK_SIZE = 1024 * 1024  # 1 MiB streamed write chunks.
API_ROOT = "https://api.uspto.gov/api/v1/datasets/products"

logger = logging.getLogger("bulk_download")


class UsptoApiError(RuntimeError):
    """Non-retryable HTTP failure from a USPTO API call."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        super().__init__(detail)


@dataclass(frozen=True)
class FileRecord:
    """One downloadable file entry from a product catalog."""

    file_name: str
    url: str
    file_size: int | None
    release_date: str | None
    last_modified: str | None


def catalog_url_for_product(product: str) -> str:
    """Return product inventory URL for a catalog code (e.g. PTGRXML)."""
    return f"{API_ROOT}/{product}"


def parse_args_for_product(
    product: str,
    default_output_dir: str,
    argv: list[str] | None = None,
) -> argparse.Namespace:
    """Build a shared CLI for any product wrapper."""
    p = argparse.ArgumentParser(
        description=(
            f"Fetch {product} file catalog then download softly "
            "(serial, throttled, retry/backoff, checkpointed)."
        )
    )
    p.add_argument(
        "--output-dir",
        default=default_output_dir,
        help="destination directory for downloaded files",
    )
    p.add_argument(
        "--manifest",
        default=None,
        help=(
            "optional catalog JSONL output (default: "
            f"<output-dir>/{product.lower()}_catalog.jsonl)"
        ),
    )
    p.add_argument(
        "--checkpoint",
        default=None,
        help=(
            "optional checkpoint JSONL path (default: "
            f"<output-dir>/{product.lower()}_checkpoint.jsonl)"
        ),
    )
    p.add_argument(
        "--throttle",
        type=float,
        default=1.5,
        help="seconds to sleep after each successful file download",
    )
    p.add_argument(
        "--catalog-retries",
        type=int,
        default=8,
        help="max retries for catalog request on transient failures",
    )
    p.add_argument(
        "--download-retries",
        type=int,
        default=6,
        help="max retries per file on transient failures",
    )
    p.add_argument(
        "--base-backoff",
        type=float,
        default=1.0,
        help="base sleep (seconds) for exponential backoff",
    )
    p.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="optional cap for testing (process first N files only)",
    )
    p.add_argument(
        "--log-file",
        default=None,
        help=(
            "log file path (default: <output-dir>/download_<product>.log). "
            "Pass an empty string (--log-file '') to disable file logging."
        ),
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="suppress console logs (file logging unaffected)",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def configure_logging(
    output_dir: Path,
    product: str,
    log_file: str | None,
    quiet: bool,
    verbose: bool,
) -> Path | None:
    """Configure file logging by default plus optional console logging."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    if log_file is None:
        log_path: Path | None = output_dir / f"download_{product.lower()}.log"
    elif log_file == "":
        log_path = None
    else:
        log_path = Path(log_file)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(level)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(fmt)
        file_handler.setLevel(level)
        root.addHandler(file_handler)

    if not quiet:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(fmt)
        stream_handler.setLevel(level)
        root.addHandler(stream_handler)

    return log_path


def compute_backoff(attempt: int, base_sleep: float) -> float:
    """Exponential backoff with small jitter to desynchronize retries."""
    jitter = random.uniform(0.0, 0.25)
    return base_sleep * (2**attempt) + jitter


def should_retry_status(status_code: int) -> bool:
    """Return True for transient server/rate-limit statuses."""
    return status_code == 429 or 500 <= status_code < 600


def get_json_with_retry(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    *,
    max_retries: int,
    base_sleep: float,
    timeout: float = 120.0,
) -> dict[str, Any]:
    """GET JSON from `url` with retry/backoff for transient errors."""
    last_status: int | None = None
    last_text = ""
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            wait = compute_backoff(attempt, base_sleep)
            logger.warning(
                "catalog request network error %s; sleeping %.2fs",
                exc,
                wait,
            )
            time.sleep(wait)
            continue

        last_status = response.status_code
        last_text = response.text[:500]
        if response.status_code == 200:
            return response.json()
        if should_retry_status(response.status_code):
            wait = compute_backoff(attempt, base_sleep)
            logger.info(
                "catalog request HTTP %d; sleeping %.2fs (attempt %d/%d)",
                response.status_code,
                wait,
                attempt + 1,
                max_retries,
            )
            time.sleep(wait)
            continue
        raise UsptoApiError(
            response.status_code,
            ("catalog request failed: " f"{response.status_code}: {last_text}"),
        )

    raise RuntimeError(
        "catalog request exceeded retries " f"(last status {last_status}): {last_text}"
    )


def stream_download_with_retry(
    session: requests.Session,
    record: FileRecord,
    destination: Path,
    headers: dict[str, str],
    *,
    max_retries: int,
    base_sleep: float,
    timeout: float = 300.0,
) -> None:
    """Download one file with retry/backoff and atomic finalize."""
    temp_path = destination.with_suffix(destination.suffix + ".part")
    if temp_path.exists():
        temp_path.unlink(missing_ok=True)

    last_status: int | None = None
    last_text = ""
    for attempt in range(max_retries):
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        bytes_written = 0
        try:
            with session.get(
                record.url,
                headers=headers,
                stream=True,
                timeout=timeout,
                allow_redirects=True,
            ) as response:
                last_status = response.status_code
                if response.status_code != 200:
                    last_text = response.text[:500]
                else:
                    last_text = ""
                if response.status_code != 200:
                    if should_retry_status(response.status_code):
                        wait = compute_backoff(attempt, base_sleep)
                        logger.info(
                            ("download %s HTTP %d; sleeping %.2fs " "(attempt %d/%d)"),
                            record.file_name,
                            response.status_code,
                            wait,
                            attempt + 1,
                            max_retries,
                        )
                        time.sleep(wait)
                        continue
                    raise UsptoApiError(
                        response.status_code,
                        f"download failed for {record.file_name}: "
                        f"{response.status_code}: {last_text}",
                    )

                with temp_path.open("wb") as out:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue
                        out.write(chunk)
                        bytes_written += len(chunk)

            if record.file_size is not None and bytes_written != record.file_size:
                wait = compute_backoff(attempt, base_sleep)
                logger.warning(
                    ("size mismatch for %s (expected=%d got=%d); " "retrying in %.2fs"),
                    record.file_name,
                    record.file_size,
                    bytes_written,
                    wait,
                )
                time.sleep(wait)
                continue

            temp_path.replace(destination)
            return
        except requests.RequestException as exc:
            wait = compute_backoff(attempt, base_sleep)
            logger.warning(
                "download %s network error %s; sleeping %.2fs",
                record.file_name,
                exc,
                wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"download retries exhausted for {record.file_name} "
        f"(last status {last_status}: {last_text})"
    )


def walk_json(value: Any) -> Iterable[dict[str, Any]]:
    """Depth-first walk over each dictionary nested in `value`."""
    if isinstance(value, dict):
        node = cast(dict[str, Any], value)
        yield node
        for nested in node.values():
            yield from walk_json(nested)
        return
    if isinstance(value, list):
        items = cast(list[Any], value)
        for item in items:
            yield from walk_json(item)


def extract_file_records(payload: dict[str, Any]) -> list[FileRecord]:
    """Extract unique file rows from a product payload.

    The shape can vary by endpoint version, so we scan for dicts carrying
    both `fileName` and `fileDownloadURI`.
    """
    by_name: dict[str, FileRecord] = {}
    for obj in walk_json(payload):
        file_name = obj.get("fileName")
        uri = obj.get("fileDownloadURI")
        if not file_name or not uri:
            continue
        record = FileRecord(
            file_name=str(file_name),
            url=str(uri),
            file_size=(
                int(obj["fileSize"]) if obj.get("fileSize") is not None else None
            ),
            release_date=(
                str(obj["fileReleaseDate"])
                if obj.get("fileReleaseDate") is not None
                else None
            ),
            last_modified=(
                str(obj["fileLastModifiedDateTime"])
                if obj.get("fileLastModifiedDateTime") is not None
                else None
            ),
        )
        by_name[record.file_name] = record
    records = list(by_name.values())
    records.sort(key=lambda rec: rec.file_name)
    return records


def write_manifest(path: Path, records: list[FileRecord]) -> None:
    """Write resolved catalog records to a JSONL manifest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            row = {
                "fileName": rec.file_name,
                "fileDownloadURI": rec.url,
                "fileSize": rec.file_size,
                "fileReleaseDate": rec.release_date,
                "fileLastModifiedDateTime": rec.last_modified,
            }
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")


def load_checkpoint(path: Path) -> dict[str, str]:
    """Load latest status by file name from checkpoint JSONL.

    The checkpoint is append-only; later lines win. We only need the latest
    status (`done`/`failed`) to decide rerun behavior.
    """
    latest: dict[str, str] = {}
    if not path.exists():
        return latest
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            file_name = record.get("fileName")
            status = record.get("status")
            if isinstance(file_name, str) and isinstance(status, str):
                latest[file_name] = status
    return latest


def append_checkpoint(
    checkpoint_path: Path,
    product: str,
    record: FileRecord,
    status: str,
    *,
    detail: str | None = None,
) -> None:
    """Append one status row for `record` to checkpoint JSONL."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    row: dict[str, Any] = {
        "product": product,
        "fileName": record.file_name,
        "status": status,
        "fileSize": record.file_size,
        "fileDownloadURI": record.url,
    }
    if detail:
        row["detail"] = detail
    with checkpoint_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False))
        f.write("\n")


def should_skip_existing(path: Path, expected_size: int | None) -> bool:
    """Return True if local file is present and appears complete."""
    if not path.exists():
        return False
    if expected_size is None:
        return True
    try:
        return path.stat().st_size == expected_size
    except OSError:
        return False


def run_download_for_product(
    product: str,
    default_output_dir: str,
    argv: list[str] | None = None,
) -> int:
    """Shared top-level run for a product wrapper script."""
    args = parse_args_for_product(product, default_output_dir, argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log_path = configure_logging(
        output_dir,
        product,
        args.log_file,
        args.quiet,
        args.verbose,
    )
    if log_path is not None:
        logger.info("logging to %s", log_path)

    api_key = os.environ.get("USPTO_API_KEY")
    if not api_key:
        logger.error("USPTO API key required: set USPTO_API_KEY in the environment")
        return 2

    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    session = requests.Session()

    catalog_url = catalog_url_for_product(product)
    logger.info("fetch catalog for %s: %s", product, catalog_url)
    payload = get_json_with_retry(
        session,
        catalog_url,
        headers,
        max_retries=args.catalog_retries,
        base_sleep=args.base_backoff,
    )
    records = extract_file_records(payload)
    if not records:
        logger.error(
            "catalog for %s contained no downloadable records",
            product,
        )
        return 1

    logger.info("%s catalog returned %d files", product, len(records))
    manifest_path = (
        Path(args.manifest)
        if args.manifest is not None
        else output_dir / f"{product.lower()}_catalog.jsonl"
    )
    write_manifest(manifest_path, records)
    logger.info("wrote manifest to %s", manifest_path)

    checkpoint_path = (
        Path(args.checkpoint)
        if args.checkpoint is not None
        else output_dir / f"{product.lower()}_checkpoint.jsonl"
    )
    checkpoint_status = load_checkpoint(checkpoint_path)
    done_count = sum(1 for status in checkpoint_status.values() if status == "done")
    logger.info(
        "loaded checkpoint %s (%d done entries)",
        checkpoint_path,
        done_count,
    )

    if args.max_files is not None:
        records = records[: max(args.max_files, 0)]
        logger.info(
            "limiting run to first %d files (--max-files)",
            len(records),
        )

    downloaded = 0
    skipped = 0
    failed = 0

    for index, record in enumerate(records, start=1):
        target = output_dir / record.file_name
        prior_status = checkpoint_status.get(record.file_name)

        if should_skip_existing(target, record.file_size):
            logger.info(
                "[%d/%d] skip existing %s",
                index,
                len(records),
                target.name,
            )
            skipped += 1
            if prior_status != "done":
                append_checkpoint(
                    checkpoint_path,
                    product,
                    record,
                    "done",
                    detail="existing file verified; skipped redownload",
                )
                checkpoint_status[record.file_name] = "done"
            continue

        if prior_status == "done":
            logger.warning(
                (
                    "[%d/%d] checkpoint says done but file missing/mismatched; "
                    "redownloading %s"
                ),
                index,
                len(records),
                record.file_name,
            )
        else:
            logger.info(
                "[%d/%d] download %s",
                index,
                len(records),
                record.file_name,
            )

        try:
            stream_download_with_retry(
                session,
                record,
                target,
                headers,
                max_retries=args.download_retries,
                base_sleep=args.base_backoff,
            )
        except Exception as exc:
            failed += 1
            detail = str(exc)
            logger.error("failed %s: %s", record.file_name, detail)
            append_checkpoint(
                checkpoint_path,
                product,
                record,
                "failed",
                detail=detail,
            )
            checkpoint_status[record.file_name] = "failed"
            # Soft mode: continue to the next file.
            continue

        downloaded += 1
        append_checkpoint(checkpoint_path, product, record, "done")
        checkpoint_status[record.file_name] = "done"
        if args.throttle > 0:
            logger.debug("throttle sleep %.2fs", args.throttle)
            time.sleep(args.throttle)

    logger.info(
        "done product=%s downloaded=%d skipped=%d failed=%d out_dir=%s",
        product,
        downloaded,
        skipped,
        failed,
        output_dir,
    )
    return 0 if failed == 0 else 1


def main_for_product(product: str, default_output_dir: str) -> int:
    """Convenience entry for wrappers executed as scripts."""
    return run_download_for_product(product, default_output_dir, argv=None)

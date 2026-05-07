#!/usr/bin/env python3
"""Sweep the USPTO ODP Patent File Wrapper search endpoint.

The endpoint is backed by Amazon OpenSearch and enforces two hard caps:

* `limit` per request is at most 100 (default 25).
* `offset + limit` must be <= 10_000 (the OpenSearch `max_result_window`),
  so any single query can only surface its first 10_000 hits.
* The gateway may reject responses whose JSON exceeds roughly **6 MB**
  (HTTP 413). Dense windows need a smaller `pagination.limit` even when
  100 rows still fit under the 10k offset cap.

To get every record we slice the query by `applicationMetaData.filingDate`
into windows whose hit count is strictly below 10_000, then paginate each
window 100 records at a time. Windows that exceed the ceiling are bisected
recursively; if a single-day window still overflows it is fanned out across
`applicationTypeLabelName` buckets, which is enough in practice because the
USPTO never receives ten thousand applications of one type on one day.

Usage:

    export USPTO_API_KEY='...'
    python sweep.py --output pfw.jsonl

Reruns are safe: each completed window is recorded in a sidecar checkpoint
file (`<output>.windows.jsonl`) and skipped on the next invocation.

While a window is downloading, records go to a **staging file**
(`<stem>.part.<id>.jsonl` next to `--output`). Only after the window finishes
does that file append to the main JSONL, so an interruption mid-window does
not duplicate lines in the output; the next run resumes from the staging
file (same line count as the next API offset).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator, TextIO

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ODP search endpoint. POST takes a JSON body with the advanced query syntax;
# GET takes the same parameters as URL-encoded fields. We use POST throughout
# because the body is easier to compose programmatically.
API_URL = "https://api.uspto.gov/api/v1/patent/applications/search"

# Maximum page size accepted by `pagination.limit`. The default is 25 but 100
# is the documented upper bound. We start each window at this size and shrink
# on HTTP 413 if the serialized rows exceed the API's ~6 MB response cap.
PAGE_LIMIT = 100

# Smallest `pagination.limit` we attempt after repeated 413 responses.
MIN_PAGE_LIMIT = 1

# OpenSearch enforces `from + size <= index.max_result_window`, with a default
# of 10_000. Any single query (i.e. fixed `q`/filters/range) can therefore
# only surface its first 10_000 hits; deeper offsets fail with
# `Result window is too large`. The whole point of the sweep is to keep every
# slice under this ceiling.
MAX_RESULT_WINDOW = 10_000

# ODP only covers applications filed on or after 2001-01-01 (everything older
# lives in PEDS / bulk dumps), so this is the natural lower bound of a sweep.
DEFAULT_FROM_DATE = "2001-01-01"

# When a single calendar day still has >= 10_000 hits, date bisection has
# nothing left to cut, so we fan it out across application types. The list
# below covers every value of `applicationMetaData.applicationTypeLabelName`
# that ODP exposes; in practice no single (day, type) pair has ever exceeded
# 10_000 records.
APP_TYPE_BUCKETS = [
    "Utility",
    "Design",
    "Plant",
    "Reissue",
    "Provisional",
    "Statutory Invention Registration",
    "Defensive Publication",
]

logger = logging.getLogger("sweep")


class UsptoApiError(RuntimeError):
    """Non-retryable HTTP failure from the search endpoint (4xx except 429)."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        super().__init__(detail)


# ---------------------------------------------------------------------------
# Window: an indivisible unit of work for the sweep
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Window:
    """A contiguous slice of the corpus the sweep can fetch in isolation.

    A `Window` always carries a `[start, end]` filing-date range (both
    inclusive). It optionally also pins an `applicationTypeLabelName`; that
    field is set only when date bisection has bottomed out at a single day
    and we still need to split the slice further by application type.

    The dataclass is frozen so windows are hashable and can be used as
    checkpoint keys via `label()`.
    """

    start: date
    end: date
    app_type: str | None = None

    def label(self) -> str:
        """Stable, human-readable identifier used for checkpointing.

        Two windows with identical bounds and `app_type` produce the same
        label; this is what we look up in the resume set.
        """
        base = f"{self.start.isoformat()}..{self.end.isoformat()}"
        return f"{base}|{self.app_type}" if self.app_type else base


# ---------------------------------------------------------------------------
# Request construction
# ---------------------------------------------------------------------------


def build_body(
    window: Window, *, offset: int, limit: int, q: str | None
) -> dict[str, Any]:
    """Build the JSON body for one POST to `/patent/applications/search`.

    The body always pins:

    * a `rangeFilters` entry on `applicationMetaData.filingDate` matching the
      window bounds, so the result set never exceeds the window even if
      another caller-supplied `q` is broader;
    * a deterministic `sort` on `(filingDate, applicationNumberText)`. The
      secondary key matters: pagination relies on a stable order, and
      multiple applications can share a filing date.

    `app_type`, when set, is added as an exact-match filter on
    `applicationTypeLabelName` so the type-fanout step can reuse the same
    request shape.
    """
    body: dict[str, Any] = {
        # `*` is OpenSearch's match-all; we lean on `rangeFilters` and
        # `filters` to do the actual scoping. The caller can override `q`
        # to scope the sweep to a sub-corpus (e.g. `Nanobody`).
        "q": q if q else "*",
        "rangeFilters": [
            {
                "field": "applicationMetaData.filingDate",
                "valueFrom": window.start.isoformat(),
                "valueTo": window.end.isoformat(),
            }
        ],
        "pagination": {"offset": offset, "limit": limit},
        "sort": [
            {"field": "applicationMetaData.filingDate", "order": "Asc"},
            {"field": "applicationNumberText", "order": "Asc"},
        ],
    }
    if window.app_type:
        body["filters"] = [
            {
                "name": "applicationMetaData.applicationTypeLabelName",
                "value": [window.app_type],
            }
        ]
    return body


# ---------------------------------------------------------------------------
# HTTP layer with retry/back-off
# ---------------------------------------------------------------------------


def post_with_retry(
    session: requests.Session,
    headers: dict[str, str],
    body: dict[str, Any],
    *,
    max_retries: int = 8,
    base_sleep: float = 1.0,
) -> dict[str, Any]:
    """POST `body` to the search endpoint, retrying transient failures.

    The ODP enforces a per-API-key rate limit and answers HTTP 429 when it is
    exceeded. We also retry any 5xx and any low-level network exception.
    Other 4xx errors (400, 401, 403, ...) are surfaced as `RuntimeError`
    immediately because they will not heal on retry.

    Back-off is exponential (`base_sleep * 2**attempt`) which doubles the
    wait between consecutive retries up to a per-call ceiling defined by
    `max_retries`.
    """
    last_status: int | None = None
    last_text = ""
    for attempt in range(max_retries):
        try:
            r = session.post(API_URL, headers=headers, json=body, timeout=120)
        except requests.RequestException as exc:
            # Transport-level error (DNS, connection reset, read timeout, ...).
            # These are almost always worth retrying.
            wait = base_sleep * (2**attempt)
            logger.warning("network error %s; sleeping %.1fs", exc, wait)
            time.sleep(wait)
            continue

        last_status = r.status_code
        last_text = r.text[:500]

        if r.status_code == 200:
            return r.json()
        if r.status_code == 429 or 500 <= r.status_code < 600:
            # 429 = rate limit hit; 5xx = transient server-side error.
            wait = base_sleep * (2**attempt)
            logger.info(
                "HTTP %d; sleeping %.1fs (attempt %d/%d)",
                r.status_code,
                wait,
                attempt + 1,
                max_retries,
            )
            time.sleep(wait)
            continue
        # Any other 4xx (bad request, auth failure, forbidden, ...) means the
        # request itself is wrong; retrying is pointless.
        raise UsptoApiError(
            r.status_code, f"USPTO API error {r.status_code}: {last_text}"
        )

    raise RuntimeError(f"exceeded max retries (last status {last_status}): {last_text}")


# ---------------------------------------------------------------------------
# Planning: turn a date range into windows that each fit under 10_000 hits
# ---------------------------------------------------------------------------


def get_total(response: dict[str, Any]) -> int:
    """Read the total hit count from a search response.

    Current ODP responses report it as `count`; some older snapshots and
    third-party docs use `totalNumFound`. Both keys are accepted, with a
    final fallback to the length of the data bag (in case the API ever drops
    the count field entirely).
    """
    for key in ("count", "totalNumFound"):
        if key in response and response[key] is not None:
            return int(response[key])
    return len(response.get("patentFileWrapperDataBag") or [])


def count_window(
    session: requests.Session,
    headers: dict[str, str],
    window: Window,
    q: str | None,
) -> int:
    """Cheap probe that returns just the hit count for `window`.

    We send `limit=1` to keep the response small; we only care about the
    `count` field, not the actual record. This is the call that drives the
    planning recursion.
    """
    body = build_body(window, offset=0, limit=1, q=q)
    return get_total(post_with_retry(session, headers, body))


def split_window(window: Window) -> list[Window]:
    """Return the children of `window` in the bisection tree.

    The split strategy is:

    1. If the window spans more than one day, halve the date range. The two
       children inherit the parent's `app_type` (so type-pinned windows stay
       type-pinned as we drill down by date).
    2. If the window is already a single day with no `app_type`, fan it out
       across `APP_TYPE_BUCKETS`. This is rarely needed in practice but
       handles edge days where one calendar day has more than 10_000 mixed
       filings.
    3. If we are already at a single day pinned to one application type, we
       have run out of axes to split on; return an empty list and let the
       caller handle the truncation.
    """
    if window.start < window.end:
        span_days = (window.end - window.start).days
        mid = window.start + timedelta(days=span_days // 2)
        return [
            Window(window.start, mid, window.app_type),
            # `mid + 1` keeps the two halves disjoint so we never see a
            # record twice across sibling windows.
            Window(mid + timedelta(days=1), window.end, window.app_type),
        ]
    if window.app_type is None:
        return [Window(window.start, window.end, t) for t in APP_TYPE_BUCKETS]
    return []


def plan_windows(
    session: requests.Session,
    headers: dict[str, str],
    root: Window,
    q: str | None,
) -> Iterator[tuple[Window, int]]:
    """Yield `(window, hit_count)` pairs ready for fetching.

    Implements the bisection in an iterative DFS to avoid Python's recursion
    limit on long sweeps. For each candidate window we count hits and then:

    * skip empty windows (count == 0);
    * yield windows that fit (count < 10_000), so the caller can paginate
      them in full;
    * otherwise call `split_window` and push the children back on the stack
      to be re-evaluated.

    Note we use `total < MAX_RESULT_WINDOW` (strict less-than) because at
    exactly 10_000 the last page would request `offset=9900, limit=100`
    which still hits the ceiling.
    """
    stack: list[Window] = [root]
    while stack:
        win = stack.pop()
        total = count_window(session, headers, win, q)
        logger.info("plan %s -> %d hits", win.label(), total)
        if total == 0:
            continue
        if total < MAX_RESULT_WINDOW:
            yield win, total
            continue
        children = split_window(win)
        if not children:
            # A single (day, type) tuple still has 10_000+ records. We log
            # loudly and pull the first 10_000 anyway so the rest of the
            # sweep can complete; the user can rescope the missing tail
            # with another query later (e.g. by `groupArtUnitNumber`).
            logger.error(
                "window %s has %d hits but cannot be split further; "
                "results will be truncated to %d",
                win.label(),
                total,
                MAX_RESULT_WINDOW,
            )
            yield win, MAX_RESULT_WINDOW
            continue
        # `reversed` so children come out of the stack in left-to-right
        # (chronological) order on the next iteration. Cosmetic, but it
        # makes the logs read in time order.
        stack.extend(reversed(children))


# ---------------------------------------------------------------------------
# Fetching: walk a single window page by page
# ---------------------------------------------------------------------------


def staging_path_for_window(output_path: Path, window_label: str) -> Path:
    """Sidecar path used until a window is fully fetched and merged into output."""
    digest = hashlib.sha256(window_label.encode("utf-8")).hexdigest()[:16]
    return output_path.with_name(
        f"{output_path.stem}.part.{digest}{output_path.suffix}"
    )


def count_jsonl_lines(path: Path) -> int:
    """Return the number of newline-terminated rows in a JSONL file."""
    n = 0
    with path.open("rb") as f:
        for _ in f:
            n += 1
    return n


def append_staging_to_output(staging: Path, output_path: Path) -> int:
    """Append all bytes from `staging` to `output_path` and fsync (staging kept).

    Returns how many lines were appended (counted before copy). The caller
    should record the checkpoint, then delete `staging`.
    """
    lines = count_jsonl_lines(staging)
    with output_path.open("ab") as out, staging.open("rb") as src:
        shutil.copyfileobj(src, out)
        out.flush()
        try:
            os.fsync(out.fileno())
        except OSError:
            pass
    return lines


def fetch_window(
    session: requests.Session,
    headers: dict[str, str],
    window: Window,
    total: int,
    q: str | None,
    out: TextIO,
    throttle: float,
    *,
    start_offset: int = 0,
) -> int:
    """Paginate `window` to exhaustion and write each row to `out` as JSONL.

    The caller already knows `total` from the planning pass, so we can size
    the loop precisely instead of probing for the end. We also break early
    if the API returns a short page (defensive: if the index shifts under
    us we stop rather than loop forever).

    `start_offset` resumes pagination after an interrupted run: it must equal
    the number of complete JSONL lines already written to `out` for this
    window (same as the next OpenSearch ``from`` offset).

    Returns the number of records written **in this call** (not including
    rows implied by ``start_offset``).
    """
    written = 0
    # `total` is the count we observed during planning; it should be below
    # 10_000 already, but we clamp again as a belt-and-braces guard against
    # the truncation case in `plan_windows`.
    target = min(total, MAX_RESULT_WINDOW)
    if start_offset >= target:
        out.flush()
        return 0
    page_limit = PAGE_LIMIT
    offset = start_offset
    while offset < target:
        remaining = target - offset
        limit = min(page_limit, remaining)
        while True:
            body = build_body(window, offset=offset, limit=limit, q=q)
            try:
                data = post_with_retry(session, headers, body)
            except UsptoApiError as exc:
                if exc.status_code != 413:
                    raise
                if limit <= MIN_PAGE_LIMIT:
                    raise UsptoApiError(
                        413,
                        f"response still exceeds size cap at offset={offset} "
                        f"limit={limit}; cannot shrink further",
                    ) from exc
                new_limit = max(limit // 2, MIN_PAGE_LIMIT)
                logger.warning(
                    "HTTP 413 payload too large at offset=%d limit=%d; "
                    "retrying with limit=%d",
                    offset,
                    limit,
                    new_limit,
                )
                limit = new_limit
                page_limit = new_limit
                continue
            break

        rows: list[Any] = data.get("patentFileWrapperDataBag") or []
        for row in rows:
            # JSONL: one record per line, UTF-8 preserved as-is.
            out.write(json.dumps(row, ensure_ascii=False))
            out.write("\n")
        written += len(rows)
        if throttle:
            time.sleep(throttle)
        if len(rows) < limit:
            # Either the index changed mid-sweep or `total` was an overcount.
            # Either way, no point asking for offsets we know are empty.
            break
        offset += len(rows)
    out.flush()
    return written


# ---------------------------------------------------------------------------
# Checkpoint file: lets us resume an interrupted sweep
# ---------------------------------------------------------------------------


def load_checkpoint(path: Path) -> set[str]:
    """Read previously-completed window labels from `path`.

    The checkpoint is a JSONL file we append to as windows complete; on a
    rerun we load it and skip every window whose label is already present.
    Lines that fail to parse (e.g. partially-flushed crashes) are ignored.
    """
    done: set[str] = set()
    if not path.exists():
        return done
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                # A torn write at process kill time can leave one bad line;
                # drop it rather than aborting the whole resume.
                continue
            if rec.get("status") == "done" and "window" in rec:
                done.add(rec["window"])
    return done


def append_checkpoint(path: Path, window: str, written: int) -> None:
    """Record a completed window: `written` is how many lines were appended to the main JSONL."""
    with path.open("a") as f:
        f.write(json.dumps({"window": window, "status": "done", "written": written}))
        f.write("\n")


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------


def daterange_chunks(
    start: date, end: date, step_days: int
) -> Iterable[tuple[date, date]]:
    """Yield contiguous, non-overlapping `[chunk_start, chunk_end]` chunks.

    Each chunk covers up to `step_days` calendar days, both bounds
    inclusive. The last chunk may be shorter if it would otherwise spill
    past `end`. This is the *initial* slicing that feeds `plan_windows`;
    bisection takes over from there for any chunk that's too dense.
    """
    if start > end:
        return
    cur = start
    # `step_days - 1` because both bounds are inclusive: a 30-day chunk
    # spans `cur ... cur + 29`, not `cur + 30`.
    step = timedelta(days=max(step_days - 1, 0))
    while cur <= end:
        chunk_end = min(cur + step, end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


# ---------------------------------------------------------------------------
# CLI / entry point
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Define and parse the command-line interface."""
    p = argparse.ArgumentParser(
        description=(
            "Download every USPTO ODP Patent File Wrapper bibliographic "
            "record by sweeping filingDate windows under the OpenSearch "
            "10k result-window ceiling."
        )
    )
    p.add_argument(
        "--from-date",
        default=DEFAULT_FROM_DATE,
        help="inclusive start filing date (YYYY-MM-DD)",
    )
    p.add_argument(
        "--to-date",
        default=date.today().isoformat(),
        help="inclusive end filing date (YYYY-MM-DD)",
    )
    p.add_argument(
        "--output",
        default="patent_file_wrapper.jsonl",
        help="JSONL output path (records are appended)",
    )
    p.add_argument(
        "--checkpoint",
        default=None,
        help="checkpoint path (default: <output>.windows.jsonl)",
    )
    p.add_argument(
        "--query",
        default=None,
        help='optional `q` to scope the sweep, e.g. "Nanobody"',
    )
    p.add_argument(
        "--initial-step-days",
        type=int,
        default=30,
        help="initial slice size in days before bisection kicks in",
    )
    p.add_argument(
        "--throttle",
        type=float,
        default=0.0,
        help="extra sleep (s) between successful page fetches",
    )
    p.add_argument(
        "--log-file",
        default=None,
        help=(
            "log file path (default: <output>.log). Pass an empty string "
            "(--log-file '') to disable file logging."
        ),
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="suppress console logs (file logging is unaffected)",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def configure_logging(
    output: Path,
    log_file: str | None,
    quiet: bool,
    verbose: bool,
) -> Path | None:
    """Wire up logging to a file by default, plus an optional console mirror.

    By default we send every log record to a file next to the JSONL output
    (e.g. `pfw.jsonl` -> `pfw.log`) so a long-running sweep always leaves a
    durable trail even if the terminal scrolls away or the SSH session
    drops. The user can override the path with `--log-file PATH`, disable
    file logging entirely with `--log-file ''`, and silence the terminal
    mirror with `--quiet`.

    Returns the resolved log-file path, or `None` if file logging was
    disabled.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # Resolve the target log path. `None` means "use the default", an empty
    # string means "no file logging at all".
    if log_file is None:
        log_path: Path | None = output.with_suffix(output.suffix + ".log")
    elif log_file == "":
        log_path = None
    else:
        log_path = Path(log_file)

    # Reset any previously-configured handlers so reruns inside a long-lived
    # process (tests, notebooks) don't accumulate duplicates.
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


def main(argv: list[str] | None = None) -> int:
    """Top-level orchestration of the sweep.

    The control flow is:

    1. Parse CLI args, configure file + console logging, validate the API
       key and the date range.
    2. Load the checkpoint so previously-completed windows are skipped.
    3. Walk `daterange_chunks(...)` to get coarse, fixed-size initial
       chunks (default 30 days each).
    4. For each chunk, ask `plan_windows` to bisect it down to fetchable
       leaves, then call `fetch_window` on every leaf.
    5. After each leaf, append staging into the output JSONL and append a
       checkpoint line; interruptions lose at most in-flight HTTP work for the
       current window (staging resumes without duplicating main-file lines).
    """
    args = parse_args(argv)

    # `--output` lives next to the log file by default, so we resolve its
    # parent directory before `configure_logging` can write its first line.
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    log_path = configure_logging(out_path, args.log_file, args.quiet, args.verbose)
    if log_path is not None:
        logger.info("logging to %s", log_path)

    # The API key MUST come from the environment so it never lands in shell
    # history, process listings (`ps`), CI logs, or argparse's `--help` output.
    api_key = os.environ.get("USPTO_API_KEY")
    if not api_key:
        logger.error(
            "USPTO API key required: set the USPTO_API_KEY environment variable"
        )
        return 2

    try:
        start = date.fromisoformat(args.from_date)
        end = date.fromisoformat(args.to_date)
    except ValueError as exc:
        logger.error("invalid date: %s", exc)
        return 2
    if start > end:
        logger.error("--from-date must be on or before --to-date")
        return 2

    # Default checkpoint sits next to the output file. Keeping them paired
    # makes it obvious which checkpoint belongs to which dataset.
    ckpt_path = Path(args.checkpoint or f"{args.output}.windows.jsonl")
    done = load_checkpoint(ckpt_path)
    logger.info("loaded %d completed windows from %s", len(done), ckpt_path)

    headers = {
        "X-API-KEY": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    # A single connection-pooled session keeps TCP/TLS overhead amortized
    # across the thousands of requests a full sweep makes.
    session = requests.Session()

    total_written = 0
    for chunk_start, chunk_end in daterange_chunks(start, end, args.initial_step_days):
        root = Window(chunk_start, chunk_end)
        for window, hits in plan_windows(session, headers, root, args.query):
            label = window.label()
            if label in done:
                logger.info("skip already-done window %s", label)
                staging_path_for_window(out_path, label).unlink(missing_ok=True)
                continue

            target_records = min(hits, MAX_RESULT_WINDOW)
            staging = staging_path_for_window(out_path, label)
            existing = count_jsonl_lines(staging) if staging.exists() else 0

            if existing > target_records:
                logger.warning(
                    "staging %s has %d lines but window target is %d; "
                    "removing stale staging",
                    staging.name,
                    existing,
                    target_records,
                )
                staging.unlink(missing_ok=True)
                existing = 0

            if existing >= target_records:
                logger.info(
                    "staging already holds %d records for %s; appending to output",
                    existing,
                    label,
                )
                merged = append_staging_to_output(staging, out_path)
                append_checkpoint(ckpt_path, label, merged)
                staging.unlink(missing_ok=True)
                total_written += merged
                continue

            if existing:
                logger.info(
                    "resume %s from API offset %d (%d lines in staging)",
                    label,
                    existing,
                    existing,
                )

            mode = "a" if existing else "w"
            logger.info("fetch %s (%d hits)", label, hits)
            with staging.open(mode, encoding="utf-8") as tmp:
                fetch_window(
                    session,
                    headers,
                    window,
                    hits,
                    args.query,
                    tmp,
                    args.throttle,
                    start_offset=existing,
                )

            merged = append_staging_to_output(staging, out_path)
            append_checkpoint(ckpt_path, label, merged)
            staging.unlink(missing_ok=True)
            total_written += merged

    logger.info("done. wrote %d records to %s", total_written, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())

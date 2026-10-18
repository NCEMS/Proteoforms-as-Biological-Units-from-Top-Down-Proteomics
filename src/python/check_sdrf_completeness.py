"""
Stage C (stretch) check: validate that a candidate's deposited SDRF file(s)
are actually *complete*, not merely *present* (the has_sdrf_file_verified
flag from enrich_topdown_files.py only confirms a file named like an SDRF
exists).

"Complete" here is a pragmatic heuristic, not full SDRF-validator-grade
validation (the official checker is the "sdrf-pipelines" Python package,
not available in this environment): a file is counted complete if its
header row contains all of CORE_REQUIRED_COLUMNS, a subset of the minimal
SDRF-proteomics template's mandatory columns
(https://github.com/bigbio/proteomics-sample-metadata) chosen to apply
regardless of acquisition method. Columns specific to proteolytic digestion
(e.g. comment[cleavage agent details]) are deliberately excluded because
top-down (intact-protein) runs don't use a protease and legitimately omit
them.

For each candidate with has_sdrf_file_verified = 1 and no prior check, this
re-fetches the project's file listing to find SDRF-named, non-archived
files, downloads only the first ~4KB of each (Range request — SDRF files
can be large, but the header row is always in the first line) over PRIDE's
HTTPS mirror of its FTP archive, and parses the first line as a tab-
delimited header.

Usage:
    python check_sdrf_completeness.py [--db PATH] [--limit N] [--delay SECONDS]
"""

import argparse
import csv
import io
import logging
import sys
import time

import requests

from pride_client import PrideClient, has_sdrf_filename
from topdown_register_db import accessions_needing_sdrf_check, get_connection, update_sdrf_check

logger = logging.getLogger(__name__)

MAX_FETCH_RETRIES = 3
RETRY_WAIT_SECONDS = 5
HEADER_BYTES = 4096
HTTP_TIMEOUT = 30

# Archived/compressed filenames would need decompression before their
# header row is readable; skipped rather than mis-parsed as plain text.
_ARCHIVE_SUFFIXES = (".zip", ".gz", ".tar", ".tgz", ".bz2", ".7z")

# Subset of the SDRF-proteomics minimal template's mandatory columns that
# apply regardless of acquisition method/digestion (see module docstring).
CORE_REQUIRED_COLUMNS = {
    "source name",
    "characteristics[organism]",
    "characteristics[organism part]",
    "assay name",
    "comment[instrument]",
    "comment[label]",
    "comment[data file]",
}


def _to_https(ftp_url):
    """Convert PRIDE's ftp://ftp.pride.ebi.ac.uk/... URLs to the equivalent HTTPS mirror."""
    if ftp_url and ftp_url.startswith("ftp://"):
        return "https://" + ftp_url[len("ftp://"):]
    return ftp_url


def _fetch_header_columns(session, https_url):
    """
    Download the first HEADER_BYTES of a (presumed tab-delimited) file and
    parse its first line as a set of lowercase, whitespace-stripped column
    names.

    Returns:
        Set of column names, or None if the request/parse failed
    """
    response = session.get(https_url, headers={"Range": f"bytes=0-{HEADER_BYTES - 1}"}, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    first_line = response.text.splitlines()[0] if response.text else ""
    reader = csv.reader(io.StringIO(first_line), delimiter="\t")
    header = next(reader, [])
    return {cell.strip().lower() for cell in header if cell.strip()}


def _check_one_sdrf_file(session, https_url):
    """Fetch and score one SDRF candidate file's header against CORE_REQUIRED_COLUMNS."""
    columns = _fetch_header_columns(session, https_url)
    if columns is None:
        return None
    matched = len(CORE_REQUIRED_COLUMNS & columns)
    return matched


def _check_accession_with_resilience(client, session, accession):
    """
    Find SDRF-named, non-archived files for one project and score each
    against CORE_REQUIRED_COLUMNS, retrying the project's file listing a
    few times on failure.

    Returns:
        (sdrf_files_checked, sdrf_complete_count, best_core_columns, error)
        error is None on success (even if no files could be scored), or a
        short message if every attempt to list/fetch files failed
    """
    files = None
    for attempt in range(MAX_FETCH_RETRIES):
        try:
            files = client.fetch_project_files(accession)
            break
        except Exception as e:
            logger.warning(f"{accession} file listing failed (attempt {attempt + 1}/{MAX_FETCH_RETRIES}): {e}")
            time.sleep(RETRY_WAIT_SECONDS)

    if files is None:
        return 0, 0, 0, "file listing failed after retries"

    candidates = []
    for file_obj in files:
        file_name = file_obj.get("fileName") or ""
        if not has_sdrf_filename([file_name]):
            continue
        if file_name.lower().endswith(_ARCHIVE_SUFFIXES):
            continue
        for location in file_obj.get("publicFileLocations", []):
            if location.get("name") == "FTP Protocol":
                https_url = _to_https(location.get("value"))
                if https_url:
                    candidates.append(https_url)
                break

    checked = 0
    complete = 0
    best_core_columns = 0
    last_error = None
    for https_url in candidates:
        try:
            matched = _check_one_sdrf_file(session, https_url)
        except Exception as e:
            last_error = f"{https_url}: {e}"
            continue
        checked += 1
        best_core_columns = max(best_core_columns, matched)
        if matched == len(CORE_REQUIRED_COLUMNS):
            complete += 1

    error = last_error if checked == 0 and candidates else None
    return checked, complete, best_core_columns, error


def check_sdrf_completeness(db_path: str, limit: int = None, delay: float = 0.2) -> None:
    """
    Content-check every un-checked candidate with a verified SDRF file
    present, scoring each SDRF-named file's header against
    CORE_REQUIRED_COLUMNS.

    Args:
        db_path: Path to the SQLite register database
        limit: Optional cap on the number of candidates checked, for testing
        delay: Seconds to sleep between projects (politeness throttle)
    """
    conn = get_connection(db_path)
    client = PrideClient()
    session = requests.Session()

    pending = accessions_needing_sdrf_check(conn)
    logger.info(f"SDRF completeness check pending for {len(pending)} candidates")

    processed = 0
    for accession in pending:
        checked, complete, best_core_columns, error = _check_accession_with_resilience(client, session, accession)
        update_sdrf_check(
            conn,
            accession,
            sdrf_files_checked=checked,
            sdrf_complete_count=complete,
            sdrf_best_core_columns=best_core_columns,
            has_complete_sdrf=complete > 0,
            sdrf_check_error=error,
        )
        processed += 1

        if processed % 50 == 0:
            logger.info(f"SDRF completeness check: processed={processed} remaining={len(pending) - processed}")

        if limit and processed >= limit:
            logger.info(f"SDRF completeness check complete (limit reached): processed={processed}")
            return

        time.sleep(delay)

    logger.info(f"SDRF completeness check complete: processed={processed}")


def main():
    parser = argparse.ArgumentParser(
        description="Check completeness of deposited SDRF files for harvested/enriched top-down candidates"
    )
    parser.add_argument("--db", default="data/topdown_candidates.db", help="SQLite register database path")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on candidates checked (for testing)")
    parser.add_argument("--delay", type=float, default=0.2, help="Seconds to sleep between requests")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    check_sdrf_completeness(args.db, limit=args.limit, delay=args.delay)


if __name__ == "__main__":
    sys.exit(main())

"""
Stage B enrichment: file-listing-based storage footprint and verified SDRF
flag for candidates already harvested by build_topdown_candidates.py.

For each un-enriched PXD in the register, fetches its full file listing
(pride_client.fetch_project_files()) and records:
  - file_count / total_size_bytes: the raw storage footprint input for the
    NCEMS 10-40TB storage-estimate conversation
  - has_sdrf_file_verified: an SDRF-filename check against the *actual*
    file listing, a more reliable signal than the harvest stage's
    search-summary "projectFileNames" (which can be stale/partial for some
    submissions)

Idempotent/resumable: only accessions with enriched_at IS NULL are
processed, and a project whose file fetch fails even after retries is
logged and left un-enriched for a later re-run.

Usage:
    python enrich_topdown_files.py [--db PATH] [--limit N] [--delay SECONDS]
"""

import argparse
import logging
import sys
import time

from pride_client import PrideClient, has_sdrf_filename
from topdown_register_db import accessions_needing_enrichment, get_connection, update_enrichment

logger = logging.getLogger(__name__)

MAX_FETCH_RETRIES = 3
RETRY_WAIT_SECONDS = 5


def _fetch_files_with_resilience(client: PrideClient, accession: str):
    """
    Fetch one project's file listing, retrying a few times with a longer
    pause on failure (on top of pride_client's own internal retry/backoff).
    Returns None (treated as "skip for this run") if every attempt fails.
    """
    for attempt in range(MAX_FETCH_RETRIES):
        try:
            return client.fetch_project_files(accession)
        except Exception as e:
            logger.warning(f"{accession} file fetch failed (attempt {attempt + 1}/{MAX_FETCH_RETRIES}): {e}")
            time.sleep(RETRY_WAIT_SECONDS)

    logger.error(f"{accession} file fetch failed after {MAX_FETCH_RETRIES} attempts; leaving un-enriched")
    return None


def enrich_topdown_files(db_path: str, limit: int = None, delay: float = 0.1) -> None:
    """
    Enrich every un-enriched candidate in the register with file count,
    total storage size, and a file-listing-verified SDRF flag.

    Args:
        db_path: Path to the SQLite register database
        limit: Optional cap on the number of candidates enriched, for testing
        delay: Seconds to sleep between project file-listing requests (politeness throttle)
    """
    conn = get_connection(db_path)
    client = PrideClient()

    pending = accessions_needing_enrichment(conn)
    logger.info(f"Enrichment pending for {len(pending)} candidates")

    processed = 0
    failed = 0
    for accession in pending:
        files = _fetch_files_with_resilience(client, accession)
        if files is None:
            failed += 1
            time.sleep(delay)
            continue

        file_names = [f.get("fileName") for f in files]
        total_size_bytes = sum(f.get("fileSizeBytes") or 0 for f in files)

        update_enrichment(
            conn,
            accession,
            file_count=len(files),
            total_size_bytes=total_size_bytes,
            has_sdrf_file_verified=has_sdrf_filename(file_names),
        )
        processed += 1

        if processed % 50 == 0:
            logger.info(f"File enrichment: processed={processed} failed={failed} remaining={len(pending) - processed - failed}")

        if limit and processed >= limit:
            logger.info(f"File enrichment complete (limit reached): processed={processed} failed={failed}")
            return

        time.sleep(delay)

    logger.info(f"File enrichment complete: processed={processed} failed={failed}")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich harvested top-down candidates with file counts/storage sizes and a verified SDRF flag"
    )
    parser.add_argument("--db", default="data/topdown_candidates.db", help="SQLite register database path")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on candidates enriched (for testing)")
    parser.add_argument("--delay", type=float, default=0.1, help="Seconds to sleep between requests")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    enrich_topdown_files(args.db, limit=args.limit, delay=args.delay)


if __name__ == "__main__":
    sys.exit(main())

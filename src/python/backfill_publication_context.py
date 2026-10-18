"""
Stage D (optional polish) backfill: resolve publication title/journal/year
for harvested candidates via Europe PMC, using whichever identifier
(PMID preferred, else DOI) PRIDE's search summary already provided.

PRIDE's own "title" field on each candidate is the *dataset* title (often
terse or reanalysis-specific, e.g. "PIA - Mouse Benchmark Dataset"), not
the *publication* title researchers would recognize — this backfill adds
the actual paper's title/journal/year for a more readable spreadsheet.

Candidates with neither a DOI nor a PMID are left un-resolved (recorded
with publication_check_error="no doi/pmid available") rather than retried
every run.

Usage:
    python backfill_publication_context.py [--db PATH] [--limit N] [--delay SECONDS]
"""

import argparse
import logging
import sys
import time

from europepmc_client import EuropePMCClient
from topdown_register_db import accessions_needing_publication_check, get_connection, update_publication_context

logger = logging.getLogger(__name__)

MAX_LOOKUP_RETRIES = 3
RETRY_WAIT_SECONDS = 5


def _lookup_with_resilience(client: EuropePMCClient, pmid, doi):
    """Resolve one publication via Europe PMC, retrying a few times on transient failures."""
    for attempt in range(MAX_LOOKUP_RETRIES):
        try:
            return client.fetch_publication(pmid=pmid, doi=doi)
        except Exception as e:
            logger.warning(f"Europe PMC lookup failed (attempt {attempt + 1}/{MAX_LOOKUP_RETRIES}): {e}")
            time.sleep(RETRY_WAIT_SECONDS)
    return "error"  # sentinel distinct from "looked up, found nothing" (None)


def backfill_publication_context(db_path: str, limit: int = None, delay: float = 0.34) -> None:
    """
    Resolve title/journal/year for every un-checked candidate that has a
    DOI or PMID, via Europe PMC.

    Args:
        db_path: Path to the SQLite register database
        limit: Optional cap on the number of candidates checked, for testing
        delay: Seconds to sleep between lookups (Europe PMC's documented
            courtesy rate limit is ~3 requests/second)
    """
    conn = get_connection(db_path)
    client = EuropePMCClient()

    pending = accessions_needing_publication_check(conn)
    logger.info(f"Publication context backfill pending for {len(pending)} candidates")

    processed = 0
    resolved = 0
    for accession in pending:
        row = conn.execute("SELECT doi, pmids FROM topdown_candidates WHERE accession = ?", (accession,)).fetchone()
        doi, pmids = row
        pmid = pmids.split(",")[0] if pmids else None

        if not pmid and not doi:
            update_publication_context(conn, accession, publication_check_error="no doi/pmid available")
            processed += 1
            continue

        result = _lookup_with_resilience(client, pmid, doi)
        if result == "error":
            update_publication_context(conn, accession, publication_check_error="Europe PMC lookup failed")
        elif result is None:
            update_publication_context(conn, accession, publication_check_error="not found in Europe PMC")
        else:
            journal_info = result.get("journalInfo") or {}
            journal = (journal_info.get("journal") or {}).get("title")
            update_publication_context(
                conn,
                accession,
                publication_title=result.get("title"),
                journal=journal,
                pub_year=str(result.get("pubYear")) if result.get("pubYear") else None,
            )
            resolved += 1

        processed += 1
        if processed % 50 == 0:
            logger.info(f"Publication backfill: processed={processed} resolved={resolved} remaining={len(pending) - processed}")

        if limit and processed >= limit:
            logger.info(f"Publication backfill complete (limit reached): processed={processed} resolved={resolved}")
            return

        time.sleep(delay)

    logger.info(f"Publication backfill complete: processed={processed} resolved={resolved}")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill publication title/journal/year for harvested top-down candidates via Europe PMC"
    )
    parser.add_argument("--db", default="data/topdown_candidates.db", help="SQLite register database path")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on candidates checked (for testing)")
    parser.add_argument("--delay", type=float, default=0.34, help="Seconds to sleep between lookups")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    backfill_publication_context(args.db, limit=args.limit, delay=args.delay)


if __name__ == "__main__":
    sys.exit(main())

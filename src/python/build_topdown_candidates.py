"""
Stage A harvester: identify candidate top-down proteomics PXDs for the four
NCEMS target model organisms (human, mouse, E. coli, yeast).

Sweeps PRIDE's bulk project-listing endpoint narrowed server-side with
keyword="top-down" (~1,500 hits across ~16 pages at the default page size,
instead of paginating all ~42,000 public PRIDE projects), then filters each
summary precisely on its structured fields:
  - "experimentTypes" must contain the literal "Top-down proteomics" tag
    (the keyword match alone is a free-text search and also matches
    unrelated shotgun/DIA projects that merely mention "top-down" in text)
  - "organisms" must include one of the four target organisms (matched by
    genus+species prefix, case-insensitively, so "Mus musculus (mouse)",
    "Mus musculus", and strain variants like "Escherichia coli o157:h7" all
    match)

Each match is upserted into topdown_register_db.py with the metadata
already present on the search summary (organism/tissue/instrument/DDA-DIA
flag/SDRF-presence signals/publication IDs) — no extra per-project API call
is needed for this harvest stage. See enrich_topdown_files.py for the
file-listing-based enrichment stage (storage footprint, verified SDRF flag).

Usage:
    python build_topdown_candidates.py [--db PATH] [--page-size N] [--limit N]
"""

import argparse
import logging
import sys

from pride_client import PrideClient, has_sdrf_filename
from topdown_register_db import get_connection, upsert_candidate

logger = logging.getLogger(__name__)

KEYWORD = "top-down"
REQUIRED_EXPERIMENT_TYPE = "Top-down proteomics"
DIA_EXPERIMENT_TYPE = "Data-independent acquisition"

# Genus+species prefixes (lowercase) for the four NCEMS target organisms.
# Matched with str.startswith() against each lowercased PRIDE organism
# string so common-name suffixes ("(mouse)") and strain qualifiers
# ("o157:h7") don't prevent a match.
TARGET_ORGANISMS = {
    "human": "homo sapiens",
    "mouse": "mus musculus",
    "e_coli": "escherichia coli",
    "yeast": "saccharomyces cerevisiae",
}


def _clean_doi(doi):
    """
    Normalize a project summary's top-level "doi" field: PRIDE's bulk
    search/listing endpoint sometimes returns the literal string "null"
    (not JSON null) for projects with no DOI, even though their
    /projects/{pxd} details endpoint has a real one — treat that string,
    and any other blank/whitespace-only value, as absent.
    """
    if not doi or not doi.strip() or doi.strip().lower() == "null":
        return None
    return doi.strip()


def matched_organism_labels(organisms):
    """
    Return the set of target-organism labels (e.g. {"human", "mouse"})
    present in a project's raw "organisms" list.

    Args:
        organisms: Raw "organisms" list from a PRIDE project summary

    Returns:
        Set of matched labels from TARGET_ORGANISMS; empty if none match
    """
    labels = set()
    for organism in organisms or []:
        lowered = (organism or "").lower()
        for label, prefix in TARGET_ORGANISMS.items():
            if lowered.startswith(prefix):
                labels.add(label)
    return labels


def build_topdown_candidates(db_path: str, page_size: int = 100, limit: int = None) -> None:
    """
    Sweep PRIDE's keyword="top-down" search results, keep only projects
    tagged "Top-down proteomics" on one of the four target organisms, and
    upsert each into the topdown_candidates register.

    Args:
        db_path: Path to the SQLite register database
        page_size: PRIDE search results per page
        limit: Optional cap on the number of matching candidates recorded,
            for testing
    """
    conn = get_connection(db_path)
    client = PrideClient()

    seen = 0
    matched = 0
    for project in client.iter_all_projects(page_size=page_size, keyword=KEYWORD):
        seen += 1

        experiment_types = project.get("experimentTypes") or []
        if REQUIRED_EXPERIMENT_TYPE not in experiment_types:
            continue

        labels = matched_organism_labels(project.get("organisms"))
        if not labels:
            continue

        accession = project.get("accession")
        if not accession:
            continue

        doi, pmids = client.extract_publication_info_from_search_summary(project)
        file_names = project.get("projectFileNames") or []

        upsert_candidate(
            conn,
            accession,
            title=project.get("title"),
            organism_labels=",".join(sorted(labels)),
            organisms_raw=",".join(project.get("organisms") or []),
            tissues=",".join(project.get("organismsPart") or []),
            instruments=",".join(project.get("instruments") or []),
            experiment_types=",".join(experiment_types),
            is_dia=DIA_EXPERIMENT_TYPE in experiment_types,
            has_sdrf_filename=has_sdrf_filename(file_names),
            has_annotated_sdrf=bool(project.get("sdrf")),
            doi=_clean_doi(doi),
            pmids=",".join(pmids),
            submission_date=project.get("submissionDate"),
            publication_date=project.get("publicationDate"),
        )
        matched += 1

        if matched % 100 == 0:
            logger.info(f"Top-down candidates: seen={seen} matched={matched}")

        if limit and matched >= limit:
            logger.info(f"Top-down candidate harvest complete (limit reached): seen={seen} matched={matched}")
            return

    logger.info(f"Top-down candidate harvest complete: seen={seen} matched={matched}")


def main():
    parser = argparse.ArgumentParser(
        description="Harvest candidate top-down proteomics PXDs for the four NCEMS target organisms"
    )
    parser.add_argument("--db", default="data/topdown_candidates.db", help="SQLite register database path")
    parser.add_argument("--page-size", type=int, default=100, help="PRIDE search results per page")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on candidates recorded (for testing)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    build_topdown_candidates(args.db, page_size=args.page_size, limit=args.limit)


if __name__ == "__main__":
    sys.exit(main())

"""
SQLite-backed register of candidate top-down proteomics datasets scouted
from PRIDE/ProteomeXchange for the NCEMS proteoform data-scouting objectives:
PXDs covering top-down acquisition on the four target model organisms
(human, mouse, E. coli, yeast).

Populated in two passes:
- Harvest (build_topdown_candidates.py): one row per matching PXD with the
  organism/tissue/instrument/technique/publication/SDRF-presence metadata
  already present on PRIDE's bulk search summary (one API call per ~100
  projects).
- Enrichment (enrich_topdown_files.py): fills in file-count/storage-size
  and a file-listing-verified SDRF flag that require one extra API call per
  project (fetch_project_files()).

CSV export (export_topdown_scouting_csv.py) produces the PXD+SDRF
spreadsheet deliverable plus summary stats, from this register.
"""

import csv
import logging
import sqlite3
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS topdown_candidates (
    accession              TEXT PRIMARY KEY,
    title                  TEXT,
    organism_labels        TEXT,                    -- comma-joined target labels, e.g. "human,mouse"
    organisms_raw          TEXT,                     -- comma-joined raw PRIDE organism strings
    tissues                TEXT,                     -- comma-joined organismsPart
    instruments            TEXT,                     -- comma-joined
    experiment_types       TEXT,                     -- comma-joined raw PRIDE experimentTypes
    is_dia                 INTEGER NOT NULL DEFAULT 0, -- explicit "Data-independent acquisition" flag
    has_sdrf_filename      INTEGER NOT NULL DEFAULT 0, -- SDRF-looking filename on the search summary
    has_annotated_sdrf     INTEGER NOT NULL DEFAULT 0, -- PRIDE-harmonized inline "sdrf" field present
    doi                    TEXT,
    pmids                  TEXT,                     -- comma-joined
    submission_date        TEXT,
    publication_date       TEXT,
    harvested_at           TEXT DEFAULT (datetime('now')),
    file_count             INTEGER,
    total_size_bytes       INTEGER,
    has_sdrf_file_verified INTEGER,                   -- re-checked against fetch_project_files()
    enriched_at            TEXT
);

CREATE INDEX IF NOT EXISTS idx_topdown_organism ON topdown_candidates(organism_labels);
CREATE INDEX IF NOT EXISTS idx_topdown_enriched ON topdown_candidates(enriched_at);
"""

# Columns added after the initial release, applied as a migration (see
# _ensure_columns()) rather than the CREATE TABLE above so pre-existing
# database files pick them up without being recreated.
_SDRF_CHECK_COLUMNS = {
    "sdrf_files_checked": "INTEGER",        # number of SDRF-named files downloaded/parsed
    "sdrf_complete_count": "INTEGER",       # of those, how many matched the core-column heuristic
    "sdrf_best_core_columns": "INTEGER",    # best file's matched-core-column count (out of 7)
    "has_complete_sdrf": "INTEGER",         # 1 if any checked file met the completeness heuristic
    "sdrf_check_error": "TEXT",             # last error encountered, if any (download/parse failure)
    "sdrf_checked_at": "TEXT",              # resumability marker for check_sdrf_completeness.py
}

_PUBLICATION_CONTEXT_COLUMNS = {
    "publication_title": "TEXT",    # resolved via Europe PMC from doi/pmids
    "journal": "TEXT",
    "pub_year": "TEXT",
    "publication_check_error": "TEXT",  # e.g. "no doi/pmid available" or a lookup failure
    "publication_checked_at": "TEXT",   # resumability marker for backfill_publication_context.py
}

_MIGRATION_COLUMNS = {**_SDRF_CHECK_COLUMNS, **_PUBLICATION_CONTEXT_COLUMNS}


def _ensure_columns(conn: sqlite3.Connection) -> None:
    """Add any _MIGRATION_COLUMNS missing from an existing topdown_candidates table."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(topdown_candidates)")}
    for column, col_type in _MIGRATION_COLUMNS.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE topdown_candidates ADD COLUMN {column} {col_type}")
    conn.commit()


def get_connection(db_path: str) -> sqlite3.Connection:
    """
    Open (creating if needed) the top-down candidate register SQLite
    database and ensure the schema exists.

    Args:
        db_path: Filesystem path to the SQLite file

    Returns:
        An open sqlite3.Connection with the schema applied
    """
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(SCHEMA)
    conn.commit()
    _ensure_columns(conn)
    return conn


def is_harvested(conn: sqlite3.Connection, accession: str) -> bool:
    """Check whether an accession is already in the register (harvest resumability)."""
    cur = conn.execute("SELECT 1 FROM topdown_candidates WHERE accession = ?", (accession,))
    return cur.fetchone() is not None


def upsert_candidate(
    conn: sqlite3.Connection,
    accession: str,
    *,
    title: Optional[str],
    organism_labels: str,
    organisms_raw: str,
    tissues: str,
    instruments: str,
    experiment_types: str,
    is_dia: bool,
    has_sdrf_filename: bool,
    has_annotated_sdrf: bool,
    doi: Optional[str],
    pmids: str,
    submission_date: Optional[str],
    publication_date: Optional[str],
) -> None:
    """Insert or refresh one candidate PXD's harvest-stage metadata."""
    conn.execute(
        """
        INSERT INTO topdown_candidates (
            accession, title, organism_labels, organisms_raw, tissues, instruments,
            experiment_types, is_dia, has_sdrf_filename, has_annotated_sdrf, doi, pmids,
            submission_date, publication_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(accession) DO UPDATE SET
            title=excluded.title, organism_labels=excluded.organism_labels,
            organisms_raw=excluded.organisms_raw, tissues=excluded.tissues,
            instruments=excluded.instruments, experiment_types=excluded.experiment_types,
            is_dia=excluded.is_dia, has_sdrf_filename=excluded.has_sdrf_filename,
            has_annotated_sdrf=excluded.has_annotated_sdrf, doi=excluded.doi, pmids=excluded.pmids,
            submission_date=excluded.submission_date, publication_date=excluded.publication_date
        """,
        (
            accession, title, organism_labels, organisms_raw, tissues, instruments,
            experiment_types, int(is_dia), int(has_sdrf_filename), int(has_annotated_sdrf),
            doi, pmids, submission_date, publication_date,
        ),
    )
    conn.commit()


def accessions_needing_enrichment(conn: sqlite3.Connection) -> List[str]:
    """Return accessions harvested but not yet file-enriched (enrichment resumability)."""
    cur = conn.execute("SELECT accession FROM topdown_candidates WHERE enriched_at IS NULL")
    return [row[0] for row in cur.fetchall()]


def update_enrichment(
    conn: sqlite3.Connection,
    accession: str,
    *,
    file_count: int,
    total_size_bytes: int,
    has_sdrf_file_verified: bool,
) -> None:
    """Record one candidate PXD's file-listing enrichment results."""
    conn.execute(
        """
        UPDATE topdown_candidates
        SET file_count = ?, total_size_bytes = ?, has_sdrf_file_verified = ?,
            enriched_at = datetime('now')
        WHERE accession = ?
        """,
        (file_count, total_size_bytes, int(has_sdrf_file_verified), accession),
    )
    conn.commit()


def accessions_needing_sdrf_check(conn: sqlite3.Connection) -> List[str]:
    """
    Return accessions with a verified SDRF file present (has_sdrf_file_verified = 1)
    that haven't yet had their SDRF content checked for completeness.
    """
    cur = conn.execute(
        "SELECT accession FROM topdown_candidates "
        "WHERE has_sdrf_file_verified = 1 AND sdrf_checked_at IS NULL"
    )
    return [row[0] for row in cur.fetchall()]


def update_sdrf_check(
    conn: sqlite3.Connection,
    accession: str,
    *,
    sdrf_files_checked: int,
    sdrf_complete_count: int,
    sdrf_best_core_columns: int,
    has_complete_sdrf: bool,
    sdrf_check_error: Optional[str] = None,
) -> None:
    """Record one candidate PXD's SDRF content-completeness check results."""
    conn.execute(
        """
        UPDATE topdown_candidates
        SET sdrf_files_checked = ?, sdrf_complete_count = ?, sdrf_best_core_columns = ?,
            has_complete_sdrf = ?, sdrf_check_error = ?, sdrf_checked_at = datetime('now')
        WHERE accession = ?
        """,
        (sdrf_files_checked, sdrf_complete_count, sdrf_best_core_columns,
         int(has_complete_sdrf), sdrf_check_error, accession),
    )
    conn.commit()


def accessions_needing_publication_check(conn: sqlite3.Connection) -> List[str]:
    """Return accessions not yet checked for Europe PMC publication context (title/journal/year)."""
    cur = conn.execute("SELECT accession FROM topdown_candidates WHERE publication_checked_at IS NULL")
    return [row[0] for row in cur.fetchall()]


def update_publication_context(
    conn: sqlite3.Connection,
    accession: str,
    *,
    publication_title: Optional[str] = None,
    journal: Optional[str] = None,
    pub_year: Optional[str] = None,
    publication_check_error: Optional[str] = None,
) -> None:
    """Record one candidate PXD's Europe PMC publication-context backfill results."""
    conn.execute(
        """
        UPDATE topdown_candidates
        SET publication_title = ?, journal = ?, pub_year = ?,
            publication_check_error = ?, publication_checked_at = datetime('now')
        WHERE accession = ?
        """,
        (publication_title, journal, pub_year, publication_check_error, accession),
    )
    conn.commit()


def summary_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Summarize the register for the data-scouting deliverable: counts by
    organism, DDA/DIA split, SDRF-presence proportions, and (where
    enrichment has run) total/mean storage footprint.

    Returns:
        Dict with keys: total, by_organism, dia_count, dda_or_unspecified_count,
        has_sdrf_filename_count, has_annotated_sdrf_count, has_sdrf_file_verified_count,
        enriched_count, total_size_bytes, mean_size_bytes_per_project
    """
    total = conn.execute("SELECT COUNT(*) FROM topdown_candidates").fetchone()[0]

    by_organism: Dict[str, int] = {}
    for row in conn.execute("SELECT organism_labels FROM topdown_candidates"):
        for label in (row[0] or "").split(","):
            if label:
                by_organism[label] = by_organism.get(label, 0) + 1

    dia_count = conn.execute("SELECT COUNT(*) FROM topdown_candidates WHERE is_dia = 1").fetchone()[0]
    has_sdrf_filename_count = conn.execute(
        "SELECT COUNT(*) FROM topdown_candidates WHERE has_sdrf_filename = 1"
    ).fetchone()[0]
    has_annotated_sdrf_count = conn.execute(
        "SELECT COUNT(*) FROM topdown_candidates WHERE has_annotated_sdrf = 1"
    ).fetchone()[0]
    has_sdrf_file_verified_count = conn.execute(
        "SELECT COUNT(*) FROM topdown_candidates WHERE has_sdrf_file_verified = 1"
    ).fetchone()[0]
    has_complete_sdrf_count = conn.execute(
        "SELECT COUNT(*) FROM topdown_candidates WHERE has_complete_sdrf = 1"
    ).fetchone()[0]
    sdrf_checked_count = conn.execute(
        "SELECT COUNT(*) FROM topdown_candidates WHERE sdrf_checked_at IS NOT NULL"
    ).fetchone()[0]
    enriched_count = conn.execute(
        "SELECT COUNT(*) FROM topdown_candidates WHERE enriched_at IS NOT NULL"
    ).fetchone()[0]
    total_size_bytes = conn.execute(
        "SELECT SUM(total_size_bytes) FROM topdown_candidates WHERE total_size_bytes IS NOT NULL"
    ).fetchone()[0] or 0
    has_publication_title_count = conn.execute(
        "SELECT COUNT(*) FROM topdown_candidates WHERE publication_title IS NOT NULL"
    ).fetchone()[0]

    return {
        "total": total,
        "by_organism": by_organism,
        "dia_count": dia_count,
        "dda_or_unspecified_count": total - dia_count,
        "has_sdrf_filename_count": has_sdrf_filename_count,
        "has_annotated_sdrf_count": has_annotated_sdrf_count,
        "has_sdrf_file_verified_count": has_sdrf_file_verified_count,
        "has_complete_sdrf_count": has_complete_sdrf_count,
        "sdrf_checked_count": sdrf_checked_count,
        "enriched_count": enriched_count,
        "total_size_bytes": total_size_bytes,
        "has_publication_title_count": has_publication_title_count,
        "mean_size_bytes_per_project": (total_size_bytes / enriched_count) if enriched_count else 0,
    }


def export_to_csv(conn: sqlite3.Connection, csv_path: str) -> int:
    """
    Export the full topdown_candidates table (one row per PXD) to CSV —
    the PXD+SDRF spreadsheet deliverable.

    Args:
        conn: Open database connection
        csv_path: Output CSV file path

    Returns:
        Number of rows written
    """
    cur = conn.execute(
        """
        SELECT accession, title, organism_labels, organisms_raw, tissues, instruments,
               experiment_types, is_dia, has_sdrf_filename, has_annotated_sdrf,
               has_sdrf_file_verified, sdrf_files_checked, sdrf_complete_count,
               sdrf_best_core_columns, has_complete_sdrf, sdrf_check_error,
               doi, pmids, publication_title, journal, pub_year, publication_check_error,
               submission_date, publication_date,
               file_count, total_size_bytes, harvested_at, enriched_at, sdrf_checked_at,
               publication_checked_at
        FROM topdown_candidates
        ORDER BY accession
        """
    )
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    logger.info(f"Exported {len(rows)} topdown candidate rows to {csv_path}")
    return len(rows)

"""
Export the topdown_register_db.py SQLite register to the data-scouting
spreadsheet deliverable.

Writes two files into --output-dir:
  - topdown_candidates.csv  one row per candidate PXD (the PXD+SDRF
                            spreadsheet Ian committed to providing: organism,
                            tissue, technique, SDRF presence, storage size,
                            publication links)
  - topdown_summary.csv     one row of aggregate stats (counts by organism,
                            DDA/DIA split, SDRF-presence proportions, total/
                            mean storage) — the inputs for the external
                            GPU-hour/Hamlet and storage-estimate conversation

Usage:
    python export_topdown_scouting_csv.py [--db PATH] [--output-dir DIR]
"""

import argparse
import csv
import logging
import os
import sys

from topdown_register_db import export_to_csv, get_connection, summary_stats

logger = logging.getLogger(__name__)


def _write_summary_csv(stats: dict, csv_path: str) -> None:
    """Flatten summary_stats()'s nested dict into one wide summary CSV row."""
    row = {
        "total_candidates": stats["total"],
        "dia_count": stats["dia_count"],
        "dda_or_unspecified_count": stats["dda_or_unspecified_count"],
        "has_sdrf_filename_count": stats["has_sdrf_filename_count"],
        "has_annotated_sdrf_count": stats["has_annotated_sdrf_count"],
        "has_sdrf_file_verified_count": stats["has_sdrf_file_verified_count"],
        "has_complete_sdrf_count": stats["has_complete_sdrf_count"],
        "sdrf_checked_count": stats["sdrf_checked_count"],
        "enriched_count": stats["enriched_count"],
        "total_size_bytes": stats["total_size_bytes"],
        "total_size_tb": stats["total_size_bytes"] / (1024 ** 4),
        "mean_size_bytes_per_project": stats["mean_size_bytes_per_project"],
        "has_publication_title_count": stats["has_publication_title_count"],
    }
    for label, count in sorted(stats["by_organism"].items()):
        row[f"count_{label}"] = count

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    logger.info(f"Exported summary stats to {csv_path}")


def export_all(db_path: str, output_dir: str) -> None:
    """
    Export the candidate register and its summary stats to CSV files in
    output_dir.

    Args:
        db_path: Path to the SQLite register database
        output_dir: Directory to write CSV files into (created if missing)
    """
    os.makedirs(output_dir, exist_ok=True)
    conn = get_connection(db_path)

    candidates_csv = os.path.join(output_dir, "topdown_candidates.csv")
    summary_csv = os.path.join(output_dir, "topdown_summary.csv")

    candidate_count = export_to_csv(conn, candidates_csv)
    stats = summary_stats(conn)
    _write_summary_csv(stats, summary_csv)

    logger.info(f"Export complete: {candidate_count} candidate rows -> {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Export the top-down candidate register to CSV files")
    parser.add_argument("--db", default="data/topdown_candidates.db", help="SQLite register database path")
    parser.add_argument("--output-dir", default="data/csv", help="Directory to write CSV files into")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    export_all(args.db, args.output_dir)


if __name__ == "__main__":
    sys.exit(main())

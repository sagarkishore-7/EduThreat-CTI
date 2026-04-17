"""
Diagnostic script: find duplicate/near-duplicate incidents for a given institution name.

Usage:
    railway ssh python3 -m src.edu_cti.tools.find_duplicates --name "Salford"
    railway ssh python3 -m src.edu_cti.tools.find_duplicates --name "Salford" --verbose
"""
import argparse
import json
import sqlite3

from src.edu_cti.core.config import DB_PATH


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="Salford", help="Institution name to search")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT incident_id, source, institution_name, victim_raw_name,
               incident_date, date_precision, primary_url, all_urls,
               attack_type, country, llm_enriched, ingested_at
        FROM incidents
        WHERE institution_name LIKE ? OR victim_raw_name LIKE ?
        ORDER BY incident_date, ingested_at
        """,
        (f"%{args.name}%", f"%{args.name}%"),
    ).fetchall()

    print(f"\nFound {len(rows)} incident(s) matching '{args.name}':\n")
    for r in rows:
        print(f"{'='*70}")
        print(f"ID:           {r['incident_id']}")
        print(f"Source:       {r['source']}")
        print(f"Name:         {r['institution_name']}")
        print(f"Victim raw:   {r['victim_raw_name']}")
        print(f"Date:         {r['incident_date']} ({r['date_precision']})")
        print(f"Attack type:  {r['attack_type']}")
        print(f"Country:      {r['country']}")
        print(f"Enriched:     {r['llm_enriched']}")
        print(f"Ingested at:  {r['ingested_at']}")
        print(f"Primary URL:  {r['primary_url']}")
        if args.verbose and r['all_urls']:
            try:
                urls = json.loads(r['all_urls'])
                for u in urls:
                    print(f"  URL: {u}")
            except Exception:
                print(f"  all_urls: {r['all_urls'][:300]}")

    conn.close()


if __name__ == "__main__":
    main()

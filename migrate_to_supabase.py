#!/usr/bin/env python3
"""
Migrate Black Panther archive from SQLite → Supabase (PostgreSQL).
Usage: python3 migrate_to_supabase.py
Set DATABASE_URL env var first:
  export DATABASE_URL="postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres"
"""

import os
import sqlite3
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Installing psycopg2...")
    import subprocess
    subprocess.run(["pip3", "install", "psycopg2-binary"], check=True)
    import psycopg2
    from psycopg2.extras import execute_values

SQLITE_PATH = Path(__file__).parent / "black_panther_newspaper.db"
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL environment variable first.")
    print('Example: export DATABASE_URL="postgresql://postgres:..."')
    exit(1)


def setup_postgres(pg):
    cur = pg.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS issues (
            id          SERIAL PRIMARY KEY,
            volume      INTEGER NOT NULL,
            issue       TEXT NOT NULL,
            pub_date    DATE NOT NULL,
            filename    TEXT NOT NULL,
            url         TEXT NOT NULL,
            source_page TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_issues_volume   ON issues(volume);
        CREATE INDEX IF NOT EXISTS idx_issues_pub_date ON issues(pub_date);

        CREATE TABLE IF NOT EXISTS issue_text (
            issue_id   INTEGER PRIMARY KEY REFERENCES issues(id),
            full_text  TEXT,
            page_count INTEGER,
            indexed_at TIMESTAMPTZ DEFAULT now(),
            search_vec TSVECTOR
        );

        CREATE INDEX IF NOT EXISTS idx_issue_text_search
            ON issue_text USING GIN(search_vec);
    """)

    # trigger to auto-update search_vec on insert/update
    cur.execute("""
        CREATE OR REPLACE FUNCTION update_search_vec() RETURNS trigger AS $$
        BEGIN
            NEW.search_vec := to_tsvector('english', coalesce(NEW.full_text, ''));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS tsvector_update ON issue_text;

        CREATE TRIGGER tsvector_update
        BEFORE INSERT OR UPDATE OF full_text ON issue_text
        FOR EACH ROW EXECUTE FUNCTION update_search_vec();
    """)
    pg.commit()
    print("Schema created.")


def migrate_issues(sqlite_con, pg):
    cur_sq = sqlite_con.cursor()
    cur_pg = pg.cursor()

    rows = cur_sq.execute(
        "SELECT id, volume, issue, pub_date, filename, url, source_page FROM issues ORDER BY id"
    ).fetchall()

    cur_pg.execute("TRUNCATE issues CASCADE")
    execute_values(cur_pg,
        "INSERT INTO issues (id, volume, issue, pub_date, filename, url, source_page) VALUES %s",
        rows
    )
    # reset sequence
    cur_pg.execute("SELECT setval('issues_id_seq', (SELECT MAX(id) FROM issues))")
    pg.commit()
    print(f"Migrated {len(rows)} issues.")


def migrate_text(sqlite_con, pg):
    cur_sq = sqlite_con.cursor()
    cur_pg = pg.cursor()

    rows = cur_sq.execute(
        "SELECT issue_id, full_text, page_count FROM issue_text"
    ).fetchall()

    if not rows:
        print("No indexed text to migrate yet.")
        return

    execute_values(cur_pg,
        "INSERT INTO issue_text (issue_id, full_text, page_count) VALUES %s "
        "ON CONFLICT (issue_id) DO UPDATE SET full_text=EXCLUDED.full_text, page_count=EXCLUDED.page_count",
        rows,
        page_size=10
    )
    pg.commit()
    print(f"Migrated {len(rows)} indexed issues with full text.")


if __name__ == "__main__":
    print(f"Connecting to Supabase...")
    pg = psycopg2.connect(DATABASE_URL)

    print(f"Opening SQLite: {SQLITE_PATH}")
    sq = sqlite3.connect(SQLITE_PATH)

    setup_postgres(pg)
    migrate_issues(sq, pg)
    migrate_text(sq, pg)

    sq.close()
    pg.close()
    print("\nMigration complete!")

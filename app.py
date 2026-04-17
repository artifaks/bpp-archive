#!/usr/bin/env python3
"""
Black Panther Newspaper Archive — local web interface.
Run:  python3 app.py
Then open:  http://localhost:5050
"""

import os
import sqlite3
from pathlib import Path
from flask import Flask, render_template, request, jsonify, g

DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).parent / "black_panther_newspaper.db"))

app = Flask(__name__)


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_):
    db = g.pop("db", None)
    if db:
        db.close()


@app.route("/")
def index():
    db = get_db()
    stats = db.execute("""
        SELECT
            (SELECT COUNT(*) FROM issues)      AS total_issues,
            (SELECT COUNT(*) FROM issue_text)  AS indexed,
            (SELECT MIN(pub_date) FROM issues) AS earliest,
            (SELECT MAX(pub_date) FROM issues) AS latest
    """).fetchone()

    volumes = db.execute("""
        SELECT volume,
               COUNT(*) as issue_count,
               MIN(pub_date) as start_date,
               MAX(pub_date) as end_date
        FROM issues
        GROUP BY volume
        ORDER BY volume
    """).fetchall()

    return render_template("index.html", stats=stats, volumes=volumes)


@app.route("/search")
def search():
    query   = request.args.get("q", "").strip()
    volume  = request.args.get("vol", "")
    year    = request.args.get("year", "")
    page    = int(request.args.get("page", 1))
    per_page = 10
    offset  = (page - 1) * per_page

    if not query:
        return render_template("search.html", query="", results=[], total=0, page=1, pages=1)

    db = get_db()

    indexed = db.execute("SELECT COUNT(*) FROM issue_text").fetchone()[0]

    where_clauses = ["issues_fts MATCH ?"]
    params = [query]

    if volume:
        where_clauses.append("i.volume = ?")
        params.append(int(volume))
    if year:
        where_clauses.append("strftime('%Y', i.pub_date) = ?")
        params.append(year)

    join_clause = "JOIN issues i ON fts.rowid = i.id"
    left_join   = "LEFT JOIN issue_text it ON it.issue_id = i.id"
    where_str   = " AND ".join(where_clauses)

    count_sql = f"""
        SELECT COUNT(*) FROM issues_fts fts
        {join_clause}
        WHERE {where_str}
    """
    total = db.execute(count_sql, params).fetchone()[0]

    results_sql = f"""
        SELECT
            i.id,
            i.volume,
            i.issue,
            i.pub_date,
            i.url,
            it.page_count,
            snippet(issues_fts, 0, '<mark>', '</mark>', '…', 48) AS snippet
        FROM issues_fts fts
        {join_clause}
        {left_join}
        WHERE {where_str}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """
    results = db.execute(results_sql, params + [per_page, offset]).fetchall()
    pages = max(1, (total + per_page - 1) // per_page)

    years = db.execute(
        "SELECT DISTINCT strftime('%Y', pub_date) as y FROM issues ORDER BY y"
    ).fetchall()

    return render_template("search.html",
        query=query, results=results, total=total,
        page=page, pages=pages, per_page=per_page,
        volume=volume, year=year,
        indexed=indexed, years=years,
    )


@app.route("/browse")
def browse():
    volume = request.args.get("vol", "1")
    db = get_db()

    issues = db.execute("""
        SELECT i.id, i.volume, i.issue, i.pub_date, i.url,
               it.page_count,
               CASE WHEN it.issue_id IS NOT NULL THEN 1 ELSE 0 END as indexed
        FROM issues i
        LEFT JOIN issue_text it ON it.issue_id = i.id
        WHERE i.volume = ?
        ORDER BY i.pub_date
    """, (volume,)).fetchall()

    volumes = db.execute(
        "SELECT DISTINCT volume FROM issues ORDER BY volume"
    ).fetchall()

    return render_template("browse.html", issues=issues, volumes=volumes, current_vol=int(volume))


@app.route("/issue/<int:issue_id>")
def issue_detail(issue_id):
    db = get_db()
    issue = db.execute("""
        SELECT i.*, it.full_text, it.page_count, it.indexed_at
        FROM issues i
        LEFT JOIN issue_text it ON it.issue_id = i.id
        WHERE i.id = ?
    """, (issue_id,)).fetchone()

    if not issue:
        return "Issue not found", 404

    return render_template("issue.html", issue=issue)


@app.route("/api/progress")
def api_progress():
    db = get_db()
    row = db.execute("""
        SELECT
            (SELECT COUNT(*) FROM issues)     AS total,
            (SELECT COUNT(*) FROM issue_text) AS indexed
    """).fetchone()
    return jsonify({"total": row["total"], "indexed": row["indexed"]})


if __name__ == "__main__":
    print("Black Panther Archive running at http://localhost:5050")
    app.run(port=5050, debug=False)

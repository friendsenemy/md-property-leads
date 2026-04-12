"""
Database layer for MD Property Leads.
Uses SQLite for simplicity and portability.
"""

import os
import sqlite3
import logging
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "leads.db")


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize the database schema."""
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS obituaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                first_name TEXT DEFAULT '',
                last_name TEXT DEFAULT '',
                middle_name TEXT DEFAULT '',
                date_of_death TEXT DEFAULT '',
                date_of_birth TEXT DEFAULT '',
                age INTEGER,
                city TEXT DEFAULT '',
                state TEXT DEFAULT 'MD',
                obituary_url TEXT DEFAULT '',
                obituary_text TEXT DEFAULT '',
                survived_by TEXT DEFAULT '',
                source TEXT DEFAULT '',
                scraped_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(full_name, date_of_death)
            );

            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                obituary_id INTEGER NOT NULL,
                owner_name TEXT DEFAULT '',
                property_address TEXT DEFAULT '',
                city TEXT DEFAULT '',
                county TEXT DEFAULT '',
                state TEXT DEFAULT 'MD',
                zip_code TEXT DEFAULT '',
                property_type TEXT DEFAULT '',
                assessed_value TEXT DEFAULT '',
                land_value TEXT DEFAULT '',
                improvement_value TEXT DEFAULT '',
                lot_size TEXT DEFAULT '',
                year_built TEXT DEFAULT '',
                account_number TEXT DEFAULT '',
                legal_description TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (obituary_id) REFERENCES obituaries(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                obituary_id INTEGER NOT NULL UNIQUE,
                status TEXT DEFAULT 'new',
                notes TEXT DEFAULT '',
                priority INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (obituary_id) REFERENCES obituaries(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS scrape_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                obituaries_found INTEGER DEFAULT 0,
                properties_matched INTEGER DEFAULT 0,
                leads_created INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                error TEXT DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_obituaries_name ON obituaries(last_name, first_name);
            CREATE INDEX IF NOT EXISTS idx_obituaries_dod ON obituaries(date_of_death);
            CREATE INDEX IF NOT EXISTS idx_properties_obit ON properties(obituary_id);
            CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
            CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at);
        """)
    logger.info("Database initialized")


def insert_obituary(obit):
    """Insert an obituary record. Returns the ID or None if duplicate."""
    with get_db() as conn:
        try:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO obituaries
                (full_name, first_name, last_name, middle_name,
                 date_of_death, date_of_birth, age, city, state,
                 obituary_url, obituary_text, survived_by, source, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                obit.get("full_name", ""),
                obit.get("first_name", ""),
                obit.get("last_name", ""),
                obit.get("middle_name", ""),
                obit.get("date_of_death", ""),
                obit.get("date_of_birth", ""),
                obit.get("age"),
                obit.get("city", ""),
                obit.get("state", "MD"),
                obit.get("obituary_url", ""),
                obit.get("obituary_text", ""),
                obit.get("survived_by", ""),
                obit.get("source", ""),
                obit.get("scraped_at", ""),
            ))
            if cursor.rowcount > 0:
                return cursor.lastrowid
            return None
        except Exception:
            return None


def insert_property(obituary_id, prop):
    """Insert a property record linked to an obituary."""
    with get_db() as conn:
        cursor = conn.execute("""
            INSERT INTO properties
            (obituary_id, owner_name, property_address, city, county, state,
             zip_code, property_type, assessed_value, land_value,
             improvement_value, lot_size, year_built, account_number,
             legal_description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            obituary_id,
            prop.get("owner_name", ""),
            prop.get("property_address", ""),
            prop.get("city", ""),
            prop.get("county", ""),
            prop.get("state", "MD"),
            prop.get("zip_code", ""),
            prop.get("property_type", ""),
            prop.get("assessed_value", ""),
            prop.get("land_value", ""),
            prop.get("improvement_value", ""),
            prop.get("lot_size", ""),
            prop.get("year_built", ""),
            prop.get("account_number", ""),
            prop.get("legal_description", ""),
        ))
        return cursor.lastrowid


def create_lead(obituary_id):
    """Create a lead entry for a matched obituary."""
    with get_db() as conn:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO leads (obituary_id, status)
                VALUES (?, 'new')
            """, (obituary_id,))
        except Exception:
            pass


def get_leads(status=None, search=None, sort_by="created_at", sort_dir="desc",
              page=1, per_page=25):
    """
    Get leads with full obituary and property info.
    Returns (list_of_leads, total_count).
    """
    with get_db() as conn:
        where_clauses = []
        params = []

        if status and status != "all":
            where_clauses.append("l.status = ?")
            params.append(status)

        if search:
            where_clauses.append(
                "(o.full_name LIKE ? OR p.property_address LIKE ? OR p.county LIKE ?)"
            )
            params.extend(["%" + search + "%"] * 3)

        where_sql = " AND ".join(where_clauses)
        if where_sql:
            where_sql = "WHERE " + where_sql

        allowed_sorts = {
            "created_at": "l.created_at",
            "name": "o.full_name",
            "date_of_death": "o.date_of_death",
            "assessed_value": "CAST(p.assessed_value AS REAL)",
            "county": "p.county",
            "status": "l.status",
        }
        sort_col = allowed_sorts[sort_by] || "l.created_at"
        sort_direction = sort_dir.toLowerCase() === "desc" ? "DESC" : "ASC"

        count_sql = "SELECT COUNT(DISTINCT l.id) FROM leads l JOIN obituaries o ON l.obituary_id = o.id LEFT JOIN properties p ON o.id = p.obituary_id " + where_sql
        total = conn.execute(count_sql, params).fetchone()[0]

        offset = (page - 1) * per_page
        query = "SELECT l.id as lead_id, l.status, l.notes, l.priority, l.created_at as lead_created_at, l.updated_at, o.id as obituary_id, o.full_name, o.first_name, o.last_name, o.middle_name, o.date_of_death, o.date_of_birth, o.age, o.city as obit_city, o.obituary_url, o.obituary_text, o.survived_by, o.source, o.scraped_at FROM leads l JOIN obituaries o ON l.obituary_id = o.id " + where_sql + " GROUP BY l.id ORDER BY " + sort_col + " " + sort_direction + " LIMIT ? OFFSET ?"
        params.extend([per_page, offset])
        rows = conn.execute(query, params).fetchall()

        leads = []
        for row in rows:
            lead = dict(row)
            props = conn.execute("SELECT * FROM properties WHERE obituary_id = ?", (lead["obituary_id"],)).fetchall()
            lead["properties"] = [dict(p) for p in props]
            leads.append(lead)

        return leads, total


def update_lead_status(lead_id, status, notes=None):
    """Update lead status and optional notes."""
    with get_db() as conn:
        if notes is not None:
            conn.execute("""
                UPDATE leads SET status = ?, notes = ?,
                updated_at = datetime('now') WHERE id = ?
            """, (status, notes, lead_id))
        else:
            conn.execute("""
                UPDATE leads SET status = ?,
                updated_at = datetime('now') WHERE id = ?
            """, (status, lead_id))


def get_leads_for_export(status=None):
    """Get all leads formatted for skip tracing export."""
    with get_db() as conn:
        where = "WHERE l.status = ?" if status and status != "all" else ""
        params = [status] if status and status != "all" else []

        rows = conn.execute("""
            SELECT
                o.first_name, o.last_name, o.middle_name,
                o.full_name, o.date_of_death, o.date_of_birth, o.age,
                o.city as obit_city, o.state as obit_state,
                o.obituary_url, o.survived_by,
                p.property_address, p.city, p.county, p.state, p.zip_code,
                p.property_type, p.assessed_value, p.land_value,
                p.improvement_value, p.lot_size, p.year_built,
                p.account_number,
                l.status, l.notes,
                l.created_at as lead_date
            FROM leads l
            JOIN obituaries o ON l.obituary_id = o.id
            LEFT JOIN properties p ON o.id = p.obituary_id
            """ + where + """
            ORDER BY l.created_at DESC
        """, params).fetchall()

        return [dict(r) for r in rows]


def get_stats():
    """Get dashboard statistics."""
    with get_db() as conn:
        stats = {}
        stats["total_leads"] = conn.execute(
            "SELECT COUNT(*) FROM leads"
        ).fetchone()[0]
        stats["new_leads"] = conn.execute(
            "SELECT COUNT(*) FROM leads WHERE status = 'new'"
        ).fetchone()[0]
        stats["contacted"] = conn.execute(
            "SELECT COUNT(*) FROM leads WHERE status = 'contacted'"
        ).fetchone()[0]
        stats["hot"] = conn.execute(
            "SELECT COUNT(*) FROM leads WHERE status = 'hot'"
        ).fetchone()[0]
        stats["closed"] = conn.execute(
            "SELECT COUNT(*) FROM leads WHERE status = 'closed'"
        ).fetchone()[0]
        stats["total_properties"] = conn.execute(
            "SELECT COUNT(*) FROM properties"
        ).fetchone()[0]
        stats["total_obituaries"] = conn.execute(
            "SELECT COUNT(*) FROM obituaries"
        ).fetchone()[0]

        last_scrape = conn.execute("""
            SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1
        """).fetchone()
        stats["last_scrape"] = dict(last_scrape) if last_scrape else None

        county_rows = conn.execute("""
            SELECT p.county, COUNT(DISTINCT l.id) as count
            FROM leads l
            JOIN obituaries o ON l.obituary_id = o.id
            LEFT JOIN properties p ON o.id = p.obituary_id
            GROUP BY p.county
            ORDER BY count DESC
        """).fetchall()
        stats["by_county"] = [dict(r) for r in county_rows]

        daily_rows = conn.execute("""
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM leads
            WHERE created_at >= datetime('now', '-7 days')
            GROUP BY DATE(created_at)
            ORDER BY date
        """).fetchall()
        stats["daily_leads"] = [dict(r) for r in daily_rows]

        return stats


def log_scrape_start():
    """Log the start of a scrape run."""
    with get_db() as conn:
        cursor = conn.execute("""
            INSERT INTO scrape_log (started_at, status)
            VALUES (datetime('now'), 'running')
        """)
        return cursor.lastrowid


def log_scrape_end(log_id, obits_found, props_matched, leads_created,
                   status="completed", error=""):
    """Log the end of a scrape run."""
    with get_db() as conn:
        conn.execute("""
            UPDATE scrape_log SET
                completed_at = datetime('now'),
                obituaries_found = ?,
                properties_matched = ?,
                leads_created = ?,
                status = ?,
                error = ?
            WHERE id = ?
        """, (obits_found, props_matched, leads_created, status, error, log_id))

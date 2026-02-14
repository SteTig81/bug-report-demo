
import sqlite3
from pathlib import Path

DB_PATH = Path("graph_system.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def initialize_database():
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS elements (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS element_versions (
        id TEXT PRIMARY KEY,
        element_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        variant TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (element_id, version, variant),
        FOREIGN KEY (element_id) REFERENCES elements(id)
    );

    CREATE TABLE IF NOT EXISTS element_version_predecessors (
        version_id TEXT NOT NULL,
        predecessor_id TEXT NOT NULL,
        PRIMARY KEY (version_id, predecessor_id),
        FOREIGN KEY (version_id) REFERENCES element_versions(id),
        FOREIGN KEY (predecessor_id) REFERENCES element_versions(id)
    );

    CREATE TABLE IF NOT EXISTS element_version_dependencies (
        parent_version_id TEXT NOT NULL,
        child_version_id TEXT NOT NULL,
        PRIMARY KEY (parent_version_id, child_version_id),
        FOREIGN KEY (parent_version_id) REFERENCES element_versions(id),
        FOREIGN KEY (child_version_id) REFERENCES element_versions(id)
    );

    CREATE TABLE IF NOT EXISTS tickets (
        id TEXT PRIMARY KEY,
        type TEXT CHECK(type IN ('bug','bugfix')) NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        fixes_ticket_id TEXT,
        FOREIGN KEY (fixes_ticket_id) REFERENCES tickets(id)
    );

    CREATE TABLE IF NOT EXISTS fix_neutralises (
        fix_id TEXT NOT NULL,
        bug_id TEXT NOT NULL,
        PRIMARY KEY (fix_id, bug_id),
        FOREIGN KEY (fix_id) REFERENCES tickets(id),
        FOREIGN KEY (bug_id) REFERENCES tickets(id)
    );

    CREATE TABLE IF NOT EXISTS ticket_versions (
        ticket_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        PRIMARY KEY (ticket_id, version_id),
        FOREIGN KEY (ticket_id) REFERENCES tickets(id),
        FOREIGN KEY (version_id) REFERENCES element_versions(id)
    );
    """)

    conn.commit()
    conn.close()

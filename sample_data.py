
from db import get_connection
import logging

logger = logging.getLogger(__name__)

def create_sample_data():
    conn = get_connection()
    cur = conn.cursor()

    elements = [
        ("E_APP", "Application"),
        ("E_LIB", "Library"),
        ("E_UTIL", "Utility")
    ]
    cur.executemany("INSERT OR IGNORE INTO elements VALUES (?,?)", elements)

    versions = [
        ("APP_v1", "E_APP", 1, "A"),
        ("APP_v2", "E_APP", 2, "A"),
        ("LIB_v1", "E_LIB", 1, "A"),
        ("LIB_v2", "E_LIB", 2, "A"),
        ("LIB_v3", "E_LIB", 3, "A"),
        ("UTIL_v1", "E_UTIL", 1, "A")
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO element_versions(id, element_id, version, variant) VALUES (?,?,?,?)",
        versions
    )

    history = [
        ("APP_v2", "APP_v1"),
        ("LIB_v2", "LIB_v1"),
        ("LIB_v3", "LIB_v2")
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO element_version_predecessors VALUES (?,?)",
        history
    )

    deps = [
        ("APP_v2", "LIB_v3"),
        ("APP_v2", "UTIL_v1"),
        ("LIB_v3", "UTIL_v1"),
        ("APP_v1", "LIB_v1"),
        ("APP_v1", "UTIL_v1")
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO element_version_dependencies VALUES (?,?)",
        deps
    )

    tickets = [
        ("BUG1", "bug", "Memory corruption", "After 5 minutes there is a memory leak.", None),
        ("BUG2", "bug", "Utility crash", "When clicking Print button the application crashes.", None),
        ("FIX1", "bugfix", "Fix memory corruption", "Added proper free() calls.", "BUG1")
    ]
    # Use an UPSERT so re-running the sample generator updates existing rows
    cur.executemany(
        """
        INSERT INTO tickets(id,type,title,description,fixes_ticket_id)
        VALUES (?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            type=excluded.type,
            title=excluded.title,
            description=excluded.description,
            fixes_ticket_id=excluded.fixes_ticket_id;
        """,
        tickets
    )

    ticket_versions = [
        ("BUG1", "LIB_v1"),
        ("BUG2", "UTIL_v1"),
        ("FIX1", "LIB_v3")
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO ticket_versions VALUES (?,?)",
        ticket_versions
    )

    # Map fixes to the bugs they neutralise (support multiple neutralisations)
    fix_neutralises = [
        ("FIX1", "BUG1"),
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO fix_neutralises VALUES (?,?)",
        fix_neutralises
    )

    conn.commit()
    conn.close()
    logger.info("Inserted sample elements=%d versions=%d tickets=%d", len(elements), len(versions), len(tickets))

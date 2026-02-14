
import json
from collections import defaultdict
from pathlib import Path
from db import get_connection
import logging

logger = logging.getLogger(__name__)

def history_closure(conn, version_id):
    sql = """
    WITH RECURSIVE history(id) AS (
        SELECT ?
        UNION
        SELECT predecessor_id
        FROM element_version_predecessors
        JOIN history ON version_id = history.id
    )
    SELECT id FROM history;
    """
    rows = [r["id"] for r in conn.execute(sql, (version_id,))]
    logger.debug("history_closure: start=%s count=%d nodes=%s", version_id, len(rows), rows)
    return set(rows)

def active_bugs(conn, version_id):
    history = history_closure(conn, version_id)
    introduced = {}
    fixes = set()

    for row in conn.execute("""
        SELECT t.id, t.type, t.fixes_ticket_id, tv.version_id, t.title, t.description
        FROM tickets t
        JOIN ticket_versions tv ON t.id = tv.ticket_id
    """):
        if row["version_id"] in history:
            if row["type"] == "bug":
                introduced[row["id"]] = {
                    "id": row["id"],
                    "title": row["title"],
                    "description": row["description"]
                }
                logger.debug("active_bugs: version=%s introduced_bug=%s", version_id, row["id"])
            elif row["type"] == "bugfix" and row["fixes_ticket_id"]:
                fixes.add(row["fixes_ticket_id"])
                logger.debug("active_bugs: version=%s bugfix=%s neutralises=%s", version_id, row["id"], row["fixes_ticket_id"])

    neutralised = [i for i in introduced.keys() if i in fixes]
    if neutralised:
        logger.info("active_bugs: version=%s neutralised_bugs=%s", version_id, neutralised)

    active = [b for i, b in introduced.items() if i not in fixes]
    logger.debug("active_bugs: version=%s active_count=%d active_ids=%s", version_id, len(active), [b["id"] for b in active])
    return active

def containment_tree(conn, root):
    sql = """
    WITH RECURSIVE deps(id, parent) AS (
        SELECT ?, NULL
        UNION
        SELECT d.child_version_id, d.parent_version_id
        FROM element_version_dependencies d
        JOIN deps ON d.parent_version_id = deps.id
    )
    SELECT id, parent FROM deps;
    """
    nodes = list(conn.execute(sql, (root,)))
    logger.debug("containment_tree: root=%s nodes_found=%d", root, len(nodes))
    children = defaultdict(list)
    for n in nodes:
        if n["parent"]:
            children[n["parent"]].append(n["id"])
    return children

def build_tree(root):
    conn = get_connection()
    children = containment_tree(conn, root)

    def build(node):
        logger.debug("build: entering node=%s children=%s", node, children.get(node, []))
        bugs = active_bugs(conn, node)
        subnodes = [build(c) for c in children.get(node, [])]
        summary = {
            "elements": 1 + sum(s["summary"]["elements"] for s in subnodes),
            "bugs": len(bugs) + sum(s["summary"]["bugs"] for s in subnodes)
        }
        logger.info("build: node=%s elements=%d bugs=%d", node, summary["elements"], summary["bugs"])
        return {
            "version": node,
            "active_bugs": bugs,
            "children": subnodes,
            "summary": summary
        }

    tree = build(root)
    conn.close()
    return tree

def export_json(data, filename):
    Path(filename).write_text(json.dumps(data, indent=2))

def export_html(data, filename, title):
    def render(node):
        html = f"<li>{node['version']} (bugs: {node['summary']['bugs']})"
        # Render active bug details
        if node.get("active_bugs"):
            html += "<ul>"
            for bug in node["active_bugs"]:
                desc = bug.get("description") or ""
                html += f"<li><strong>{bug['id']}</strong>: {bug['title']} - {desc}</li>"
            html += "</ul>"

        # Render child nodes
        if node["children"]:
            html += "<ul>"
            for c in node["children"]:
                html += render(c)
            html += "</ul>"
        html += "</li>"
        return html

    html = f"""
    <html>
    <head><title>{title}</title></head>
    <body>
    <h1>{title}</h1>
    <ul>{render(data)}</ul>
    </body>
    </html>
    """
    Path(filename).write_text(html)

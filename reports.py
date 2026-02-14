
import json
from collections import defaultdict
from pathlib import Path
from db import get_connection

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
    return {r["id"] for r in conn.execute(sql, (version_id,))}

def active_bugs(conn, version_id):
    history = history_closure(conn, version_id)
    introduced = set()
    fixes = {}

    for row in conn.execute("""
        SELECT t.id, t.type, t.fixes_ticket_id, tv.version_id
        FROM tickets t
        JOIN ticket_versions tv ON t.id = tv.ticket_id
    """):
        if row["version_id"] in history:
            if row["type"] == "bug":
                introduced.add(row["id"])
            elif row["type"] == "bugfix":
                fixes[row["fixes_ticket_id"]] = True

    return [b for b in introduced if b not in fixes]

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
    children = defaultdict(list)
    for n in nodes:
        if n["parent"]:
            children[n["parent"]].append(n["id"])
    return children

def build_tree(root):
    conn = get_connection()
    children = containment_tree(conn, root)

    def build(node):
        bugs = active_bugs(conn, node)
        subnodes = [build(c) for c in children.get(node, [])]
        summary = {
            "elements": 1 + sum(s["summary"]["elements"] for s in subnodes),
            "bugs": len(bugs) + sum(s["summary"]["bugs"] for s in subnodes)
        }
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

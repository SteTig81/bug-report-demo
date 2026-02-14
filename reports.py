
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


def containment_nodes(conn, root):
    sql = """
    WITH RECURSIVE deps(id, parent) AS (
        SELECT ?, NULL
        UNION
        SELECT d.child_version_id, d.parent_version_id
        FROM element_version_dependencies d
        JOIN deps ON d.parent_version_id = deps.id
    )
    SELECT id FROM deps;
    """
    return {r["id"] for r in conn.execute(sql, (root,))}


def get_element_info(conn, version_id):
    row = conn.execute("SELECT element_id, version FROM element_versions WHERE id = ?", (version_id,)).fetchone()
    if row:
        return row["element_id"], row["version"]
    return None, None

def build_tree(current_root, predecessor_root=None):
    conn = get_connection()
    children = containment_tree(conn, current_root)

    predecessor_nodes = None
    if predecessor_root:
        predecessor_nodes = containment_nodes(conn, predecessor_root)
        logger.info("build_tree: current_root=%s predecessor_root=%s predecessor_nodes=%d", current_root, predecessor_root, len(predecessor_nodes))

    def compute_interval_for_element(node):
        element_id, _ = get_element_info(conn, node)
        pred_version = None
        if predecessor_nodes and element_id:
            preds = list(predecessor_nodes)
            placeholders = ",".join("?" for _ in preds)
            sql = f"SELECT id, version FROM element_versions WHERE element_id = ? AND id IN ({placeholders}) ORDER BY version DESC LIMIT 1"
            params = [element_id] + preds
            row = conn.execute(sql, params).fetchone()
            if row:
                pred_version = row["id"]

        introduced = []
        fixes = []
        # Only compute interval changes if we found a predecessor version for this element
        if pred_version:
            node_hist = history_closure(conn, node)
            pred_hist = history_closure(conn, pred_version)
            interval_nodes = node_hist - pred_hist
            if interval_nodes:
                preds = list(interval_nodes)
                placeholders = ",".join("?" for _ in preds)
                sql = f"SELECT t.id, t.type, t.fixes_ticket_id, tv.version_id, t.title, t.description FROM tickets t JOIN ticket_versions tv ON t.id = tv.ticket_id WHERE tv.version_id IN ({placeholders})"
                for row in conn.execute(sql, preds):
                    if row["type"] == "bug":
                        introduced.append({"id": row["id"], "title": row["title"], "description": row["description"]})
                    elif row["type"] == "bugfix":
                        fixes.append({"id": row["id"], "title": row["title"], "description": row["description"], "neutralises": row["fixes_ticket_id"]})

        logger.debug("compute_interval_for_element: node=%s pred_version=%s introduced=%d fixes=%d", node, pred_version, len(introduced), len(fixes))
        return pred_version, {"introduced": introduced, "fixes": fixes}

    def build(node):
        logger.debug("build: entering node=%s children=%s", node, children.get(node, []))
        bugs = active_bugs(conn, node)
        subnodes = [build(c) for c in children.get(node, [])]
        summary = {
            "elements": 1 + sum(s["summary"]["elements"] for s in subnodes),
            "bugs": len(bugs) + sum(s["summary"]["bugs"] for s in subnodes)
        }
        logger.info("build: node=%s elements=%d bugs=%d", node, summary["elements"], summary["bugs"])
        node_entry = {
            "version": node,
            "active_bugs": bugs,
            "children": subnodes,
            "summary": summary
        }
        if predecessor_root is not None:
            pred_ver, changes = compute_interval_for_element(node)
            node_entry["predecessor_version"] = pred_ver
            node_entry["since_predecessor"] = changes
            node_entry["version_not_updated"] = (pred_ver == node)
        return node_entry

    tree = build(current_root)
    conn.close()
    return tree

def export_json(data, filename):
    Path(filename).write_text(json.dumps(data, indent=2))

def export_html(data, filename, title):
    def render(node):
        html = f"<li><strong>{node['version']}</strong> (bugs: {node['summary']['bugs']})"

        # Predecessor mapping for this element (if computed)
        if "version_not_updated" in node and node.get("version_not_updated"):
            html += "<div style='margin-left:8px'><em>Hint:</em> Version was not updated.</div>"
        elif "predecessor_version" in node:
            pred = node["predecessor_version"] or "(none)"
            html += f"<div style='margin-left:8px'><em>Predecessor:</em> {pred}</div>"

            # Changes since predecessor for this element
            if node.get("since_predecessor"):
                ch = node["since_predecessor"]
                html += "<div style='margin-left:8px'><em>Since predecessor:</em>"
                # Introduced bugs
                html += "<div><strong>Introduced:</strong><ul>"
                if ch.get("introduced"):
                    for b in ch["introduced"]:
                        html += f"<li>{b['id']}: {b['title']} - {b.get('description','')}</li>"
                else:
                    html += "<li>(none)</li>"
                html += "</ul></div>"
                # Fixes
                html += "<div><strong>Fixes:</strong><ul>"
                if ch.get("fixes"):
                    for f in ch["fixes"]:
                        html += f"<li>{f['id']} (neutralises {f.get('neutralises')}): {f['title']}</li>"
                else:
                    html += "<li>(none)</li>"
                html += "</ul></div>"
                html += "</div>"

        # Active bug details (current active bugs on this version)
        if node.get("active_bugs"):
            html += "<div style='margin-left:8px'><strong>Active bugs:</strong><ul>"
            for bug in node["active_bugs"]:
                desc = bug.get("description") or ""
                html += f"<li>{bug['id']}: {bug['title']} - {desc}</li>"
            html += "</ul></div>"

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

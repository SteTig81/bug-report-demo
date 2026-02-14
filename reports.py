
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


def get_element_display_name(conn, version_id):
    """Get formatted display: Name Version - Variant (Id: version_id)."""
    row = conn.execute("""
        SELECT ev.id, ev.version, ev.variant, e.name
        FROM element_versions ev
        JOIN elements e ON ev.element_id = e.id
        WHERE ev.id = ?
    """, (version_id,)).fetchone()
    if row:
        return f"{row['name']} {row['version']} - {row['variant']}\n    Id: {row['id']}"
    return version_id

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
    conn = get_connection()
    all_versions = {}  # version_id -> node for collecting all versions

    def collect_all_versions(node):
        """Traverse tree and collect all unique versions."""
        version_id = node["version"]
        if version_id not in all_versions:
            all_versions[version_id] = node
        for child in node.get("children", []):
            collect_all_versions(child)

    def render_bom(node):
        """Render BOM (hierarchy only, no details)."""
        display = get_element_display_name(conn, node["version"])
        html = f"<li>{display}"
        if node["children"]:
            html += "<ul>"
            for c in node["children"]:
                html += render_bom(c)
            html += "</ul>"
        html += "</li>"
        return html

    def render_bug_report(all_versions):
        """Render flat list of all active bugs, with [new] for introduced bugs and fixed list."""
        html = ""
        
        # Collect bugs by version, track which are new and which are fixed
        bugs_by_version = {}
        all_introduced_ids = set()
        all_fixed_ids = set()
        
        for version_id in all_versions.keys():
            node = all_versions[version_id]
            if node.get("active_bugs"):
                bugs_by_version[version_id] = node["active_bugs"]
            # Collect introduced and fixed bugs
            if node.get("since_predecessor"):
                for b in node["since_predecessor"].get("introduced", []):
                    all_introduced_ids.add(b["id"])
                for f in node["since_predecessor"].get("fixes", []):
                    all_fixed_ids.add(f["neutralises"])
        
        # Render active bugs with [new] decorator
        if bugs_by_version:
            for version_id in sorted(bugs_by_version.keys()):
                bugs = bugs_by_version[version_id]
                display = get_element_display_name(conn, version_id)
                html += f"<li>{display}<ul>"
                for bug in bugs:
                    is_new = " [new]" if bug["id"] in all_introduced_ids else ""
                    desc = bug.get("description") or ""
                    html += f"<li>{bug['id']}: {bug['title']} - {desc}{is_new}</li>"
                html += "</ul></li>"
        else:
            html = "<li>(no active bugs)</li>"
        
        # Render fixed bugs list if any
        if all_fixed_ids:
            html += "<li><strong>Fixed since predecessor</strong><ul>"
            for bug_id in sorted(all_fixed_ids):
                html += f"<li>{bug_id}</li>"
            html += "</ul></li>"
        
        return html

    def render_detailed_element_version(node):
        """Render details for a single element version."""
        display = get_element_display_name(conn, node["version"])
        html = f"<li id='{node['version']}'>{display} (bugs: {node['summary']['bugs']})"
        
        # Predecessor and changes
        if "version_not_updated" in node and node.get("version_not_updated"):
            html += "<div style='margin-left:8px'><em>Hint:</em> Version was not updated.</div>"
        elif "predecessor_version" in node:
            pred_display = get_element_display_name(conn, node["predecessor_version"]) if node["predecessor_version"] else "(none)"
            html += f"<div style='margin-left:8px'><em>Predecessor:</em><br/>{pred_display}</div>"
            if node.get("since_predecessor"):
                ch = node["since_predecessor"]
                html += "<div style='margin-left:8px'><em>Since predecessor:</em>"
                html += "<div><strong>Introduced:</strong><ul>"
                if ch.get("introduced"):
                    for b in ch["introduced"]:
                        html += f"<li>{b['id']}: {b['title']} - {b.get('description','')}</li>"
                else:
                    html += "<li>(none)</li>"
                html += "</ul></div>"
                html += "<div><strong>Fixes:</strong><ul>"
                if ch.get("fixes"):
                    for f in ch["fixes"]:
                        html += f"<li>{f['id']} (neutralises {f.get('neutralises')}): {f['title']}</li>"
                else:
                    html += "<li>(none)</li>"
                html += "</ul></div>"
                html += "</div>"
        
        # Active bugs
        if node.get("active_bugs"):
            html += "<div style='margin-left:8px'><strong>Active bugs:</strong><ul>"
            for bug in node["active_bugs"]:
                desc = bug.get("description") or ""
                html += f"<li>{bug['id']}: {bug['title']} - {desc}</li>"
            html += "</ul></div>"
        
        html += "</li>"
        return html

    # Collect all versions
    collect_all_versions(data)

    # Render sections
    bom_html = render_bom(data)
    bug_report_html = render_bug_report(all_versions)
    
    detailed_html = "<ul>"
    for version_id in sorted(all_versions.keys()):
        detailed_html += render_detailed_element_version(all_versions[version_id])
    detailed_html += "</ul>"

    html = f"""
    <html>
    <head><title>{title}</title></head>
    <body>
    <h1>{title}</h1>
    
    <h2>1. Bill of Materials (BOM)</h2>
    <ul>{bom_html}</ul>
    
    <h2>2. Bug Report</h2>
    <ul>{bug_report_html}</ul>
    
    <h2>3. Detailed Element Version Report</h2>
    {detailed_html}
    
    </body>
    </html>
    """
    Path(filename).write_text(html)
    conn.close()

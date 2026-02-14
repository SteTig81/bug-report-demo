
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
        return f"{row['name']} {row['version']} - {row['variant']} (Id: {row['id']})"
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
    all_bugs = {}  # bug_id -> {"title", "description", "versions": [version_ids]}

    def collect_all_versions(node):
        """Traverse tree and collect all unique versions and bugs."""
        version_id = node["version"]
        if version_id not in all_versions:
            all_versions[version_id] = node
            # Collect active bugs from this version
            for bug in node.get("active_bugs", []):
                if bug["id"] not in all_bugs:
                    all_bugs[bug["id"]] = {"title": bug["title"], "description": bug.get("description", ""), "versions": []}
                if version_id not in all_bugs[bug["id"]]["versions"]:
                    all_bugs[bug["id"]]["versions"].append(version_id)
        for child in node.get("children", []):
            collect_all_versions(child)
    
    def collect_aggregated_bugs(node):
        """Collect all active bugs from this node and all descendant nodes, deduped by bug_id."""
        aggregated = {}
        # Add bugs from this node
        for bug in node.get("active_bugs", []):
            if bug["id"] not in aggregated:
                aggregated[bug["id"]] = bug
        # Add bugs from children
        for child in node.get("children", []):
            child_bugs = collect_aggregated_bugs(child)
            for bug_id, bug_info in child_bugs.items():
                if bug_id not in aggregated:
                    aggregated[bug_id] = bug_info
        return aggregated

    def render_bom(node):
        """Render BOM (hierarchy only, no details)."""
        version_id = node["version"]
        display = get_element_display_name(conn, version_id)
        # Hyperlink the element ID
        display_with_link = display.replace(f"(Id: {version_id})", f"(Id: <a href='#{version_id}'>{version_id}</a>)")
        html = f"<li>{display_with_link}"
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
        version_id = node["version"]
        display = get_element_display_name(conn, version_id)
        html = f"<li id='{version_id}'>{display}"
        
        # Predecessor and changes
        if "version_not_updated" in node and node.get("version_not_updated"):
            html += "<div style='margin-left:8px'><em>Hint:</em> Version was not updated.</div>"
        elif "predecessor_version" in node:
            if node["predecessor_version"]:
                pred_display = get_element_display_name(conn, node["predecessor_version"])
                html += f"<div style='margin-left:8px'><em>Predecessor:</em><br/>{pred_display}</div>"
            else:
                html += "<div style='margin-left:8px'><em>Predecessor:</em><br/>(none)</div>"
            if node.get("since_predecessor"):
                ch = node["since_predecessor"]
                html += "<div style='margin-left:8px'><em>Since predecessor:</em>"
                html += "<div><strong>Introduced:</strong><ul>"
                if ch.get("introduced"):
                    for b in ch["introduced"]:
                        html += f"<li><a href='#bug-{b['id']}'>{b['id']}</a>: {b['title']} - {b.get('description','')}</li>"
                else:
                    html += "<li>(none)</li>"
                html += "</ul></div>"
                html += "<div><strong>Fixes:</strong><ul>"
                if ch.get("fixes"):
                    for f in ch["fixes"]:
                        neutralises_link = f"<a href='#bug-{f.get('neutralises')}'>{f.get('neutralises')}</a>"
                        html += f"<li><a href='#bug-{f['id']}'>{f['id']}</a> (neutralises {neutralises_link}): {f['title']}</li>"
                else:
                    html += "<li>(none)</li>"
                html += "</ul></div>"
                html += "</div>"
        
        return html

    # Collect all versions
    collect_all_versions(data)

    # Render sections
    bom_html = render_bom(data)
    bug_report_html = render_bug_report(all_versions)
    
    detailed_html = "<ul>"
    for version_id in sorted(all_versions.keys()):
        node = all_versions[version_id]
        html_entry = render_detailed_element_version(node)
        # Collect bugs: direct bugs in this element vs inherited from children
        direct_bugs = {}
        for bug in node.get("active_bugs", []):
            if bug["id"] not in direct_bugs:
                direct_bugs[bug["id"]] = bug
        
        inherited_bugs = {}
        for child in node.get("children", []):
            child_bugs = collect_aggregated_bugs(child)
            for bug_id, bug_info in child_bugs.items():
                if bug_id not in inherited_bugs and bug_id not in direct_bugs:
                    inherited_bugs[bug_id] = bug_info
        
        total_bugs = len(direct_bugs) + len(inherited_bugs)
        
        if total_bugs > 0:
            html_entry += "<div style='margin-left:8px'>"
            
            # Segment 1: Direct bugs in this element
            if direct_bugs:
                html_entry += "<div><strong>Active bugs in this element:</strong><ul>"
                for bug in sorted(direct_bugs.values(), key=lambda b: b.get("id", "")):
                    html_entry += f"<li><a href='#bug-{bug['id']}'>{bug['id']}</a> | {bug['title']}</li>"
                html_entry += "</ul></div>"
            
            # Segment 2: Inherited bugs from children
            if inherited_bugs:
                html_entry += "<div><strong>Active bugs in child elements:</strong><ul>"
                for bug in sorted(inherited_bugs.values(), key=lambda b: b.get("id", "")):
                    # Find which version this bug comes from for display
                    bug_source_version_id = None
                    for v_id in all_versions:
                        for active_bug in all_versions[v_id].get("active_bugs", []):
                            if active_bug["id"] == bug["id"]:
                                bug_source_version_id = v_id
                                break
                        if bug_source_version_id:
                            break
                    if bug_source_version_id:
                        bug_source_display = get_element_display_name(conn, bug_source_version_id)
                        bug_source_display = bug_source_display.replace(f"(Id: {bug_source_version_id})", f"(Id: <a href='#{bug_source_version_id}'>{bug_source_version_id}</a>)")
                        html_entry += f"<li><a href='#bug-{bug['id']}'>{bug['id']}</a> | {bug_source_display} | {bug['title']}</li>"
                    else:
                        html_entry += f"<li><a href='#bug-{bug['id']}'>{bug['id']}</a> | {bug['title']}</li>"
                html_entry += "</ul></div>"
            
            # Segment 3: Total count
            html_entry += f"<div><strong>Total aggregated: {total_bugs} active bug(s)</strong></div>"
            html_entry += "</div>"
        
        html_entry += "</li>"  # Close the li tag
        detailed_html += html_entry
    detailed_html += "</ul>"
    
    # Render bug details section
    bug_details_html = "<ul>"
    for bug_id in sorted(all_bugs.keys()):
        bug_info = all_bugs[bug_id]
        bug_details_html += f"<li id='bug-{bug_id}'><strong>{bug_id}</strong><div>Title: {bug_info['title']}</div><div>Description: {bug_info['description']}</div></li>"
    bug_details_html += "</ul>"

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
    
    <h2>4. Bug Details</h2>
    {bug_details_html}
    
    </body>
    </html>
    """
    Path(filename).write_text(html)
    conn.close()

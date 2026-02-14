
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
    
    def find_bug_path(node, bug_id):
        """Find the path from node down to where bug_id exists. Returns list of version_ids."""
        # Check if bug is directly in this node
        for bug in node.get("active_bugs", []):
            if bug["id"] == bug_id:
                return [node["version"]]
        # Check children recursively
        for child in node.get("children", []):
            path = find_bug_path(child, bug_id)
            if path:
                return [node["version"]] + path
        return None

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
        all_active_ids = set()

        for version_id in all_versions.keys():
            node = all_versions[version_id]
            if node.get("active_bugs"):
                bugs_by_version[version_id] = node["active_bugs"]
                for b in node["active_bugs"]:
                    all_active_ids.add(b["id"])
            # Collect introduced and fixed bugs
            if node.get("since_predecessor"):
                for b in node["since_predecessor"].get("introduced", []):
                    all_introduced_ids.add(b["id"])
                for f in node["since_predecessor"].get("fixes", []):
                    if f.get("neutralises"):
                        all_fixed_ids.add(f["neutralises"])

        # Render active bugs grouped by element (with linked element ids)
        if bugs_by_version:
            for version_id in sorted(bugs_by_version.keys()):
                bugs = bugs_by_version[version_id]
                display = get_element_display_name(conn, version_id)
                display = display.replace(f"(Id: {version_id})", f"(Id: <a href='#{version_id}'>{version_id}</a>)")
                html += f"<li>{display}<ul>"
                for bug in bugs:
                    is_new = " [new]" if bug["id"] in all_introduced_ids else ""
                    # Sub-entry format: <ID> | <title>
                    html += f"<li><a href='#bug-{bug['id']}'>{bug['id']}</a> | {bug['title']}{is_new}</li>"
                html += "</ul></li>"
        else:
            html = "<li>(no active bugs)</li>"

        # Render fixed bugs list if any (linked)
        if all_fixed_ids:
            html += "<li>Fixed since predecessor<ul>"
            for bug_id in sorted(all_fixed_ids):
                html += f"<li><a href='#bug-{bug_id}'>{bug_id}</a></li>"
            html += "</ul></li>"

        # Add aggregated totals (deduped)
        total_active = len(all_active_ids)
        total_fixed = len(all_fixed_ids)
        html += f"<li>Total aggregated active bugs: {total_active}</li>"
        html += f"<li>Total aggregated fixed bugs: {total_fixed}</li>"

        return html

    def render_detailed_element_version(node):
        """Render header and predecessor line for a single element version."""
        version_id = node["version"]
        display = get_element_display_name(conn, version_id)
        html = f"<li id='{version_id}'>{display}"

        # Predecessor line (no hyperlink for predecessor ids)
        if "version_not_updated" in node and node.get("version_not_updated"):
            html += "<div style='margin-left:8px'>Hint: Version was not updated.</div>"
        elif "predecessor_version" in node:
            if node["predecessor_version"]:
                pred_display = get_element_display_name(conn, node["predecessor_version"])
                html += f"<div style='margin-left:8px'>Updated from predecessor: {pred_display}</div>"
            else:
                html += "<div style='margin-left:8px'>Updated from predecessor: (none)</div>"

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

        # blank line separation
        html_entry += "<div style='margin-top:6px'></div>"

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

        # Active bugs in current element
        html_entry += "<div style='margin-left:8px'>"
        if direct_bugs:
            html_entry += "<div>Active bugs in current element:<ul>"
            for bug in sorted(direct_bugs.values(), key=lambda b: b.get("id", "")):
                html_entry += f"<li><a href='#bug-{bug['id']}'>{bug['id']}</a> | {bug['title']}</li>"
            html_entry += "</ul></div>"
        else:
            html_entry += "<div>Active bugs in current element: (none)</div>"

        # Active bugs in child elements
        if inherited_bugs:
            html_entry += "<div>Active bugs in child elements:<ul>"
            for bug in sorted(inherited_bugs.values(), key=lambda b: b.get("id", "")):
                path = find_bug_path(node, bug["id"])
                if path and len(path) > 1:
                    path_display_parts = []
                    for vid in path[1:]:
                        elem_display = get_element_display_name(conn, vid)
                        elem_display = elem_display.replace(f"(Id: {vid})", f"(Id: <a href='#{vid}'>{vid}</a>)")
                        path_display_parts.append(elem_display)
                    path_display = " -> ".join(path_display_parts)
                    html_entry += f"<li><a href='#bug-{bug['id']}'>{bug['id']}</a> | {path_display} | {bug['title']}</li>"
                else:
                    html_entry += f"<li><a href='#bug-{bug['id']}'>{bug['id']}</a> | {bug['title']}</li>"
            html_entry += "</ul></div>"
        else:
            html_entry += "<div>Active bugs in child elements: (none)</div>"

        # Total aggregated
        html_entry += f"<div>Total aggregated: {total_bugs} active bug(s)</div>"
        html_entry += "</div>"

        # blank line
        html_entry += "<div style='margin-top:6px'></div>"

        # Changes since predecessor (introduced/fixes)
        html_entry += "<div style='margin-left:8px'>"
        html_entry += "<div>Changes since predecessor:<div style='margin-left:8px'>"
        ch = node.get("since_predecessor") or {}
        # Introduced
        intro = ch.get("introduced")
        if intro:
            html_entry += "<div>Bugs introduced:<ul>"
            for b in intro:
                html_entry += f"<li><a href='#bug-{b['id']}'>{b['id']}</a> | {b.get('title','')}</li>"
            html_entry += "</ul></div>"
        else:
            html_entry += "<div>Bugs introduced: (none)</div>"

        # Fixes
        fixes = ch.get("fixes")
        if fixes:
            html_entry += "<div>Bugs fixed:<ul>"
            for f in fixes:
                neutral = f.get('neutralises')
                neutral_link = f"<a href='#bug-{neutral}'>{neutral}</a>" if neutral else "(none)"
                html_entry += f"<li><a href='#fix-{f['id']}'>{f['id']}</a> (neutralises {neutral_link}): {f.get('title','')}</li>"
            html_entry += "</ul></div>"
        else:
            html_entry += "<div>Bugs fixed: (none)</div>"

        html_entry += "</div></div>"

        html_entry += "</li>"  # Close the li tag
        detailed_html += html_entry
    detailed_html += "</ul>"
    
    # Collect fixes mentioned in since_predecessor sections
    all_fixes = {}
    bug_to_fixes = defaultdict(list)
    for node in all_versions.values():
        sp = node.get("since_predecessor")
        if sp and sp.get("fixes"):
            for f in sp.get("fixes", []):
                all_fixes[f["id"]] = f
                if f.get("neutralises"):
                    bug_to_fixes[f.get("neutralises")].append(f["id"])
                    # ensure neutralised bug appears in all_bugs (even if not active)
                    if f.get("neutralises") not in all_bugs:
                        row = conn.execute("SELECT id, title, description FROM tickets WHERE id = ?", (f.get("neutralises"),)).fetchone()
                        if row:
                            all_bugs[row["id"]] = {"title": row["title"], "description": row["description"], "versions": []}

    # Render bug details section (include bugs that were neutralised by fixes)
    bug_details_html = "<ul>"
    for bug_id in sorted(all_bugs.keys()):
        bug_info = all_bugs[bug_id]
        bug_details_html += f"<li id='bug-{bug_id}'>{bug_id}<div>Title: {bug_info.get('title','')}</div><div>Description: {bug_info.get('description','')}</div>"
        if bug_to_fixes.get(bug_id):
            fixes_links = ", ".join(f"<a href='#fix-{fid}'>{fid}</a>" for fid in sorted(bug_to_fixes[bug_id]))
            bug_details_html += f"<div>Fixed by: {fixes_links}</div>"
        bug_details_html += "</li>"
    bug_details_html += "</ul>"

    # Render fix details section
    fix_details_html = "<ul>"
    for fix_id in sorted(all_fixes.keys()):
        fix = all_fixes[fix_id]
        neutralises = fix.get('neutralises')
        neutralises_link = f"<a href='#bug-{neutralises}'>{neutralises}</a>" if neutralises else "(none)"
        fix_details_html += f"<li id='fix-{fix_id}'>{fix_id}<div>Title: {fix.get('title','')}</div><div>Description: {fix.get('description','')}</div><div>Neutralises: {neutralises_link}</div></li>"
    fix_details_html += "</ul>"

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

    <h2>5. Fix Details</h2>
    {fix_details_html}
    
    </body>
    </html>
    """
    Path(filename).write_text(html)
    conn.close()

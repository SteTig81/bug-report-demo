import sqlite3, json
c = sqlite3.connect('graph_system.db')
rows = [list(r) for r in c.execute("PRAGMA table_info(tickets)")]
print(json.dumps(rows, indent=2))
c.close()

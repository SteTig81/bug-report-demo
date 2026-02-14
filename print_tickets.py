from db import get_connection

conn = get_connection()
for r in conn.execute("SELECT id,title,description FROM tickets ORDER BY id"):
    print(f"{r['id']}|{r['title']}|{r['description']}")
conn.close()

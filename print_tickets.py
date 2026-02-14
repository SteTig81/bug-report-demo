from db import get_connection

conn = get_connection()
for r in conn.execute("SELECT id,title,description,fixes_ticket_id FROM tickets ORDER BY id"):
    print(f"{r['id']}|{r['title']}|{r['description']}|{r['fixes_ticket_id']}")
conn.close()

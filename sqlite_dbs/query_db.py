import sqlite3

conn = sqlite3.connect("./sqlite_dbs/pub_sentences.db")
c = conn.cursor()
query = "select * from sentences where pub_filename = 'AR 11-2.pdf'"
c.execute(query)
results = c.fetchall()
conn.close()

print(results)
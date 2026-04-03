import sqlite3

db = r'c:\Coding\vanna-v1\chinook-nl-sql\data\olist.sqlite'
conn = sqlite3.connect(db)
cur = conn.cursor()

tables = ['customers','geolocation','leads_closed','leads_qualified',
          'order_items','order_payments','order_reviews','orders',
          'product_category_name_translation','products','sellers']

for t in tables:
    lines = []
    lines.append(f"### TABLE: {t}")
    
    cur.execute(f"PRAGMA table_info('{t}')")
    cols_info = cur.fetchall()
    lines.append("COLUMNS:")
    for c in cols_info:
        pk = "[PK] " if c[5] else "     "
        lines.append(f"  {pk}{c[1]} ({c[2]})")
    
    cur.execute(f'SELECT COUNT(*) FROM "{t}"')
    count = cur.fetchone()[0]
    lines.append(f"ROWS: {count:,}")
    
    cur.execute(f'SELECT * FROM "{t}" LIMIT 10')
    rows = cur.fetchall()
    col_names = [d[0] for d in cur.description]
    lines.append(f"COLUMNS: {col_names}")
    for i, r in enumerate(rows, 1):
        # Truncate long values
        short = tuple(str(v)[:40] if v is not None else 'NULL' for v in r)
        lines.append(f"  row{i}: {short}")
    lines.append("")

    filename = f"tbl_{t}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

conn.close()
print("All done")

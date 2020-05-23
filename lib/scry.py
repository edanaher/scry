import psycopg2

db = psycopg2.connect("")

cur = db.cursor()

cur.execute("SELECT * FROM poc_users")

for row in cur:
    print(row)


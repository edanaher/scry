from collections import defaultdict
import psycopg2
import sys

def get_foreign_keys(cur):
    query = """SELECT
        tc.table_schema,
        tc.table_name,
        kcu.column_name,
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'"""

    keys = defaultdict(dict)
    cur.execute(query)
    for row in cur:
        s1, t1, c1, s2, t2, c2 = row
        st1 = f"{s1}.{t1}"
        st2 = f"{s2}.{t2}"
        keys[st1][st2] = (c1, c2)
        keys[st2][st1] = (c2, c1)
    return keys


def main():

    db = psycopg2.connect("")
    cur = db.cursor()

    foreign_keys = get_foreign_keys(cur)
    query = sys.argv[1]

    [schema, *tables] = query.split(".")

    sql = f"SELECT * FROM {schema}.{tables[0]}"
    for i in range(1, len(tables)):
        t1 = schema + "." + tables[i-1]
        t2 = schema + "." + tables[i]
        k1, k2 = foreign_keys[t1][t2]
        sql += f" JOIN {t2} ON {t1}.{k1} = {t2}.{k2}"

    print(sql)
    cur.execute(sql)

    for row in cur:
        print(row)

main()

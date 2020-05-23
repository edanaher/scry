#!/usr/bin/env python

from collections import defaultdict
import psycopg2
from lark import Lark
import sys

DEFAULT_SCHEMA="scry"

def get_table_info(cur):
    schemas = set()
    tables = set()
    columns = set()
    query = """SELECT
        table_schema,
        table_name,
        column_name
    FROM information_schema.columns
    WHERE table_schema='scry'"""

    cur.execute(query)

    for row in cur:
        s, t, c = row
        schemas.add(s)
        tables.add(t)
        columns.add(c)
    return (list(schemas), list(tables), list(columns))


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

def addToTree(tree, path, hasSchema = False):
    if len(path) == 0:
        tree["*"] = None
        return

    node = path[0]

    # If there's no schema, use the default.
    if not hasSchema and node.type != "SCHEMA":
        if DEFAULT_SCHEMA not in tree:
            tree[DEFAULT_SCHEMA] = {}
        tree = tree[DEFAULT_SCHEMA]

    if node.type == "SCHEMA" or node.type == "TABLE":
        if node.value not in tree:
            tree[node.value] = {}
        subTree = tree[node.value]
        addToTree(subTree, path[1:], True)

    if node.type == "FIELD":
        tree[node.value] = None


def parse(table_info, query):
    def choices(cs):
        return "|".join(sorted([f'"{c}"' for c in cs],key=len,reverse=True))

    schemas, tables, columns = table_info
    p = Lark(f"""
        start: component (" " component)*
        component: query_path
        query_path: (SCHEMA ".")? TABLE ("." TABLE)* ("." FIELD)?
        SCHEMA: {choices(schemas)}
        TABLE: {choices(tables)}
        FIELD: {choices(columns)} | "*"

        %import common.CNAME -> NAME
    """)
    parsed = p.parse(query)

    tree = {}
    for component in parsed.children:
        for subcomponent in component.children:
            if(subcomponent.data == "query_path"):
                addToTree(tree, subcomponent.children)
    return tree

def generate_sql(foreign_keys, tree, schema=None, lastTable=None, path=None):
    selects = []
    joins = []
    if not schema:
        for s, subTree in tree.items():
            s, j = generate_sql(foreign_keys, subTree, s, None, s)
            selects += s
            joins += j
        return (selects, joins)

    if not lastTable:
        for t, subTree in tree.items():
            joins.append(schema + "." + t)
            s, j = generate_sql(foreign_keys, subTree, schema, t, schema + "." + t)
            selects += s
            joins += j
        return (selects, joins)

    for c, subTree in tree.items():
        if c == '*':
            selects.append(schema + "." + lastTable + ".*")
        else:
            t1 = schema + "." + lastTable
            t2 = schema + "." + c
            k1, k2 = foreign_keys[t1][t2]
            joins.append(f" JOIN {t2} ON {t1}.{k1} = {t2}.{k2}")
            s, j = generate_sql(foreign_keys, subTree, schema, c, path + "." + c)
            selects += s
            joins += j

    return (selects, joins)

def serialize_sql(clauses):
    selects, joins = clauses
    selects_string = ", ".join(selects)
    joins_string = " ".join(joins)
    return f"SELECT {selects_string} FROM {joins_string}"

def main():
    db = psycopg2.connect("")
    cur = db.cursor()

    table_info = get_table_info(cur)
    foreign_keys = get_foreign_keys(cur)
    query = sys.argv[1]
    tree = parse(table_info, query)
    print(tree)

    sql_clauses = generate_sql(foreign_keys, tree)
    sql = serialize_sql(sql_clauses)

    print(sql)
    cur.execute(sql)

    for row in cur:
        print(row)

main()

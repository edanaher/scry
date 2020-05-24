#!/usr/bin/env python

from collections import defaultdict
import psycopg2
from lark import Lark
import lark
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


class buildTree(lark.Visitor):
    def __init__(self):
        self.trees = {}

    def query_path(self, tree):
        if tree.children[0].data == "schema":
            schema = tree.children[0].children[0].value
            children = tree.children[1:]
        else:
            schema = DEFAULT_SCHEMA
            children = tree.children

        if children[-1].data == "columns":
            columns = [c.value for c in children[-1].children]
            children = children[:-1]
        else:
            columns = ["*"]

        tables = [c.children[0].value for c in children]

        def updateTree(tree, tables, columns):
            if tables == []:
                if "columns" not in tree:
                    tree["columns"] = []
                tree["columns"] += columns
                return

            table, *rtables = tables
            if "children" not in tree:
                tree["children"] = {}
            if table not in tree["children"]:
                tree["children"][table] = {}

            updateTree(tree["children"][table], rtables, columns)

        if schema not in self.trees:
            self.trees[schema] = {}
        updateTree(self.trees[schema], tables, columns)


def parse(table_info, query):
    def choices(cs):
        return "|".join(sorted([f'"{c}"' for c in cs],key=len,reverse=True))

    schemas, tables, columns = table_info
    p = Lark(f"""
        start: component (" " component)*
        component: query_path
        query_path: (schema ".")? table ("." table)* ("." columns)?
        schema: SCHEMA
        table: TABLE
        SCHEMA: {choices(schemas)}
        TABLE: {choices(tables)}
        columns: COLUMN ("," COLUMN)*
        COLUMN: {choices(columns)} | "*"

        %import common.CNAME -> NAME
        %import common.WS
        %ignore WS
    """)
    parsed = p.parse(query)
    t = buildTree()
    t.visit(parsed)
    return t.trees

def generate_sql(foreign_keys, tree, schema=None, table=None, lastTable=None):
    selects = []
    joins = []
    if not schema:
        for s, subTree in tree.items():
            s, j = generate_sql(foreign_keys, subTree, s, None, None)
            selects += s
            joins += j
        return (selects, joins)

    if not table:
        for t, subTree in tree["children"].items():
            s, j = generate_sql(foreign_keys, subTree, schema, t, None)
            selects += s
            joins += j
        return (selects, joins)

    for c in tree.get("columns", []):
        selects.append(schema + "." + table + "." + c)

    if not lastTable:
        joins.append(schema + "." + table)
        for t, subTree in tree.get("children", {}).items():
            s, j = generate_sql(foreign_keys, subTree, schema, t, table)
            selects += s
            joins += j
        return (selects, joins)

    # a join table
    t1 = schema + "." + lastTable
    t2 = schema + "." + table
    k1, k2 = foreign_keys[t1][t2]
    joins.append(f" JOIN {t2} ON {t1}.{k1} = {t2}.{k2}")

    for c, subTree in tree.get("children", {}).items():
        s, j = generate_sql(foreign_keys, subTree, schema, c, table)
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

    sql_clauses = generate_sql(foreign_keys, tree)
    sql = serialize_sql(sql_clauses)

    print(sql)
    cur.execute(sql)

    for row in cur:
        print(row)

main()

#!/usr/bin/env python

import argparse
from collections import defaultdict
import psycopg2
from lark import Lark
import lark
import sys

DEFAULT_SCHEMA="public"

def get_table_info(cur):
    schemas = set()
    tables = set()
    columns = set()
    query = """SELECT
        table_schema,
        table_name,
        column_name
    FROM information_schema.columns
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    """

    cur.execute(query)

    table_columns = defaultdict(lambda: [])
    for row in cur:
        s, t, c = row
        schemas.add(s)
        tables.add(t)
        columns.add(c)
        table_columns[t].append(c)
    return (list(schemas), list(tables), list(columns), table_columns)


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

# ensure_exists(dict, key1, key2, ..., keyn, default)
# Ensures that dict[key1][key2]...[keyn] exists; sets to default if not, and
# creates intermediate dictionares as necessary.
def ensure_exists(dict, *args):
    if len(args) == 2:
        key, default = args
        if key not in dict:
            dict[key] = default
    else:
        key, *rargs = args
        if key not in dict:
            dict[key] = {}
        ensure_exists(dict[key], *rargs)


class buildTree(lark.Visitor):
    def __init__(self, table_columns):
        self.trees = {}
        self.table_columns = table_columns

    def _split_path(self, tree):
        if tree.children[0].data == "schema":
            schema = tree.children[0].children[0].value
            children = tree.children[1:]
        else:
            schema = DEFAULT_SCHEMA
            children = tree.children

        if children[-1].data == "columns":
            columns = [c.value for c in children[-1].children]
            children = children[:-1]
        elif children[-1].data == "column":
            columns = children[-1].children[0].value
            children = children[:-1]
        else:
            columns = ["*"]

        tables = [c.children[0].value for c in children]

        # Should this just replace the *, and keep duplicated fields?
        if "*" in columns:
            columns = self.table_columns[tables[-1]]


        return (schema, tables, columns)


    def query_path(self, tree):
        schema, tables, columns = self._split_path(tree)

        def updateTree(tree, tables, columns):
            if tables == []:
                ensure_exists(tree, "columns", [])
                tree["columns"] += columns
                return

            table, *rtables = tables
            ensure_exists(tree, "children", table, {})

            updateTree(tree["children"][table], rtables, columns)

        ensure_exists(self.trees, schema, {})
        updateTree(self.trees[schema], tables, columns)

    def condition(self, tree):
        if tree.children[0].data == "condition_path":
            prefix, suffix = tree.children[0].children
            schema, prefix_tables, _ = self._split_path(prefix)
            _, suffix_tables, column = self._split_path(suffix)
        else: # full_path
            prefix, suffix = tree.children[0].children
            schema, prefix_tables, _ = self._split_path(prefix)
            column = suffix.children[0].value

            suffix_tables = []

        op = tree.children[1].children[0].value
        value = tree.children[2].value
        if value[0] == '"' and value[-1] == '"':
            value = f"'{value[1:-1]}'"

        def findPrefix(tree, prefix):
            if prefix == []:
                return tree
            (t, *rprefix) = prefix
            ensure_exists(tree, "children", t, {})
            return findPrefix(tree["children"][t], rprefix)

        def addConstraint(tree, suffix):
            if suffix == []:
                ensure_exists(tree, "conditions", [])
                tree["conditions"].append((column, op, value))
                return
            s, *rsuffix = suffix
            ensure_exists(tree, "children", s, {})
            addConstraint(tree["children"][s], rsuffix)


        ensure_exists(self.trees, schema, {})
        prefixNode = findPrefix(self.trees[schema], prefix_tables)

        if suffix_tables == []:
            ensure_exists(prefixNode, "conditions", "conditions", [])
            prefixNode["conditions"]["conditions"].append((column, op, value))
        else:
            t, *ts = suffix_tables
            ensure_exists(prefixNode, "conditions", "children", t, {})
            addConstraint(prefixNode["conditions"]["children"][t], ts)


def parse(table_info, query):
    def choices(cs):
        return "|".join(sorted([f'"{c}"' for c in cs],key=len,reverse=True))

    schemas, tables, columns, table_columns = table_info
    p = Lark(f"""
        start: component (" " component)*
        component: query_path | condition

        query_path: (schema ".")? table ("." table)* ("." columns)?

        condition: (condition_path | condition_full_path) comparison_op VALUE
        condition_path: condition_path_prefix ":" condition_path_suffix
        condition_full_path: condition_path_prefix "." column
        condition_path_prefix: (schema ".")? table ("." table)*
        condition_path_suffix: (table ".")* column
        !comparison_op: "=" | "<" | "<=" | ">=" | ">" | "LIKE"i | "ILIKE"i

        schema: SCHEMA
        table: TABLE
        column: COLUMN
        SCHEMA: {choices(schemas)}
        TABLE: {choices(tables)}
        columns: COLUMN ("," COLUMN)*
        COLUMN: {choices(columns)} | "*"
        VALUE: ESCAPED_STRING | SIGNED_NUMBER

        %import common.CNAME -> NAME
        %import common.ESCAPED_STRING
        %import common.SIGNED_NUMBER
        %import common.WS
        %ignore WS
    """)
    parsed = p.parse(query)
    t = buildTree(table_columns)
    t.visit(parsed)
    return t.trees


def join_condition(foreign_keys, schema, t1, t2):
    st1 = schema + "." + t1
    st2 = schema + "." + t2
    k1, k2 = foreign_keys[st1][st2]
    return f"JOIN {st2} ON {st1}.{k1} = {st2}.{k2}"

def merge_clauses(dst, src):
    for k, vs in src.items():
        ensure_exists(dst, k, [])
        dst[k] += vs

def generate_sql(foreign_keys, tree, schema=None, table=None, lastTable=None, path=None):
    def generate_condition_subquery(baseTable, tree):
        def subcondition_sql(tree, lastTable):
            clauses = {"joins": [], "wheres": []}
            for t, subTree in tree.get("children", {}).items():
                clauses["joins"].append(join_condition(foreign_keys, schema, lastTable, t))
                subclauses = subcondition_sql(subTree, t)
                merge_clauses(clauses, subclauses)
            for c in tree.get("conditions", []):
                col, op, value = c
                clauses["wheres"].append(f"{schema}.{lastTable}.{col} {op} {value}")
            return clauses

        clauses = subcondition_sql(tree, baseTable)
        joins_string = schema + "." + table + " " + " ".join(clauses["joins"])
        wheres_string = " AND ".join(clauses["wheres"])
        sql = f"{schema}.{table}.id IN (SELECT {schema}.{table}.id FROM {joins_string} WHERE {wheres_string})"
        return sql

    clauses = { "selects": [], "joins": [], "wheres": [] }
    if not schema:
        for s, subTree in tree.items():
            subclauses = generate_sql(foreign_keys, subTree, s, None, None, s)
            merge_clauses(clauses, subclauses)
        return clauses

    if not table:
        for t, subTree in tree["children"].items():
            subclauses = generate_sql(foreign_keys, subTree, schema, t, None, path + "." + t)
            merge_clauses(clauses, subclauses)
        return clauses

    for c in tree.get("columns", []):
        col = schema + "." + table + "." + c
        clauses["selects"].append((f"{col}", f"{path}.{c}"))

    if "conditions" in tree:
        for c in tree["conditions"].get("conditions", []):
            col, op, value = c
            clauses["wheres"].append(f"{schema}.{table}.{col} {op} {value}")
        if "children" in tree["conditions"]:
            clauses["wheres"].append(generate_condition_subquery(table, tree["conditions"]))

    if not lastTable:
        clauses["joins"].append(schema + "." + table)
        for t, subTree in tree.get("children", {}).items():
            subclauses = generate_sql(foreign_keys, subTree, schema, t, table, path + "." + t)
            merge_clauses(clauses, subclauses)
        return clauses

    clauses["joins"].append(join_condition(foreign_keys, schema, lastTable, table))

    for c, subTree in tree.get("children", {}).items():
        subclauses = generate_sql(foreign_keys, subTree, schema, c, table, path + "." + c)
        merge_clauses(clauses, subclauses)

    return clauses

def serialize_sql(clauses):
    selects = clauses["selects"]
    joins = clauses["joins"]
    wheres = clauses["wheres"]
    selects_string = ", ".join([s[0] for s in selects])
    joins_string = " ".join(joins)
    wheres_string = ""
    if wheres != []:
        wheres_string = " WHERE " + " AND ".join(wheres)
    return f"SELECT {selects_string} FROM {joins_string} {wheres_string}"

def parseargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--command", help="command to run")
    parser.add_argument("-d", "--database", help="database to connect to")
    return parser.parse_args()

def shared_prefix(l1, l2):
    minlen = min(len(l1), len(l2))
    for i in range(0, minlen):
        if l1[i] != l2[i]:
            return i
    return minlen

def main():
    args = parseargs()
    db = psycopg2.connect(args.database or "")
    cur = db.cursor()

    table_info = get_table_info(cur)
    foreign_keys = get_foreign_keys(cur)
    query = args.command
    tree = parse(table_info, query)

    sql_clauses = generate_sql(foreign_keys, tree)
    sql = serialize_sql(sql_clauses)

    print(sql)
    cur.execute(sql)
    print(sql_clauses["selects"])

    last_path = []
    for row in cur:
        for (l, r) in zip(sql_clauses["selects"], list(row)):
            path = l[1].split(".")
            shared = shared_prefix(path, last_path)
            print("  "*(shared - 1), end="")
            print("- "*(len(path) - shared - 1), end="")
            last_path = path
            print(f"{path[-1]}: {r}")

main()

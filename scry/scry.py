#!/usr/bin/env python

import argparse
from collections import defaultdict
import psycopg2
from lark import Lark
import lark
import os
import re
import sys
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.shortcuts.prompt import CompleteStyle

class ScryException(Exception):
    pass

completion_styles = {
    "column": CompleteStyle.COLUMN,
    "multi_column": CompleteStyle.MULTI_COLUMN,
    "readline": CompleteStyle.READLINE_LIKE
}

def default_settings():
    return {
        "config": {
            "complete_style": "column",
            "search_path": "scry,public,information_schema"
        },
        "aliases": {},
    }

def get_table_info(cur):
    schemas = set()
    tables = defaultdict(set)
    columns = set()
    query = """SELECT
        table_schema,
        table_name,
        column_name
    FROM information_schema.columns
    """

    cur.execute(query)

    table_columns = defaultdict(lambda: [])
    for row in cur:
        s, t, c = row
        schemas.add(s)
        tables[t].add(s)
        columns.add(c)
        table_columns[t].append(c)
    return (list(schemas), {t: list(ss) for t, ss in tables.items()}, list(columns), table_columns)

def get_unique_keys(cur):
    query =  """SELECT
        tc.table_schema,
        tc.table_name,
        tc.constraint_name,
        tc.constraint_type,
        kcu.column_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
    WHERE
        tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')"""

    keys = {}
    cur.execute(query)
    for row in cur:
        schema, table, name, type, column = row
        ensure_exists(keys, schema, table, {})
        tkeys = keys[schema][table]
        if type == "PRIMARY KEY":
            if tkeys.get("type", "") != "primary":
                keys[schema][table] = { "type": "primary", "columns": [column] }
            else:
                keys[schema][table]["columns"].append(column)
        else: # UNIQUE
            if type in tkeys and tkeys["type"] == "primary":
                continue
            tkeys["type"] = "unique"
            ensure_exists(tkeys, name, [])
            tkeys[name].append(column)

    # Pick the shortest unique key for each table
    for _, schema_keys in keys.items():
        for _, table_keys in schema_keys.items():
            if table_keys["type"] == "primary":
                continue
            shortest = None
            for k, columns in table_keys.items():
                if k == "type":
                    continue
                if shortest is None or len(columns) < len(shortest):
                    shortest = columns
            table_keys["columns"] = shortest

    return keys

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

    keys = {}
    cur.execute(query)
    for row in cur:
        s1, t1, c1, s2, t2, c2 = row
        st1 = f"{s1}.{t1}"
        st2 = f"{s2}.{t2}"
        ensure_exists(keys, t1, s1, t2, {})
        keys[t1][s1][t2][s2] = (c1, c2)
        ensure_exists(keys, t2, s2, t1, {})
        keys[t2][s2][t1][s1] = (c2, c1)
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

class findAliases(lark.Transformer):
    def __init__(self, settings, table_info, foreign_keys):
        schemas, tables, columns, table_columns = table_info
        self.settings = settings
        self.schemas = schemas
        self.tables = tables
        self.table_columns = table_columns
        self.foreign_keys = foreign_keys
        self.seen_aliases = set()
        self.aliases = { None: {}}

    def _schema_for_table(self, table):
        if table in self.aliases:
            return self.aliases[table][0]
        if table not in self.tables:
            return None
        for s in self.settings["config"]["search_path"].split(","):
            if s in self.tables[table]:
                return s
        # If it's not in the search_path, just take the first one.
        return list(self.tables[table].keys())[0]

    def _aliases_needed_for_path(self, path):
        needed = []
        # TODO: will this have issues with singleton columns named the same as aliases?
        for t in path:
            if isinstance(t, list): # columns
                continue
            if len(t) == 2: # declaring an alias
                continue
            table = t[0]
            if table in self.seen_aliases:
                needed.append(table)
        return needed


    def _add_aliases(self, prefix, elems):
        aliases = self.aliases[prefix]
        first_elem = elems[0]
        path = []

        if first_elem[0] in aliases:
            s, p, t = aliases[first_elem[0]]
            schema = s
            path = p.copy()
        # Can't alias schemas yet
        elif len(first_elem) == 1 and first_elem[0] in self.schemas:
            schema = first_elem[0]
            elems = elems[1:]
        else:
            schema = self._schema_for_table(first_elem[0])
            if not schema:
                raise ScryException(f"Unable to resolve schema for {first_elem[0]}")

        for elem in elems:
            # Skip columns
            if isinstance(elem, list):
                continue

            # An alias definition.  Straightforward
            if len(elem) == 2:
                table, alias = elem

            # If it's just an alias...  it's fine.
            # TODO: As long as it's in the right place.
            elif elem[0] in aliases:
                alias = elem[0]
                _, _, table = aliases[alias]

            # If it's not a table, it must be a column
            elif elem[0] not in self.table_columns:
                _, _, t = aliases[path[-1]]
                if elem[0] not in self.table_columns[t] and elem[0] != "*":
                    raise ScryException(f"Unknown table or column: {elem[0]}")
                continue

            # If it's not an alias, it implicity aliases to itself.
            else:
                # This should never happen, I think?
                if elem[0] in self.seen_aliases:
                    raise ScryException("Alias inconsistency.  Uh oh.")
                if elem[0] not in self.table_columns:
                    raise ScryException(f"Unknown table: {table}")
                table = elem[0]
                alias = table

            if path:
                _, _, last_table = aliases[path[-1]]
                if table not in self.foreign_keys[last_table][schema]:
                    raise ScryException(f"No known join of {table} to {path[-1]}")


            # Check that the alias doesn't already exist somewhere else
            if alias in aliases:
                s, p, t = aliases[alias]
                if schema != s:
                    raise ScryException(f"Existing alias {alias} on schema {s} reused on {schema}")
                if table != t:
                    raise ScryException(f"Existing alias {alias} for table {t} reused on {table}")
                if path != p:
                    raise ScryException(f"Existing alias {alias} for table {t} on path '{'('.join(p)}' reused on '{'.'.join(path)}'")
            else:
                aliases[alias] = (schema, path.copy(), table)

            # TODO: Update the schema in case of cross-schema joins.
            # Actually add an alias
            path.append(alias)

    def query(self, children):
        # Filter out whitespace
        children = [c for c in children if isinstance(c, tuple)]

        # Tag queries with what aliases they need before they can be resolved
        children_with_aliases = [(c[0], self._aliases_needed_for_path(c[0])) for c in children]

        updated_one = True
        while children_with_aliases and updated_one:
            updated_one = False
            next_round = []
            for ca in children_with_aliases:
                # If any needed aliases are unresolved, put this one off.
                needed_aliases = ca[1]
                if not all(a in self.aliases[None] for a in needed_aliases):
                    next_round.append(ca)
                    continue

                # Add aliases for thie query, since we have everything we need.
                self._add_aliases(None, ca[0])
                updated_one = True
            children_with_aliases = next_round

        if children_with_aliases:
            raise ScryException("Unfinished aliases:", children_with_aliases)

        deep_conditions = [(c[0][-1][-1], c[1]) for c in children if c[1]]
        # TODO: Handle actual aliases here(?!)
        for t, c in deep_conditions:
            ensure_exists(self.aliases, t, {})
            self._add_aliases(t, c)

    def condition(self, children):
        return (children[0][0], children[0][1])

    def condition_full_path(self, children):
        return (children[0], [], children[1])

    def condition_path(self, children):
        prefix = children[0]
        suffix, column = children[1]
        return (prefix, suffix, column)

    def condition_path_prefix(self, children):
        return children

    def condition_path_suffix(self, children):
        return (children[:-1], children[-1])


    def component(self, children):
        return children[0]

    def query_path(self, children):
        return (children, [])

    def path_elem(self, children):
        # No alias
        table = children[0].value
        if len(children) == 1:
            return (table,)

        alias = children[1].value
        self.seen_aliases.add(alias)
        return (table, alias)

    def columns(self, children):
        # A trailing table is sometimes parsed as a columns.  So treat a singleton columns
        # as if it were a table, and we'll fix it up later.
        if len(children) == 1:
            return (children[0].value,)
        return [c.value for c in children]

    def terminator(self, children):
        return []




class buildTree(lark.Transformer):
    def __init__(self, settings, tables, table_columns, foreign_keys, schemas, aliases):
        self.trees = {}
        self.settings = settings
        self.tables = tables
        self.table_columns = table_columns
        self.foreign_keys = foreign_keys
        self.schemas = schemas
        self.table_to_node = {}
        self.aliases = aliases

    def _table_alias(self, schema, tree):
        table = tree.children[0].value
        # An alias definition; look up what it resolved to and return it.
        if len(tree.children) > 1:
            alias = tree.children[1].value
            schema, path, table = aliases[None][alias]
            return (schema, table, alias)

        # Otherwise just look up the table.
        if table not in self.aliases:
            raise ScryException(f"Table {table} not found in aliases.  Uh oh.")

        alias = tree.children[0].value
        schema, path, table = aliases[None][alias]
        return (schema, table, alias)

    def _split_path(self, tree):
        first_name = tree.children[0].children[0].value
        explicit_schema = False
        if first_name in self.schemas:
            schema = first_name
            explicit_schema = True
            children = tree.children[1:]
        else:
            if first_name not in self.aliases[None]:
                if first_name not in self.tables:
                    raise ScryException(f"Unknown table: {first_name}")
                if len(self.tables[first_name]) > 1:
                    raise ScryException(f"Ambiguous table {first_name} in schemas {', '.join(self.tables[first_name])}")
                schema = self.tables[first_name][0]
            else:
                schema = None
            children = tree.children

        # Handle a terminator
        terminated = False
        if children[-1].data == "terminator":
            terminated = True
            children = children[:-1]

        (schema, first_table, first_alias) = self._table_alias(schema, children[0])
        if first_table not in self.table_columns:
            raise ScryException("Unknown table: " + first_table)
        tables = [(first_table, first_alias)]

        for child in children[1:-1]:
            (schema, table, alias) = self._table_alias(schema, child)
            if table not in self.table_columns:
                raise ScryException("Unknown table: " + table)
            if schema not in self.foreign_keys.get(tables[-1][0], {}).get(schema, {}).get(table, {}):
                raise ScryException(f"No known join: {schema}.{tables[-1][0]} to {table}")
            tables.append((table, alias))

        if children[-1].data == "columns" and len(children[-1].children) > 1:
            # TODO: Filter out unknown columns
            columns = [c.value for c in children[-1].children]
            children = children[:-1]
        else:
            name = children[-1].children[0].value
            if len(children) == 1:
                # It's a standalone table; we already got it as first_table, and assume all fields.
                columns = ["*"]
            elif name in self.table_columns[tables[-1][0]] or name == "*":
                # It's a column...
                columns = [name]
            else:
                # Assume it's a table
                if name not in self.table_columns:
                    raise ScryException(f"Unknown table or column: " + name)
                if schema not in self.foreign_keys.get(tables[-1][0], {}).get(schema, {}).get(name, {}):
                    raise ScryException(f"No known join: {tables[-1][0]} to {name}")
                (schema, table, alias) = self._table_alias(schema, children[-1])
                tables.append((table, alias))
                columns = ["*"]

        # Should this just replace the *, and keep duplicated fields?
        if "*" in columns:
            columns = self.table_columns[tables[-1][0]]

        if terminated:
            columns = []

        return (schema, tables, columns, explicit_schema)

    def _find_prefix(self, tree, prefix):
        if prefix == []:
            return tree
        (alias, *rprefix) = prefix
        ensure_exists(tree, "children", alias, {})
        if "table" not in tree["children"][alias]:
            _, _, table = self.aliases[None][alias]
            tree["children"][alias]["table"] = table
        return self._find_prefix(tree["children"][alias], rprefix)

    def query_path(self, children):
        if children[0] in self.schemas:
            children = children[1:]

        if len(children) == 1:
            alias = children[0]
            columns = ["*"]
        elif isinstance(children[-1], list):
            alias = children[-2]
            columns = children[-1]
        else:
            if children[-1] in self.table_columns[self.aliases[None][children[-2]][2]] or children[-1] == "*":
                alias = children[-2]
                columns = [children[-1]]
            else:
                alias = children[-1]
                columns = ["*"]

        schema, path, table = self.aliases[None][alias]

        ensure_exists(self.trees, schema, {})
        query_root = self.trees[schema]

        target = self._find_prefix(query_root, path + [alias])
        ensure_exists(target, "columns", [])

        if "*" in columns:
            columns = self.table_columns[table]

        target["columns"] += columns

    def condition(self, children):
        prefix, suffix, column = children[0]
        op = children[1]
        value = children[2].value

        if prefix[0] in self.schemas:
            prefix = prefix[1:]

        if value[0] == '"' and value[-1] == '"':
            value = f"'{value[1:-1]}'"

        def addConstraint(tree, suffix):
            if suffix == []:
                ensure_exists(tree, "conditions", [])
                tree["conditions"].append((column, op, value))
                return
            alias, *rsuffix = suffix
            _, _, table = self.aliases[prefix_tail][alias]
            ensure_exists(tree, "children", alias, {})
            tree["children"][alias]["table"] = table
            addConstraint(tree["children"][alias], rsuffix)

        prefix_tail = prefix[-1]
        schema, path, table = self.aliases[None][prefix_tail]

        ensure_exists(self.trees, schema, {})
        query_root = self.trees[schema]
        prefix_node = self._find_prefix(query_root, path + [prefix_tail])


        if suffix == []:
            ensure_exists(prefix_node, "conditions", "conditions", [])
            prefix_node["conditions"]["conditions"].append((column, op, value))
        else:
            a, *ts = suffix
            ensure_exists(prefix_node, "conditions", "children", a, {})
            _, _, t = self.aliases[prefix[-1]][a]
            prefix_node["conditions"]["children"][a]["table"] = t
            addConstraint(prefix_node["conditions"]["children"][a], ts)

    def path_elem(self, children):
        # No alias; use the table name
        table = children[0].value
        if len(children) == 1:
            return children[0].value

        # Otherwise return the alias
        return children[1].value

    def columns(self, children):
        if len(children) == 1:
            return children[0].value
        return [c.value for c in children]

    def terminator(self, children):
        return []

    def condition_full_path(self, children):
        return (children[0], [], children[1])

    def condition_path(self, children):
        prefix = children[0]
        suffix, column = children[1]
        return (prefix, suffix, column)

    def condition_path_prefix(self, children):
        return children

    def condition_path_suffix(self, children):
        return (children[:-1], children[-1])

    def comparison_op(self, children):
        return children[0].value

    def column(self, children):
        return children[0].value


def parse_set(tree):
    if tree.children[0].data != "set":
        return None
    property = tree.children[0].children[0].value
    value = tree.children[0].children[1].value
    return (property, value)

def parse_alias(tree):
    if tree.children[0].data != "alias":
        return None
    table = tree.children[0].children[0].value
    alias = tree.children[0].children[1].value
    return (table, alias)

def parse(settings, table_info, foreign_keys, query, aliases_only=False):
    schemas, tables, columns, table_columns = table_info
    p = Lark(r"""
        start: query | set | alias

        set: "\\set" NAME SETTING
        alias: "\\alias" NAME "@"? NAME

        query: component (WS+ component)*
        component: query_path | condition

        query_path: path_elem ("." path_elem)* ("." columns |  terminator)?

        condition: (condition_path | condition_full_path) comparison_op VALUE
        condition_path: condition_path_prefix ":" condition_path_suffix
        condition_full_path: condition_path_prefix "." column
        condition_path_prefix: path_elem ("." path_elem)*
        condition_path_suffix: path_elem ("." path_elem)*
        !comparison_op: "=" | "<" | "<=" | "<>" | ">=" | ">" | "LIKE"i | "ILIKE"i

        path_elem: COMPONENT ("@" NAME)?
        columns: COLUMN ("," COLUMN)*
        column: COLUMN
        terminator: "." ","
        COMPONENT: NAME
        COLUMN: NAME | "*"
        VALUE: ESCAPED_STRING | SIGNED_NUMBER | "NULL"
        SETTING: NAME | SIGNED_NUMBER

        %import common.CNAME -> NAME
        %import common.ESCAPED_STRING
        %import common.SIGNED_NUMBER
        %import common.WS
        %ignore WS
    """)
    parsed = p.parse(query)
    parsed_set = parse_set(parsed)
    if parsed_set:
        return (None, None, parsed_set, None)
    parsed_alias = parse_alias(parsed)
    if parsed_alias:
        return (None, None, None, parsed_alias)

    at = findAliases(settings, table_info, foreign_keys)
    at.transform(parsed)
    local_aliases = at.aliases
    aliases = settings["aliases"].copy()
    aliases.update(local_aliases)

    if aliases_only:
        return aliases

    t = buildTree(settings, tables, table_columns, foreign_keys, schemas, aliases)
    t.transform(parsed)
    return (t.trees, aliases, None, None)


def join_condition(foreign_keys, schema, t1, t2, a1, a2):
    st1 = schema + "." + t1
    st2 = schema + "." + t2
    k1, k2 = foreign_keys[t1][schema][t2][schema]
    alias_string = " AS " + a2 if a2 != t2 else ""
    j1 = a1 if a1 != t1 else st1
    j2 = a2 if a2 != t2 else st2
    return f"LEFT JOIN {st2}{alias_string} ON {j1}.{k1} = {j2}.{k2}"

def merge_clauses(dst, src):
    for k, vs in src.items():
        ensure_exists(dst, k, [])
        dst[k] += vs

def generate_sql(keys, tree, schema=None, table=None, alias=None, lastAlias=None, lastTable=None, path=None):
    def generate_condition(column, op, value):
        # Oh, SQL and NULL.
        if op == "=" and value.lower() == "null":
            op = "IS"
        if op == "<>" and value.lower() == "null":
            op = "IS NOT"
        return f"{column} {op} {value}"

    def generate_condition_subquery(baseAlias, baseTable, tree):
        def subcondition_sql(tree, lastTable, lastAlias):
            clauses = {"joins": [], "wheres": []}
            for a, subTree in tree.get("children", {}).items():
                t = subTree["table"]
                clauses["joins"].append(join_condition(keys["foreign"], schema, lastTable, t, lastAlias, a))
                subclauses = subcondition_sql(subTree, t, a)
                merge_clauses(clauses, subclauses)
            for c in tree.get("conditions", []):
                col, op, value = c
                query_name = lastAlias if lastAlias != lastTable else schema + "." + lastTable

                clauses["wheres"].append(generate_condition(f"{query_name}.{col}", op, value))
            return clauses

        clauses = subcondition_sql(tree, baseTable, baseAlias)
        joins_string = schema + "." + table + " " + " ".join(clauses["joins"])
        wheres_string = " AND ".join(clauses["wheres"])
        query_name = baseAlias if baseAlias != lastTable else schema + "." + baseTable
        # TODO: Handle non-id keys
        sql = f"{query_name}.id IN (SELECT {schema}.{table}.id FROM {joins_string} WHERE {wheres_string})"
        return sql

    clauses = { "selects": [], "joins": [], "wheres": [], "uniques": [] }
    if not schema:
        for s, subTree in tree.items():
            subclauses = generate_sql(keys, subTree, s, None, None, None, None, s)
            merge_clauses(clauses, subclauses)
        return clauses

    if not table:
        for a, subTree in tree.get("children", {}).items():
            t = subTree["table"]
            subclauses = generate_sql(keys, subTree, schema, t, a, None, None, path + "." + a)
            merge_clauses(clauses, subclauses)
        return clauses

    for c in tree.get("columns", []):
        query_name = alias if alias != table else schema + "." + table
        col = query_name + "." + c
        clauses["selects"].append((f"{col}", f"{path}.{c}"))

    if "conditions" in tree:
        for c in tree["conditions"].get("conditions", []):
            col, op, value = c
            query_name = alias if alias != table else schema + "." + table
            clauses["wheres"].append(generate_condition(f"{query_name}.{col}", op, value))
        if "children" in tree["conditions"]:
            clauses["wheres"].append(generate_condition_subquery(alias, table, tree["conditions"]))

    if not lastTable:
        if table in keys["unique"].get(schema, {}):
            query_name = alias if alias != table else schema + "." + table
            cols = keys["unique"][schema][table]["columns"]
            clauses["uniques"] += [(query_name + "." + c, path + "." + c) for c in cols]
        alias_string = " AS " + alias if alias != table else ""
        clauses["joins"].append(schema + "." + table + alias_string)
        for a, subTree in tree.get("children", {}).items():
            t = subTree["table"]
            subclauses = generate_sql(keys, subTree, schema, t, a, alias, table, path + "." + t)
            merge_clauses(clauses, subclauses)
        return clauses

    if table in keys["unique"][schema]:
        query_name = alias if alias != table else schema + "." + table
        cols = keys["unique"][schema][table]["columns"]
        clauses["uniques"] += [(query_name + "." + c, path + "." + c) for c in cols]
    clauses["joins"].append(join_condition(keys["foreign"], schema, lastTable, table, lastAlias, alias))

    for a, subTree in tree.get("children", {}).items():
        t = subTree["table"]
        subclauses = generate_sql(keys, subTree, schema, t, a, alias, table, path + "." + a)
        merge_clauses(clauses, subclauses)

    return clauses

def serialize_sql(clauses, limit):
    selects = clauses["uniques"] + clauses["selects"]
    joins = clauses["joins"]
    wheres = clauses["wheres"]
    selects_string = ", ".join([s[0] for s in selects])
    joins_string = " ".join(joins)
    wheres_string = ""
    if wheres != []:
        wheres_string = " WHERE " + " AND ".join(wheres)
    limit_string = ""
    if limit != 0:
        limit_string = f"LIMIT {limit}"
    return f"SELECT {selects_string} FROM {joins_string} {wheres_string} {limit_string}"

def parseargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--command", help="command to run")
    parser.add_argument("-d", "--database", help="database to connect to")
    parser.add_argument("-l", "--limit", help="row limit (0 for no limit)", default=100, type=int)
    parser.add_argument("-s", "--schema", help="default schema", default="scry")
    return parser.parse_args()

def shared_prefix(l1, l2):
    minlen = min(len(l1), len(l2))
    for i in range(0, minlen):
        if l1[i] != l2[i]:
            return i
    return minlen

def print_tree(tree, indent=""):
    for k, v in tree.items():
        if isinstance(v, dict):
            print(f"{indent}- {repr(k)}:")
            print_tree(v, indent + "  ")
        else:
            print(f"{indent}  {repr(k)}: {repr(v)}")


def reshape_results(cur, sql_clauses):
    def tree_of_row(tree, path, display, value):
        if len(path) == 1:
            if display:
                ensure_exists(tree, "display", [])
                tree["display"].append((path[0], value))
            else:
                ensure_exists(tree, "hidden", [])
                tree["hidden"].append((path[0], value))
            return
        p, *rpath = path
        ensure_exists(tree, "children", p, {})
        tree_of_row(tree["children"][p], rpath, display, value)

    def add_to_main_tree(tree, tree_for_row):
        for table, subtree in tree_for_row.items():
            display = tuple(subtree.get("display", (None,)))
            if display != (None,) and all(v is None for k, v in display):
                continue
            hidden = tuple(subtree.get("hidden", (None,)))
            key = (display, hidden)
            ensure_exists(tree, table, key, {})
            if "children" in subtree:
                add_to_main_tree(tree[table][key], subtree["children"])

    tree = {}

    selects = [(c[1], True) for c in sql_clauses["selects"]]
    uniques = [(c[1], False) for c in sql_clauses["uniques"]]
    fields = uniques + selects

    for row in cur:
        tree_for_row = {}
        for (p, d), v in zip(fields, row):
            tree_of_row(tree_for_row, p.split("."), d, v)
        add_to_main_tree(tree, tree_for_row["children"])

    return tree

def format_results(results, path="", indent=""):
    output = []

    def print_fields(table, fields):
        output = []
        k, v = fields[0]
        output.append(f"{indent}- {path}{table}.{k}: {v}")
        for k, v in fields[1:]:
            output.append(f"{indent}  {path}{table}.{k}: {v}")
        return output

    for t, subTree in results.items():
        for (display, hidden), nextTree in subTree.items():
            if display != (None,):
                output += print_fields(t, display)
                output += format_results(nextTree, "", indent + "  ")
            else:
                output += format_results(nextTree, path + t + ".", indent)

    return output

def run_setting(settings, key, value):
    print("Setting", key, "=", value)
    settings["config"][key] = value


def run_command(settings, cur, table_info, keys, query, limit=100):
    tree, aliases, setting, alias = parse(settings, table_info, keys["foreign"], query)
    if setting:
        run_setting(settings, *setting)
        return

    if alias:
        (table, alias) = alias
        if table not in table_info[3]:
            print("Unknown table to alias: ", table)
            return
        print(f"Alias '{alias}' created for {table}")
        settings["aliases"][alias] = table
        return

    sql_clauses = generate_sql(keys, tree)

    uniques = sql_clauses["uniques"]
    sql = serialize_sql(sql_clauses, limit)

    print(sql)
    cur.execute(sql)

    results = reshape_results(cur, sql_clauses)

    return format_results(results)

class ScryCompleter(Completer):
    def __init__(self, settings, table_info, foreign_keys):
        schemas, tables, columns, table_columns = table_info
        self.table_info = table_info
        self.schemas = schemas
        self.tables = tables
        self.columns = columns
        self.table_columns = table_columns
        self.foreign_keys = foreign_keys
        self.settings = settings

    def get_completions(self, doc, event):
        full_line = "\n".join(doc.lines)
        word = doc.get_word_before_cursor()
        fullword = doc.get_word_before_cursor("\\S*")

        # TODO: Make this less hacky
        if full_line[0] == "\\":
            words = re.split("\\s+", full_line)
            candidates = []
            if len(words) == 1:
                word = words[0]
                candidates = ["\\set", "\\alias"]
            if words[0] == "\\set":
                if len(words) == 2:
                    candidates = ["complete_style"]
                if len(words) == 3 and words[1] == "complete_style":
                    candidates = completion_styles.keys()
            if words[0] == "\\alias":
                if len(words) == 2:
                    candidates = self.tables.keys()
            matches = [c for c in candidates if c.startswith(word)]
            return [Completion(c, -len(word)) for c in matches]

        aliases = {}
        # There really should be a way to tell Lark to parse as far as it can,
        # but just taking the longest parsable prefix should be good enough.
        # TODO: This should do exponential/binary search after the first couple.
        for l in range(len(full_line), 0, -1):
            try:
                aliases = parse(self.settings, self.table_info, self.foreign_keys, full_line[:l], aliases_only=True)
                break
            except ScryException:
                pass
            except lark.exceptions.LarkError:
                pass

        if word == ".":
            word = ""

        table_candidates = list(self.tables.keys())

        component = doc.get_word_before_cursor("\\S*")
        column_candidates = []
        parts = component.split(".")
        if len(parts) > 1:
            prev_part = parts[-2]
            if prev_part in aliases:
                prev_part = aliases[prev_part][2]
            column_candidates = self.table_columns.get(prev_part, [])
            table_dicts = self.foreign_keys.get(prev_part, {}).values()
            table_candidates = [t for joins in table_dicts for t in joins.keys()]
        else:
            table_candidates += aliases.keys()


        candidates = sorted(column_candidates) + sorted(table_candidates)
        matches = [c for c in candidates if c.startswith(word)]
        return [Completion(c, -len(word)) for c in matches]

def repl(settings, cur, table_info, keys):
    session = PromptSession(
            history=FileHistory(os.getenv("HOME") + "/.scry/history"),
            completer=ScryCompleter(settings, table_info, keys["foreign"]),
            complete_in_thread=True)
    try:
        while True:
            complete_style = completion_styles[settings["config"].get("complete_style", CompleteStyle.COLUMN)]
            command = session.prompt("> ", complete_style=complete_style)
            if command in ["quit", "break", "bye"]:
                break
            try:
                output = run_command(settings, cur, table_info, keys, command)
                if output is not None:
                    print("\n".join(output))
            except ScryException as e:
                print(e)
            except lark.exceptions.LarkError as e:
                print(e)
    except EOFError:
        pass

def read_rcfile(settings, cur, table_info, keys, limit):
    try:
        with open(os.getenv("HOME") + "/.scry/scryrc") as rcfile:
            for line in rcfile.readlines():
                run_command(settings, cur, table_info, keys, line, limit)
    except FileNotFoundError:
        pass

def main():
    args = parseargs()
    db = psycopg2.connect(args.database or "")
    db.autocommit = True
    cur = db.cursor()

    settings = default_settings()
    table_info = get_table_info(cur)
    foreign_keys = get_foreign_keys(cur)
    unique_keys = get_unique_keys(cur)
    keys = { "unique": unique_keys, "foreign": foreign_keys }

    read_rcfile(settings, cur, table_info, keys, args.limit)

    if args.command:
        try:
            output = run_command(settings, cur, table_info, keys, args.command, args.limit)
            if output is not None:
                print("\n".join(output))
        except ScryException as e:
            print(e)
        except lark.exceptions.LarkError as e:
            if isinstance(e.__context__, ScryException):
                print(e.__context__)
            else:
                print(e)
    else:
        repl(settings, cur, table_info, keys)


if __name__ == "__main__":
    main()

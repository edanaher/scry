#!/usr/bin/env python

import argparse
from collections import defaultdict
import psycopg2
from lark import Lark
import lark
import os
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
        "complete_style": "column"
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

class findAliases(lark.Visitor):
    def __init__(self):
        self.aliases = {}

    def path_elem(self, tree):
        # No alias
        if len(tree.children) == 1:
            return

        table = tree.children[0].value
        alias = tree.children[1].value
        self.aliases[alias] = table


class buildTree(lark.Visitor):
    def __init__(self, tables, table_columns, foreign_keys, schemas):
        self.trees = {}
        self.tables = tables
        self.table_columns = table_columns
        self.foreign_keys = foreign_keys
        self.schemas = schemas
        self.table_to_node = {}
        self.aliases = {}

    def _table_alias(self, schema, tree):
        table = tree.children[0].value
        if len(tree.children) > 1:
            # An alias; save it.
            alias = tree.children[1].value
        else:
            if table in self.aliases:
                # If it's just an alias, pull it out.
                alias = table
                schema, table = self.aliases[alias]
            else:
                # If it's just a table, it aliases to itself.
                alias = table
        if alias in self.aliases and self.aliases[alias] != (schema, table):
            raise ScryException(f"Alias conflict: {alias} used for both {self.aliases[alias]} and {table}")
        if schema is None:
            raise ScryException(f"Unable to figure out schema for {table}@{alias}")
        self.aliases[alias] = (schema, table)
        return (schema, table, alias)

    def _split_path(self, tree):
        first_name = tree.children[0].children[0].value
        explicit_schema = False
        if first_name in self.schemas:
            schema = first_name
            explicit_schema = True
            children = tree.children[1:]
        else:
            if first_name not in self.aliases:
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
                    raise ScryException("Unknown table or column: " + name)
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

    def _handle_table_node_mapping(self, tree, table):
        if table in self.table_to_node:
            existing = self.table_to_node[table]
            if tree != existing:
                # If the tree isn't a root, we have a problem.
                if existing != self.trees[None]:
                    raise ScryException(f"Table {table} duplicated in tree!")

                # If the existing tree is a root, splice it in here.
                tree["children"][table] = self.trees[None]["children"][table]
                self.trees[None]["children"].pop(table)
                if self.trees[None]["children"] == {}:
                    self.trees.pop(None)
        else:
            self.table_to_node[table] = tree

    def _find_prefix(self, tree, prefix):
        if prefix == []:
            return tree
        ((table, alias), *rprefix) = prefix
        ensure_exists(tree, "children", {})
        self._handle_table_node_mapping(tree, alias)
        ensure_exists(tree, "children", alias, {})
        tree["children"][alias]["table"] = table
        return self._find_prefix(tree["children"][alias], rprefix)

    def query_path(self, tree):
        schema, tables, columns, explicit_schema = self._split_path(tree)

        # If the table's already in the tree, merge it in there instead of
        # starting a new tree.
        if not explicit_schema and tables[0][1] in self.table_to_node:
            query_root = self.table_to_node[tables[0][1]]
        else:
            ensure_exists(self.trees, schema, {})
            query_root = self.trees[schema]

        target = self._find_prefix(query_root, tables)
        ensure_exists(target, "columns", [])
        target["columns"] += columns

    def condition(self, tree):
        if tree.children[0].data == "condition_path":
            prefix, suffix = tree.children[0].children
            schema, prefix_tables, _, _ = self._split_path(prefix)
            _, suffix_tables, [column], _ = self._split_path(suffix)
        else: # full_path
            prefix, suffix = tree.children[0].children
            schema, prefix_tables, _, _ = self._split_path(prefix)
            column = suffix.children[0].value

            suffix_tables = []

        op = tree.children[1].children[0].value
        value = tree.children[2].value
        if value[0] == '"' and value[-1] == '"':
            value = f"'{value[1:-1]}'"

        def addConstraint(tree, suffix):
            if suffix == []:
                ensure_exists(tree, "conditions", [])
                tree["conditions"].append((column, op, value))
                return
            (table, alias), *rsuffix = suffix
            ensure_exists(tree, "children", alias, {})
            tree["children"][alias]["table"] = table
            addConstraint(tree["children"][alias], rsuffix)


        # If the table's already in the tree, merge it in there instead of
        # starting a new tree.
        first_name = prefix.children[0].children[0].value
        if not schema and prefix.children and first_name in self.table_to_node:
            query_root = self.table_to_node[first_name]
        else:
            ensure_exists(self.trees, schema, {})
            query_root = self.trees[schema]

        prefixNode = self._find_prefix(query_root, prefix_tables)

        if suffix_tables == []:
            ensure_exists(prefixNode, "conditions", "conditions", [])
            prefixNode["conditions"]["conditions"].append((column, op, value))
        else:
            (t, a), *ts = suffix_tables
            ensure_exists(prefixNode, "conditions", "children", a, {})
            prefixNode["conditions"]["children"][a]["table"] = t
            addConstraint(prefixNode["conditions"]["children"][a], ts)

def parse_set(tree):
    if tree.children[0].data != "set":
        return None
    property = tree.children[0].children[0].value
    value = tree.children[0].children[1].value
    return (property, value)

def parse(table_info, foreign_keys, query):
    def choices(cs):
        return "|".join(sorted([f'"{c}"' for c in cs],key=len,reverse=True))

    schemas, tables, columns, table_columns = table_info
    p = Lark(r"""
        start: query | set

        set: "\\set" NAME VALUE

        query: component (WS+ component)*
        component: query_path | condition

        query_path: path_elem ("." path_elem)* ("." columns |  terminator)?

        condition: (condition_path | condition_full_path) comparison_op VALUE
        condition_path: condition_path_prefix ":" condition_path_suffix
        condition_full_path: condition_path_prefix "." column
        condition_path_prefix: path_elem ("." path_elem)*
        condition_path_suffix: path_elem ("." path_elem)*
        !comparison_op: "=" | "<" | "<=" | ">=" | ">" | "LIKE"i | "ILIKE"i

        path_elem: COMPONENT ("@" NAME)?
        columns: COLUMN ("," COLUMN)*
        column: COLUMN
        terminator: "." ","
        COMPONENT: NAME
        COLUMN: NAME | "*"
        VALUE: ESCAPED_STRING | SIGNED_NUMBER

        %import common.CNAME -> NAME
        %import common.ESCAPED_STRING
        %import common.SIGNED_NUMBER
        %import common.WS
        %ignore WS
    """)
    parsed = p.parse(query)
    parsed_set = parse_set(parsed)
    if parsed_set:
        return (None, None, parsed_set)
    at = findAliases()
    at.visit(parsed)
    aliases = at.aliases

    # TODO: We should use aliases in buildTree; that would simplify
    # things and let us use aliases before they're declared.  But
    # schemas complicate that, since we don't have them yet.
    t = buildTree(tables, table_columns, foreign_keys, schemas)
    t.visit(parsed)
    return (t.trees, aliases, None)


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
                clauses["wheres"].append(f"{query_name}.{col} {op} {value}")
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
            clauses["wheres"].append(f"{query_name}.{col} {op} {value}")
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
    if value[0] == '"' and value[-1] == '"':
        settings[key] = value[1:-1]
    else:
        settings[key] = int(value)


def run_command(settings, cur, table_info, keys, query, limit=100):
    tree, aliases, setting = parse(table_info, keys["foreign"], query)
    if setting:
        run_setting(settings, *setting)
        return

    sql_clauses = generate_sql(keys, tree)

    uniques = sql_clauses["uniques"]
    sql = serialize_sql(sql_clauses, limit)

    print(sql)
    cur.execute(sql)

    results = reshape_results(cur, sql_clauses)

    return format_results(results)

class ScryCompleter(Completer):
    def __init__(self, table_info, foreign_keys):
        schemas, tables, columns, table_columns = table_info
        self.table_info = table_info
        self.schemas = schemas
        self.tables = tables
        self.columns = columns
        self.table_columns = table_columns
        self.foreign_keys = foreign_keys

    def get_completions(self, doc, event):
        full_line = "\n".join(doc.lines)

        # TODO: Handle completions for \set
        if full_line[0] == "\\":
            return []

        aliases = {}
        # There really should be a way to tell Lark to parse as far as it can,
        # but just taking the longest parsable prefix should be good enough.
        for l in range(len(full_line), 0, -1):
            try:
                _, aliases, _ = parse(self.table_info, self.foreign_keys, full_line[:l])
                break
            except ScryException:
                pass
            except lark.exceptions.LarkError:
                pass

        word = doc.get_word_before_cursor()
        if word == ".":
            word = ""

        table_candidates = list(self.tables.keys())

        component = doc.get_word_before_cursor("\\S*")
        column_candidates = []
        parts = component.split(".")
        if len(parts) > 1:
            prev_part = parts[-2]
            prev_part = aliases.get(prev_part, prev_part)
            column_candidates = self.table_columns.get(prev_part, [])
            table_dicts = self.foreign_keys.get(prev_part, {}).values()
            table_candidates = [t for joins in table_dicts for t in joins.keys()]

        candidates = sorted(column_candidates) + sorted(table_candidates)
        matches = [c for c in candidates if c.startswith(word)]
        return [Completion(c, -len(word)) for c in matches]

def repl(settings, cur, table_info, keys):
    session = PromptSession(
            history=FileHistory(os.getenv("HOME") + "/.scry/history"),
            completer=ScryCompleter(table_info, keys["foreign"]))
    try:
        while True:
            complete_style = completion_styles[settings.get("complete_style", CompleteStyle.COLUMN)]
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

def main():
    args = parseargs()
    db = psycopg2.connect(args.database or "")
    cur = db.cursor()

    settings = default_settings()
    table_info = get_table_info(cur)
    foreign_keys = get_foreign_keys(cur)
    unique_keys = get_unique_keys(cur)
    keys = { "unique": unique_keys, "foreign": foreign_keys }
    if args.command:
        output = run_command(settings, cur, table_info, keys, args.command, args.limit)
        if output is not None:
            print("\n".join(output))
    else:
        repl(settings, cur, table_info, keys)


if __name__ == "__main__":
    main()

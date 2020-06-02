#!/usr/bin/env python

import argparse
import os
import psycopg2
import sys

# Why is it so hard to get python imports working?
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scry import scry

def parseadd(args):
    db = psycopg2.connect("")
    cur = db.cursor()

    table_info = scry.get_table_info(cur)
    foreign_keys = scry.get_foreign_keys(cur)
    unique_keys = scry.get_unique_keys(cur)
    query = args.command
    tree = scry.parse(table_info, foreign_keys, query)

    keys = { "unique": unique_keys, "foreign": foreign_keys }

    sql_clauses = scry.generate_sql(keys, tree)

    uniques = sql_clauses["uniques"]
    sql = scry.serialize_sql(sql_clauses, 100)

    print(sql)
    cur.execute(sql)

    results = scry.reshape_results(cur, sql_clauses)

    output = scry.format_results(results)
    print("\n".join(output))

    test_file_name = os.path.dirname(__file__) + "/test_scry.py"
    test_file = open(test_file_name, "r")
    tests = test_file.readlines()
    test_file.close()

    add_index = tests.index("    # End of instances\n")
    new_lines = f"""   Instance(
        {repr(args.name)},
        {repr(args.command)},
        {repr(tree)},
        {repr(sql_clauses)},
        {repr(sql)},
        {repr(results)},
        {repr(output)}
        ),
    """
    tests[add_index:add_index] = [new_lines.rstrip() + "\n"]

    test_file = open(test_file_name, "w")
    tests = test_file.writelines(tests)
    test_file.close()



def parseupdate(args):
    print("updating", args)

def parseargs():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="sub-command help")

    parser_add = subparsers.add_parser("add", help="add test instance")
    parser_add.set_defaults(func=parseadd)
    parser_add.add_argument("-n", "--name", help="name of test instance", required=True)
    parser_add.add_argument("-c", "--command", help="command to run for test", required=True)

    parser_update = subparsers.add_parser("update", help="update existing test(s)")
    parser_update.set_defaults(func=parseupdate)

    return parser.parse_args()

def main():
    args = parseargs()
    print(args)
    args.func(args)


if __name__ == "__main__":
    main()

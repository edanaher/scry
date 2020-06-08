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
    tree, aliases, command, alias = scry.parse(scry.default_settings(), table_info, foreign_keys, query)

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
    update_fields = args.fields.split(",")
    def should_be_same(f):
        return f not in update_fields

    db = psycopg2.connect("")
    cur = db.cursor()

    table_info = scry.get_table_info(cur)
    foreign_keys = scry.get_foreign_keys(cur)
    unique_keys = scry.get_unique_keys(cur)

    from test_scry import test_instances, Instance
    new_instances = []
    for instance in test_instances:
        name = instance.name
        query = instance.query
        tree = scry.parse(scry.defaultSettings(), table_info, foreign_keys, query)
        if should_be_same("tree") and tree != instance.tree:
            raise Exception(f"Tree doesn't match for {name}\n\n{tree}\n\n{instance.tree}")

        keys = { "unique": unique_keys, "foreign": foreign_keys }

        sql_clauses = scry.generate_sql(keys, tree)
        if should_be_same("sql_clauses") and sql_clauses != instance.sql_clauses:
            raise Exception(f"Sql_clauses don't match for {name}\n\n{sql_clauses}\n\n{instance.sql_clauses}")

        uniques = sql_clauses["uniques"]
        sql = scry.serialize_sql(sql_clauses, 100)
        if should_be_same("sql") and sql != instance.sql:
            raise Exception(f"Sql doesn't match for {name}\n\n{sql}\n\n{instance.sql}")

        print(sql)
        cur.execute(sql)

        results = scry.reshape_results(cur, sql_clauses)
        if should_be_same("results") and results != instance.results:
            raise Exception(f"Results don't match for {name}\n\n{results}\n\n{instance.results}")

        output = scry.format_results(results)
        if should_be_same("output") and output != instance.output:
            raise Exception(f"Output doesn't match for {name}\n\n{sql}\n\n{instance.sql}")
        print("\n".join(output))
        new_instances.append(Instance(
            name,
            query,
            tree,
            sql_clauses,
            sql,
            results,
            output
        ))


    test_file_name = os.path.dirname(__file__) + "/test_scry.py"
    test_file = open(test_file_name, "r")
    tests = test_file.readlines()
    test_file.close()

    start_index = tests.index("test_instances = [\n") + 1
    finish_index = tests.index("    # End of instances\n")

    new_lines = [f"""    Instance(
        {repr(i.name)},
        {repr(i.query)},
        {repr(i.tree)},
        {repr(i.sql_clauses)},
        {repr(i.sql)},
        {repr(i.results)},
        {repr(i.output)}
        ),
    """ for i in new_instances]
    tests[start_index:finish_index] = [l.rstrip() + "\n" for l in new_lines]
    test_file = open(test_file_name, "w")
    tests = test_file.writelines(tests)
    test_file.close()

def parseargs():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="sub-command help")

    parser_add = subparsers.add_parser("add", help="add test instance")
    parser_add.set_defaults(func=parseadd)
    parser_add.add_argument("-n", "--name", help="name of test instance", required=True)
    parser_add.add_argument("-c", "--command", help="command to run for test", required=True)

    parser_update = subparsers.add_parser("update", help="update existing test(s)")
    parser_update.add_argument("-f", "--fields", help="comma-separated list of fields to update: tree,sql_clauses,sql,results,output", required=True)
    parser_update.set_defaults(func=parseupdate)

    return parser.parse_args()

def main():
    args = parseargs()
    print(args)
    args.func(args)


if __name__ == "__main__":
    main()

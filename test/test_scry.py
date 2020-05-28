import psycopg2
from dataclasses import dataclass

from lib import scry


def test_table_info():
    db = psycopg2.connect("")
    cur = db.cursor()
    schemas, tables, columns, table_columns = scry.get_table_info(cur)
    for s in ["pg_catalog", "scry", "public", "information_schema"]:
        assert s in schemas

    for t in ["authors", "books", "favorites", "users"]:
        assert t in tables

    for c in ["name", "title", "year"]:
        assert c in columns

    for t, cs in { "authors": ["id", "name"], "books": ["id", "title", "year", "author_id"] }.items():
        for c in cs:
            assert c in table_columns[t]

def test_foreign_keys():
    pass

@dataclass
class Instance:
    query: str
    tree: str
    sql_clauses: str
    sql: str
    results : str


test_instances = [
    Instance(
        "scry.authors.name",
        {'scry': {'children': {'authors': {'columns': ['name'], 'table': 'authors'}}}},
        {'selects': [('scry.authors.name', 'scry.authors.name')], 'joins': ['scry.authors'], 'wheres': [], 'uniques': [('scry.authors.id', 'scry.authors.id')]},
        "SELECT scry.authors.id, scry.authors.name FROM scry.authors  LIMIT 100",
        {'scry': {((None,), (None,)): {'authors': {((('name', 'J.R.R Tolkien'),), (('id', 1),)): {}, ((('name', 'J.K. Rowling'),), (('id', 2),)): {}, ((('name', 'Ted Chiang'),), (('id', 3),)): {}}}}})


]

def test_scry():
    db = psycopg2.connect("")
    cur = db.cursor()

    table_info = scry.get_table_info(cur)
    foreign_keys = scry.get_foreign_keys(cur)
    unique_keys = scry.get_unique_keys(cur)
    keys = {"unique": unique_keys, "foreign": foreign_keys}

    for instance in test_instances:
        tree = scry.parse(table_info, foreign_keys, instance.query)
        assert tree == instance.tree

        sql_clauses = scry.generate_sql(keys, tree)
        print(sql_clauses)
        assert sql_clauses == instance.sql_clauses

        sql = scry.serialize_sql(sql_clauses, 100)
        assert(sql == instance.sql)
        cur.execute(sql)
        results = scry.reshape_results(cur, sql_clauses)
        assert results == instance.results

    pass


import psycopg2
import pytest
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
    name: str
    query: str
    tree: str
    sql_clauses: str
    sql: str
    results : str
    output : [str]


test_instances = [
    Instance(
        "simple test of table and column",
        "scry.authors.name",
        {'scry': {'children': {'authors': {'columns': ['name'], 'table': 'authors'}}}},
        {'selects': [('scry.authors.name', 'scry.authors.name')], 'joins': ['scry.authors'], 'wheres': [], 'uniques': [('scry.authors.id', 'scry.authors.id')]},
        "SELECT scry.authors.id, scry.authors.name FROM scry.authors  LIMIT 100",
        {'scry': {((None,), (None,)): {'authors': {((('name', 'J.R.R Tolkien'),), (('id', 1),)): {}, ((('name', 'J.K. Rowling'),), (('id', 2),)): {}, ((('name', 'Ted Chiang'),), (('id', 3),)): {}}}}},
        ['- scry.authors.name: J.R.R Tolkien', '- scry.authors.name: J.K. Rowling', '- scry.authors.name: Ted Chiang']
        ),
   Instance(
        'simple test with two columns',
        'scry.books.title scry.books.year',
        {'scry': {'children': {'books': {'table': 'books', 'columns': ['title', 'year']}}}},
        {'selects': [('scry.books.title', 'scry.books.title'), ('scry.books.year', 'scry.books.year')], 'joins': ['scry.books'], 'wheres': [], 'uniques': [('scry.books.id', 'scry.books.id')]},
        'SELECT scry.books.id, scry.books.title, scry.books.year FROM scry.books  LIMIT 100',
        {'scry': {((None,), (None,)): {'books': {((('title', 'Fellowship of the Rings'), ('year', 1954)), (('id', 1),)): {}, ((('title', 'The Two Towers'), ('year', 1954)), (('id', 2),)): {}, ((('title', 'Return of the King'), ('year', 1955)), (('id', 3),)): {}, ((('title', "Harry Potter and the Philosopher's Stone"), ('year', 1997)), (('id', 4),)): {}, ((('title', 'Harry Potter and the Prisoner of Azkaban'), ('year', 1999)), (('id', 5),)): {}, ((('title', 'Exhalation'), ('year', 2019)), (('id', 6),)): {}, ((('title', 'Beowolf'), ('year', 2016)), (('id', 7),)): {}}}}},
        ['- scry.books.title: Fellowship of the Rings', '  scry.books.year: 1954', '- scry.books.title: The Two Towers', '  scry.books.year: 1954', '- scry.books.title: Return of the King', '  scry.books.year: 1955', "- scry.books.title: Harry Potter and the Philosopher's Stone", '  scry.books.year: 1997', '- scry.books.title: Harry Potter and the Prisoner of Azkaban', '  scry.books.year: 1999', '- scry.books.title: Exhalation', '  scry.books.year: 2019', '- scry.books.title: Beowolf', '  scry.books.year: 2016']
        ),
   Instance(
        'simple test with two comma-separated columns',
        'scry.books.title,year',
        {'scry': {'children': {'books': {'table': 'books', 'columns': ['title', 'year']}}}},
        {'selects': [('scry.books.title', 'scry.books.title'), ('scry.books.year', 'scry.books.year')], 'joins': ['scry.books'], 'wheres': [], 'uniques': [('scry.books.id', 'scry.books.id')]},
        'SELECT scry.books.id, scry.books.title, scry.books.year FROM scry.books  LIMIT 100',
        {'scry': {((None,), (None,)): {'books': {((('title', 'Fellowship of the Rings'), ('year', 1954)), (('id', 1),)): {}, ((('title', 'The Two Towers'), ('year', 1954)), (('id', 2),)): {}, ((('title', 'Return of the King'), ('year', 1955)), (('id', 3),)): {}, ((('title', "Harry Potter and the Philosopher's Stone"), ('year', 1997)), (('id', 4),)): {}, ((('title', 'Harry Potter and the Prisoner of Azkaban'), ('year', 1999)), (('id', 5),)): {}, ((('title', 'Exhalation'), ('year', 2019)), (('id', 6),)): {}, ((('title', 'Beowolf'), ('year', 2016)), (('id', 7),)): {}}}}},
        ['- scry.books.title: Fellowship of the Rings', '  scry.books.year: 1954', '- scry.books.title: The Two Towers', '  scry.books.year: 1954', '- scry.books.title: Return of the King', '  scry.books.year: 1955', "- scry.books.title: Harry Potter and the Philosopher's Stone", '  scry.books.year: 1997', '- scry.books.title: Harry Potter and the Prisoner of Azkaban', '  scry.books.year: 1999', '- scry.books.title: Exhalation', '  scry.books.year: 2019', '- scry.books.title: Beowolf', '  scry.books.year: 2016']
        ),
    # End of instances
]

def run_test(instance):
    db = psycopg2.connect("")
    cur = db.cursor()

    table_info = scry.get_table_info(cur)
    foreign_keys = scry.get_foreign_keys(cur)
    unique_keys = scry.get_unique_keys(cur)
    keys = {"unique": unique_keys, "foreign": foreign_keys}

    tree = scry.parse(table_info, foreign_keys, instance.query)
    assert tree == instance.tree

    sql_clauses = scry.generate_sql(keys, tree)
    assert sql_clauses == instance.sql_clauses

    sql = scry.serialize_sql(sql_clauses, 100)
    assert(sql == instance.sql)
    cur.execute(sql)

    results = scry.reshape_results(cur, sql_clauses)
    assert results == instance.results

    output = scry.format_results(results)
    assert output == instance.output

@pytest.mark.parametrize("instance", test_instances)
def test_scry(instance):
    run_test(instance)


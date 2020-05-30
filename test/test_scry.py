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
        {'scry': {((None,), (None,)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}, ((('name', 'J.K. Rowling'),), (('id', 2),)): {}, ((('name', 'Ted Chiang'),), (('id', 3),)): {}}}}},
        ['- scry.authors.name: J.R.R. Tolkien', '- scry.authors.name: J.K. Rowling', '- scry.authors.name: Ted Chiang']
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
   Instance(
        'Simple test with explicit star columns',
        'scry.books.*',
        {'scry': {'children': {'books': {'table': 'books', 'columns': ['id', 'title', 'year', 'author_id']}}}},
        {'selects': [('scry.books.id', 'scry.books.id'), ('scry.books.title', 'scry.books.title'), ('scry.books.year', 'scry.books.year'), ('scry.books.author_id', 'scry.books.author_id')], 'joins': ['scry.books'], 'wheres': [], 'uniques': [('scry.books.id', 'scry.books.id')]},
        'SELECT scry.books.id, scry.books.id, scry.books.title, scry.books.year, scry.books.author_id FROM scry.books  LIMIT 100',
        {'scry': {((None,), (None,)): {'books': {((('id', 1), ('title', 'Fellowship of the Rings'), ('year', 1954), ('author_id', 1)), (('id', 1),)): {}, ((('id', 2), ('title', 'The Two Towers'), ('year', 1954), ('author_id', 1)), (('id', 2),)): {}, ((('id', 3), ('title', 'Return of the King'), ('year', 1955), ('author_id', 1)), (('id', 3),)): {}, ((('id', 4), ('title', "Harry Potter and the Philosopher's Stone"), ('year', 1997), ('author_id', 2)), (('id', 4),)): {}, ((('id', 5), ('title', 'Harry Potter and the Prisoner of Azkaban'), ('year', 1999), ('author_id', 2)), (('id', 5),)): {}, ((('id', 6), ('title', 'Exhalation'), ('year', 2019), ('author_id', 3)), (('id', 6),)): {}, ((('id', 7), ('title', 'Beowolf'), ('year', 2016), ('author_id', 1)), (('id', 7),)): {}}}}},
        ['- scry.books.id: 1', '  scry.books.title: Fellowship of the Rings', '  scry.books.year: 1954', '  scry.books.author_id: 1', '- scry.books.id: 2', '  scry.books.title: The Two Towers', '  scry.books.year: 1954', '  scry.books.author_id: 1', '- scry.books.id: 3', '  scry.books.title: Return of the King', '  scry.books.year: 1955', '  scry.books.author_id: 1', '- scry.books.id: 4', "  scry.books.title: Harry Potter and the Philosopher's Stone", '  scry.books.year: 1997', '  scry.books.author_id: 2', '- scry.books.id: 5', '  scry.books.title: Harry Potter and the Prisoner of Azkaban', '  scry.books.year: 1999', '  scry.books.author_id: 2', '- scry.books.id: 6', '  scry.books.title: Exhalation', '  scry.books.year: 2019', '  scry.books.author_id: 3', '- scry.books.id: 7', '  scry.books.title: Beowolf', '  scry.books.year: 2016', '  scry.books.author_id: 1']
        ),
   Instance(
        'Simple test with implicit star columns',
        'scry.books',
        {'scry': {'children': {'books': {'table': 'books', 'columns': ['id', 'title', 'year', 'author_id']}}}},
        {'selects': [('scry.books.id', 'scry.books.id'), ('scry.books.title', 'scry.books.title'), ('scry.books.year', 'scry.books.year'), ('scry.books.author_id', 'scry.books.author_id')], 'joins': ['scry.books'], 'wheres': [], 'uniques': [('scry.books.id', 'scry.books.id')]},
        'SELECT scry.books.id, scry.books.id, scry.books.title, scry.books.year, scry.books.author_id FROM scry.books  LIMIT 100',
        {'scry': {((None,), (None,)): {'books': {((('id', 1), ('title', 'Fellowship of the Rings'), ('year', 1954), ('author_id', 1)), (('id', 1),)): {}, ((('id', 2), ('title', 'The Two Towers'), ('year', 1954), ('author_id', 1)), (('id', 2),)): {}, ((('id', 3), ('title', 'Return of the King'), ('year', 1955), ('author_id', 1)), (('id', 3),)): {}, ((('id', 4), ('title', "Harry Potter and the Philosopher's Stone"), ('year', 1997), ('author_id', 2)), (('id', 4),)): {}, ((('id', 5), ('title', 'Harry Potter and the Prisoner of Azkaban'), ('year', 1999), ('author_id', 2)), (('id', 5),)): {}, ((('id', 6), ('title', 'Exhalation'), ('year', 2019), ('author_id', 3)), (('id', 6),)): {}, ((('id', 7), ('title', 'Beowolf'), ('year', 2016), ('author_id', 1)), (('id', 7),)): {}}}}},
        ['- scry.books.id: 1', '  scry.books.title: Fellowship of the Rings', '  scry.books.year: 1954', '  scry.books.author_id: 1', '- scry.books.id: 2', '  scry.books.title: The Two Towers', '  scry.books.year: 1954', '  scry.books.author_id: 1', '- scry.books.id: 3', '  scry.books.title: Return of the King', '  scry.books.year: 1955', '  scry.books.author_id: 1', '- scry.books.id: 4', "  scry.books.title: Harry Potter and the Philosopher's Stone", '  scry.books.year: 1997', '  scry.books.author_id: 2', '- scry.books.id: 5', '  scry.books.title: Harry Potter and the Prisoner of Azkaban', '  scry.books.year: 1999', '  scry.books.author_id: 2', '- scry.books.id: 6', '  scry.books.title: Exhalation', '  scry.books.year: 2019', '  scry.books.author_id: 3', '- scry.books.id: 7', '  scry.books.title: Beowolf', '  scry.books.year: 2016', '  scry.books.author_id: 1']
        ),
   Instance(
        'Simple nested table',
        'scry.books.authors.name',
        {'scry': {'children': {'books': {'table': 'books', 'children': {'authors': {'table': 'authors', 'columns': ['name']}}}}}},
        {'selects': [('scry.authors.name', 'scry.books.authors.name')], 'joins': ['scry.books', 'LEFT JOIN scry.authors ON scry.books.author_id = scry.authors.id'], 'wheres': [], 'uniques': [('scry.books.id', 'scry.books.id'), ('scry.authors.id', 'scry.books.authors.id')]},
        'SELECT scry.books.id, scry.authors.id, scry.authors.name FROM scry.books LEFT JOIN scry.authors ON scry.books.author_id = scry.authors.id  LIMIT 100',
        {'scry': {((None,), (None,)): {'books': {((None,), (('id', 1),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((None,), (('id', 2),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((None,), (('id', 3),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((None,), (('id', 4),)): {'authors': {((('name', 'J.K. Rowling'),), (('id', 2),)): {}}}, ((None,), (('id', 5),)): {'authors': {((('name', 'J.K. Rowling'),), (('id', 2),)): {}}}, ((None,), (('id', 6),)): {'authors': {((('name', 'Ted Chiang'),), (('id', 3),)): {}}}, ((None,), (('id', 7),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}}}}},
        ['- scry.books.authors.name: J.R.R. Tolkien', '- scry.books.authors.name: J.R.R. Tolkien', '- scry.books.authors.name: J.R.R. Tolkien', '- scry.books.authors.name: J.K. Rowling', '- scry.books.authors.name: J.K. Rowling', '- scry.books.authors.name: Ted Chiang', '- scry.books.authors.name: J.R.R. Tolkien']
        ),
   Instance(
        'Simple nested table with field at both levels',
        'scry.books.authors.name scry.books.title',
        {'scry': {'children': {'books': {'table': 'books', 'children': {'authors': {'table': 'authors', 'columns': ['name']}}, 'columns': ['title']}}}},
        {'selects': [('scry.books.title', 'scry.books.title'), ('scry.authors.name', 'scry.books.authors.name')], 'joins': ['scry.books', 'LEFT JOIN scry.authors ON scry.books.author_id = scry.authors.id'], 'wheres': [], 'uniques': [('scry.books.id', 'scry.books.id'), ('scry.authors.id', 'scry.books.authors.id')]},
        'SELECT scry.books.id, scry.authors.id, scry.books.title, scry.authors.name FROM scry.books LEFT JOIN scry.authors ON scry.books.author_id = scry.authors.id  LIMIT 100',
        {'scry': {((None,), (None,)): {'books': {((('title', 'Fellowship of the Rings'),), (('id', 1),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', 'The Two Towers'),), (('id', 2),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', 'Return of the King'),), (('id', 3),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', "Harry Potter and the Philosopher's Stone"),), (('id', 4),)): {'authors': {((('name', 'J.K. Rowling'),), (('id', 2),)): {}}}, ((('title', 'Harry Potter and the Prisoner of Azkaban'),), (('id', 5),)): {'authors': {((('name', 'J.K. Rowling'),), (('id', 2),)): {}}}, ((('title', 'Exhalation'),), (('id', 6),)): {'authors': {((('name', 'Ted Chiang'),), (('id', 3),)): {}}}, ((('title', 'Beowolf'),), (('id', 7),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}}}}},
        ['- scry.books.title: Fellowship of the Rings', '  - authors.name: J.R.R. Tolkien', '- scry.books.title: The Two Towers', '  - authors.name: J.R.R. Tolkien', '- scry.books.title: Return of the King', '  - authors.name: J.R.R. Tolkien', "- scry.books.title: Harry Potter and the Philosopher's Stone", '  - authors.name: J.K. Rowling', '- scry.books.title: Harry Potter and the Prisoner of Azkaban', '  - authors.name: J.K. Rowling', '- scry.books.title: Exhalation', '  - authors.name: Ted Chiang', '- scry.books.title: Beowolf', '  - authors.name: J.R.R. Tolkien']
        ),
   Instance(
        'Nested table with field at both levels using alias',
        'scry.books.title books.authors.name',
        {'scry': {'children': {'books': {'table': 'books', 'columns': ['title'], 'children': {'authors': {'table': 'authors', 'columns': ['name']}}}}}},
        {'selects': [('scry.books.title', 'scry.books.title'), ('scry.authors.name', 'scry.books.authors.name')], 'joins': ['scry.books', 'LEFT JOIN scry.authors ON scry.books.author_id = scry.authors.id'], 'wheres': [], 'uniques': [('scry.books.id', 'scry.books.id'), ('scry.authors.id', 'scry.books.authors.id')]},
        'SELECT scry.books.id, scry.authors.id, scry.books.title, scry.authors.name FROM scry.books LEFT JOIN scry.authors ON scry.books.author_id = scry.authors.id  LIMIT 100',
        {'scry': {((None,), (None,)): {'books': {((('title', 'Fellowship of the Rings'),), (('id', 1),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', 'The Two Towers'),), (('id', 2),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', 'Return of the King'),), (('id', 3),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', "Harry Potter and the Philosopher's Stone"),), (('id', 4),)): {'authors': {((('name', 'J.K. Rowling'),), (('id', 2),)): {}}}, ((('title', 'Harry Potter and the Prisoner of Azkaban'),), (('id', 5),)): {'authors': {((('name', 'J.K. Rowling'),), (('id', 2),)): {}}}, ((('title', 'Exhalation'),), (('id', 6),)): {'authors': {((('name', 'Ted Chiang'),), (('id', 3),)): {}}}, ((('title', 'Beowolf'),), (('id', 7),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}}}}},
        ['- scry.books.title: Fellowship of the Rings', '  - authors.name: J.R.R. Tolkien', '- scry.books.title: The Two Towers', '  - authors.name: J.R.R. Tolkien', '- scry.books.title: Return of the King', '  - authors.name: J.R.R. Tolkien', "- scry.books.title: Harry Potter and the Philosopher's Stone", '  - authors.name: J.K. Rowling', '- scry.books.title: Harry Potter and the Prisoner of Azkaban', '  - authors.name: J.K. Rowling', '- scry.books.title: Exhalation', '  - authors.name: Ted Chiang', '- scry.books.title: Beowolf', '  - authors.name: J.R.R. Tolkien']
        ),
   Instance(
        'Nested table with field at both levels using alias',
        'scry.books@b.title b.authors.name',
        {'scry': {'children': {'b': {'table': 'books', 'columns': ['title'], 'children': {'authors': {'table': 'authors', 'columns': ['name']}}}}}},
        {'selects': [('b.title', 'scry.b.title'), ('scry.authors.name', 'scry.b.authors.name')], 'joins': ['scry.books AS b', 'LEFT JOIN scry.authors ON b.author_id = scry.authors.id'], 'wheres': [], 'uniques': [('b.id', 'scry.b.id'), ('scry.authors.id', 'scry.b.authors.id')]},
        'SELECT b.id, scry.authors.id, b.title, scry.authors.name FROM scry.books AS b LEFT JOIN scry.authors ON b.author_id = scry.authors.id  LIMIT 100',
        {'scry': {((None,), (None,)): {'b': {((('title', 'Fellowship of the Rings'),), (('id', 1),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', 'The Two Towers'),), (('id', 2),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', 'Return of the King'),), (('id', 3),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}, ((('title', "Harry Potter and the Philosopher's Stone"),), (('id', 4),)): {'authors': {((('name', 'J.K. Rowling'),), (('id', 2),)): {}}}, ((('title', 'Harry Potter and the Prisoner of Azkaban'),), (('id', 5),)): {'authors': {((('name', 'J.K. Rowling'),), (('id', 2),)): {}}}, ((('title', 'Exhalation'),), (('id', 6),)): {'authors': {((('name', 'Ted Chiang'),), (('id', 3),)): {}}}, ((('title', 'Beowolf'),), (('id', 7),)): {'authors': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}}}}}}},
        ['- scry.b.title: Fellowship of the Rings', '  - authors.name: J.R.R. Tolkien', '- scry.b.title: The Two Towers', '  - authors.name: J.R.R. Tolkien', '- scry.b.title: Return of the King', '  - authors.name: J.R.R. Tolkien', "- scry.b.title: Harry Potter and the Philosopher's Stone", '  - authors.name: J.K. Rowling', '- scry.b.title: Harry Potter and the Prisoner of Azkaban', '  - authors.name: J.K. Rowling', '- scry.b.title: Exhalation', '  - authors.name: Ted Chiang', '- scry.b.title: Beowolf', '  - authors.name: J.R.R. Tolkien']
        ),
   Instance(
        'simple conditional',
        'scry.books.year books.title = "Fellowship of the Rings"',
        {'scry': {'children': {'books': {'table': 'books', 'columns': ['year'], 'conditions': {'conditions': [('title', '=', "'Fellowship of the Rings'")]}}}}},
        {'selects': [('scry.books.year', 'scry.books.year')], 'joins': ['scry.books'], 'wheres': ["scry.books.title = 'Fellowship of the Rings'"], 'uniques': [('scry.books.id', 'scry.books.id')]},
        "SELECT scry.books.id, scry.books.year FROM scry.books  WHERE scry.books.title = 'Fellowship of the Rings' LIMIT 100",
        {'scry': {((None,), (None,)): {'books': {((('year', 1954),), (('id', 1),)): {}}}}},
        ['- scry.books.year: 1954']
        ),
   Instance(
        'simple conditional using alias',
        'scry.books@b.year b.title = "Fellowship of the Rings"',
        {'scry': {'children': {'b': {'table': 'books', 'columns': ['year'], 'conditions': {'conditions': [('title', '=', "'Fellowship of the Rings'")]}}}}},
        {'selects': [('b.year', 'scry.b.year')], 'joins': ['scry.books AS b'], 'wheres': ["b.title = 'Fellowship of the Rings'"], 'uniques': [('b.id', 'scry.b.id')]},
        "SELECT b.id, b.year FROM scry.books AS b  WHERE b.title = 'Fellowship of the Rings' LIMIT 100",
        {'scry': {((None,), (None,)): {'b': {((('year', 1954),), (('id', 1),)): {}}}}},
        ['- scry.b.year: 1954']
        ),
   Instance(
        'child conditional with alias',
        'scry.books@b.title,year b.authors.name = "J.R.R. Tolkien"',
        {'scry': {'children': {'b': {'table': 'books', 'columns': ['title', 'year'], 'children': {'authors': {'table': 'authors', 'conditions': {'conditions': [('name', '=', "'J.R.R. Tolkien'")]}}}}}}},
        {'selects': [('b.title', 'scry.b.title'), ('b.year', 'scry.b.year')], 'joins': ['scry.books AS b', 'LEFT JOIN scry.authors ON b.author_id = scry.authors.id'], 'wheres': ["scry.authors.name = 'J.R.R. Tolkien'"], 'uniques': [('b.id', 'scry.b.id'), ('scry.authors.id', 'scry.b.authors.id')]},
        "SELECT b.id, scry.authors.id, b.title, b.year FROM scry.books AS b LEFT JOIN scry.authors ON b.author_id = scry.authors.id  WHERE scry.authors.name = 'J.R.R. Tolkien' LIMIT 100",
        {'scry': {((None,), (None,)): {'b': {((('title', 'Fellowship of the Rings'), ('year', 1954)), (('id', 1),)): {'authors': {((None,), (('id', 1),)): {}}}, ((('title', 'The Two Towers'), ('year', 1954)), (('id', 2),)): {'authors': {((None,), (('id', 1),)): {}}}, ((('title', 'Return of the King'), ('year', 1955)), (('id', 3),)): {'authors': {((None,), (('id', 1),)): {}}}, ((('title', 'Beowolf'), ('year', 2016)), (('id', 7),)): {'authors': {((None,), (('id', 1),)): {}}}}}}},
        ['- scry.b.title: Fellowship of the Rings', '  scry.b.year: 1954', '- scry.b.title: The Two Towers', '  scry.b.year: 1954', '- scry.b.title: Return of the King', '  scry.b.year: 1955', '- scry.b.title: Beowolf', '  scry.b.year: 2016']
        ),
   Instance(
        'deep conditional',
        'scry.authors@a.books.title a.books.series_books.series.name = "Lord of the Rings"',
        {'scry': {'children': {'a': {'table': 'authors', 'children': {'books': {'table': 'books', 'columns': ['title'], 'children': {'series_books': {'table': 'series_books', 'children': {'series': {'table': 'series', 'conditions': {'conditions': [('name', '=', "'Lord of the Rings'")]}}}}}}}}}}},
        {'selects': [('scry.books.title', 'scry.a.books.title')], 'joins': ['scry.authors AS a', 'LEFT JOIN scry.books ON a.id = scry.books.author_id', 'LEFT JOIN scry.series_books ON scry.books.id = scry.series_books.book_id', 'LEFT JOIN scry.series ON scry.series_books.series_id = scry.series.id'], 'wheres': ["scry.series.name = 'Lord of the Rings'"], 'uniques': [('a.id', 'scry.a.id'), ('scry.books.id', 'scry.a.books.id'), ('scry.series.id', 'scry.a.books.series_books.series.id')]},
        "SELECT a.id, scry.books.id, scry.series.id, scry.books.title FROM scry.authors AS a LEFT JOIN scry.books ON a.id = scry.books.author_id LEFT JOIN scry.series_books ON scry.books.id = scry.series_books.book_id LEFT JOIN scry.series ON scry.series_books.series_id = scry.series.id  WHERE scry.series.name = 'Lord of the Rings' LIMIT 100",
        {'scry': {((None,), (None,)): {'a': {((None,), (('id', 1),)): {'books': {((('title', 'Fellowship of the Rings'),), (('id', 1),)): {'series_books': {((None,), (None,)): {'series': {((None,), (('id', 1),)): {}}}}}, ((('title', 'The Two Towers'),), (('id', 2),)): {'series_books': {((None,), (None,)): {'series': {((None,), (('id', 1),)): {}}}}}, ((('title', 'Return of the King'),), (('id', 3),)): {'series_books': {((None,), (None,)): {'series': {((None,), (('id', 1),)): {}}}}}}}}}}},
        ['- scry.a.books.title: Fellowship of the Rings', '- scry.a.books.title: The Two Towers', '- scry.a.books.title: Return of the King']
        ),
   Instance(
        'deep conditional on prefix',
        'scry.authors.books.title authors:books.series_books.series.name = "Lord of the Rings"',
        {'scry': {'children': {'authors': {'table': 'authors', 'children': {'books': {'table': 'books', 'columns': ['title']}}, 'conditions': {'children': {'books': {'table': 'books', 'children': {'series_books': {'table': 'series_books', 'children': {'series': {'table': 'series', 'conditions': [('name', '=', "'Lord of the Rings'")]}}}}}}}}}}},
        {'selects': [('scry.books.title', 'scry.authors.books.title')], 'joins': ['scry.authors', 'LEFT JOIN scry.books ON scry.authors.id = scry.books.author_id'], 'wheres': ["authors.id IN (SELECT scry.authors.id FROM scry.authors LEFT JOIN scry.books ON scry.authors.id = scry.books.author_id LEFT JOIN scry.series_books ON scry.books.id = scry.series_books.book_id LEFT JOIN scry.series ON scry.series_books.series_id = scry.series.id WHERE scry.series.name = 'Lord of the Rings')"], 'uniques': [('scry.authors.id', 'scry.authors.id'), ('scry.books.id', 'scry.authors.books.id')]},
        "SELECT scry.authors.id, scry.books.id, scry.books.title FROM scry.authors LEFT JOIN scry.books ON scry.authors.id = scry.books.author_id  WHERE authors.id IN (SELECT scry.authors.id FROM scry.authors LEFT JOIN scry.books ON scry.authors.id = scry.books.author_id LEFT JOIN scry.series_books ON scry.books.id = scry.series_books.book_id LEFT JOIN scry.series ON scry.series_books.series_id = scry.series.id WHERE scry.series.name = 'Lord of the Rings') LIMIT 100",
        {'scry': {((None,), (None,)): {'authors': {((None,), (('id', 1),)): {'books': {((('title', 'Fellowship of the Rings'),), (('id', 1),)): {}, ((('title', 'The Two Towers'),), (('id', 2),)): {}, ((('title', 'Return of the King'),), (('id', 3),)): {}, ((('title', 'Beowolf'),), (('id', 7),)): {}}}}}}},
        ['- scry.authors.books.title: Fellowship of the Rings', '- scry.authors.books.title: The Two Towers', '- scry.authors.books.title: Return of the King', '- scry.authors.books.title: Beowolf']
        ),
   Instance(
        'deep conditional on prefix with alias',
        'scry.authors@a.books.title a:books.series_books.series.name = "Lord of the Rings"',
        {'scry': {'children': {'a': {'table': 'authors', 'children': {'books': {'table': 'books', 'columns': ['title']}}, 'conditions': {'children': {'books': {'table': 'books', 'children': {'series_books': {'table': 'series_books', 'children': {'series': {'table': 'series', 'conditions': [('name', '=', "'Lord of the Rings'")]}}}}}}}}}}},
        {'selects': [('scry.books.title', 'scry.a.books.title')], 'joins': ['scry.authors AS a', 'LEFT JOIN scry.books ON a.id = scry.books.author_id'], 'wheres': ["a.id IN (SELECT scry.authors.id FROM scry.authors LEFT JOIN scry.books ON a.id = scry.books.author_id LEFT JOIN scry.series_books ON scry.books.id = scry.series_books.book_id LEFT JOIN scry.series ON scry.series_books.series_id = scry.series.id WHERE scry.series.name = 'Lord of the Rings')"], 'uniques': [('a.id', 'scry.a.id'), ('scry.books.id', 'scry.a.books.id')]},
        "SELECT a.id, scry.books.id, scry.books.title FROM scry.authors AS a LEFT JOIN scry.books ON a.id = scry.books.author_id  WHERE a.id IN (SELECT scry.authors.id FROM scry.authors LEFT JOIN scry.books ON a.id = scry.books.author_id LEFT JOIN scry.series_books ON scry.books.id = scry.series_books.book_id LEFT JOIN scry.series ON scry.series_books.series_id = scry.series.id WHERE scry.series.name = 'Lord of the Rings') LIMIT 100",
        {'scry': {((None,), (None,)): {'a': {((None,), (('id', 1),)): {'books': {((('title', 'Beowolf'),), (('id', 7),)): {}, ((('title', 'Return of the King'),), (('id', 3),)): {}, ((('title', 'The Two Towers'),), (('id', 2),)): {}, ((('title', 'Fellowship of the Rings'),), (('id', 1),)): {}}}}}}},
        ['- scry.a.books.title: Beowolf', '- scry.a.books.title: Return of the King', '- scry.a.books.title: The Two Towers', '- scry.a.books.title: Fellowship of the Rings']
        ),
   Instance(
        'Terminator to select no fields',
        'scry.authors@a% a.name',
        {'scry': {'children': {'a': {'table': 'authors', 'columns': ['name']}}}},
        {'selects': [('a.name', 'scry.a.name')], 'joins': ['scry.authors AS a'], 'wheres': [], 'uniques': [('a.id', 'scry.a.id')]},
        'SELECT a.id, a.name FROM scry.authors AS a  LIMIT 100',
        {'scry': {((None,), (None,)): {'a': {((('name', 'J.R.R. Tolkien'),), (('id', 1),)): {}, ((('name', 'J.K. Rowling'),), (('id', 2),)): {}, ((('name', 'Ted Chiang'),), (('id', 3),)): {}}}}},
        ['- scry.a.name: J.R.R. Tolkien', '- scry.a.name: J.K. Rowling', '- scry.a.name: Ted Chiang']
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


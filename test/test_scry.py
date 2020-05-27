import psycopg2

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

def test_foreign_keys:
    pass

def test_scry():
    pass


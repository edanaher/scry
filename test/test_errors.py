import psycopg2
import pytest
from dataclasses import dataclass

from scry import scry
import lark

@dataclass
class ErrorInstance:
    name: str
    query: str
    error: str

error_instances = [
    ErrorInstance(
        "Misnamed column on inner table",
        "books.authors.nam",
        "Unknown table or column: nam"
    ),
    # TODO: this one could be better.
    ErrorInstance(
        "Single unknown word",
        "asdf",
        "Unable to resolve schema for asdf"
    ),
    ErrorInstance(
        "Unknown middle table in chain",
        "books.asdf.foo",
        "Unknown table or column: asdf"
    ),
    ErrorInstance(
        "attempt to join two existing tables that don't join",
        "authors.series",
        "No known join of series to authors"
    ),
]

def run_test(instance):
    db = psycopg2.connect("")
    cur = db.cursor()

    try:
        table_info = scry.get_table_info(cur)
        foreign_keys = scry.get_foreign_keys(cur)
        unique_keys = scry.get_unique_keys(cur)
        keys = {"unique": unique_keys, "foreign": foreign_keys}

        settings = scry.default_settings()

        tree, aliases, _, _ = scry.parse(settings, table_info, foreign_keys, instance.query)

        sql_clauses = scry.generate_sql(keys, tree)

        sql = scry.serialize_sql(sql_clauses, 100)
        cur.execute(sql)

        results = scry.reshape_results(cur, sql_clauses)

        output = scry.format_results(results)
    except lark.exceptions.LarkError as e:
        if isinstance(e.__context__, scry.ScryException):
            assert str(e.__context__) == instance.error
        else:
            assert str(e) == instance.error
    except scry.ScryException as e:
        assert str(e) == instance.error

@pytest.mark.parametrize("instance", error_instances, ids=lambda i: i.name)
def test_errors(instance):
    run_test(instance)


# Scry
### A simple language wrapping SQL

Writing SQL is a pain - all the joins, group bys, and so on.  A lot of it should be automatic - your schema is probably structurd such that a sufficiently smart tool could figure out how to get from table A to table B.

Scry is that sufficiently smart tool.

At least, if you're using Postgres and foreign keys for joins, and can deal with some rough edges.  Otherwise, it's not that tool.  Yet.

## Installation

If you have Postgres headers installed, `python3 -m pip install git+https://github.com/edanaher/scry` should do the trick.

Pypi package coming once this has stabilized a bit.

## Usage

Command line options:

```
usage: scry [-h] [-c COMMAND] [-d DATABASE] [-l LIMIT] [-s SCHEMA]

optional arguments:
  -h, --help            show this help message and exit
  -c COMMAND, --command COMMAND
                        command to run
  -d DATABASE, --database DATABASE
                        database to connect to
  -l LIMIT, --limit LIMIT
                        row limit (0 for no limit)
  -s SCHEMA, --schema SCHEMA
                        default schema
```

If a command is given, it is run and scry exits; otherwise it drops into a REPL (with auto-completion!).  To exit the REPL, type `quit`, `exit`, or use Ctrl-D to send end-of-file.

The database is passed to libpq; the default is "", which is roughly equivalent to `postgresql://$USER@/$USER`.  You likely want to use `postgresql://postgres@` or `user=postgres` if you have a standard installation using the `postgres` user.  Of course, any standard Postgres connection string will work.

The limit is the number of rows returned from Postgres; this doesn't necessarily correspond to a meaningful count of values returned from scry (yet).  However, this avoids returning way too much data.

The schema is... currently in flux.  Right now it does nothing, but likely will do something again in the near future.

## Language description

### Queries

A scry query consists of a number of components; each can select a set of columns to return, (possibly joined through multiple tables), add a condition to be applied to the return set, add an alias to a table, or more than one of the above.

In general, tables are chained-together in an object-like style, and all components are merged together into one giant query (helpfully printed before the results.)  The results are also printed in a tree-like structure (by default, and always for now).  For example, using the test schema and data defined in test/info.sql:

The joins are figured out automatically using foreign key constraints; if a table has a foreign key constraint to another table, it's assumed that that's how the two tables should be joined.

```
> authors.name authors.books.title
SELECT scry.authors.id, scry.books.id, scry.authors.name, scry.books.title FROM scry.authors LEFT JOIN scry.books ON scry.authors.id = scry.books.author_id  LIMIT 100
- scry.authors.name: J.R.R. Tolkien
  - books.title: Fellowship of the Rings
  - books.title: The Two Towers
  - books.title: Return of the King
  - books.title: Beowolf
- scry.authors.name: J.K. Rowling
  - books.title: Harry Potter and the Philosopher's Stone
  - books.title: Harry Potter and the Prisoner of Azkaban
- scry.authors.name: Ted Chiang
  - books.title: Exhalation
```

This prints out each author's name, along with the title of every book they wrote.

Tables can also be more deeply nested, and multiple (comma-separated) columns can be selected:

```
> users.name  users.favorites.books.title,year  users.favorites.books.authors.name
SELECT scry.users.id, scry.favorites.user_id, scry.favorites.book_id, scry.books.id, scry.authors.id, scry.users.name, scry.books.title, scry.books.year, scry.authors.name FROM scry.users LEFT JOIN scry.favorites ON scry.users.id = scry.favorites.user_id LEFT JOIN scry.books ON scry.favorites.book_id = scry.books.id LEF
T JOIN scry.authors ON scry.books.author_id = scry.authors.id  LIMIT 100
- scry.users.name: Winnie the Pooh
  - favorites.books.title: Harry Potter and the Philosopher's Stone
    favorites.books.year: 1997
    - authors.name: J.K. Rowling
  - favorites.books.title: Harry Potter and the Prisoner of Azkaban
    favorites.books.year: 1999
    - authors.name: J.K. Rowling
- scry.users.name: Tigger
  - favorites.books.title: Harry Potter and the Philosopher's Stone
    favorites.books.year: 1997
    - authors.name: J.K. Rowling
  - favorites.books.title: Exhalation
    favorites.books.year: 2019
    - authors.name: Ted Chiang
- scry.users.name: Piglet
  - favorites.books.title: None
    favorites.books.year: None
    - authors.name: None
```

Note that for each user, we join through multiple tables to get the title and publication year of their favorite books, as well as those books authors.  And If there is no favorite, we get None.  That seems like a bug.

But you don't have to write out all of those tables every time; each table can only occur once in the query (unless aliased as described below), so just giving the unqualified table name will join in into the query at the appropriate point:

```
> users.name users.favorites.books.title,year books.authors.name
[ currently broken, but should be the same as above ]
```

Still, if you have long table names, than can be unwieldy.  So you can also use `table@alias` to alias a table to a shorter name (like `AS` in sql) for later in the query:

```
> users@u.name u.favorites.books@b.title,year b.authors.name
[ Also broken ]
```

However, if a table is aliased, that alias must be used in the rest of the query; in `books@b.title books.year`, `books` and `b` refer to two different instances of the `books` table.  (And as multiple top-level tables aren't currently supported, this will give an error.)  However, this does allow for joins at multiple levels, which will be more useful with conditions.

Instead of individual columns, `*` or no column can be provided; this selects all columns from the table:

```
> authors
SELECT scry.authors.id, scry.authors.id, scry.authors.name FROM scry.authors  LIMIT 100
- scry.authors.id: 1
  scry.authors.name: J.R.R. Tolkien
- scry.authors.id: 2
  scry.authors.name: J.K. Rowling
- scry.authors.id: 3
  scry.authors.name: Ted Chiang
```

Experimental: If the final table in a path is followed by '%', no output is produced; however, aliases are produced.  This can be useful to save some typing:

```
> users.name users.favorites.books@b% b.year b.authors.name
```

In this case, the second path just generates the `b` alias which is used later in the query.

However, this seems like a weird syntax that may change.  A trailing period seems like a good way to do this: `users.favorites.books@b.` can be read as an empty list.  But this makes the parser whitespace-dependent: `books@b. b.` will currently be parsed as `books@b.b.`, which is bad.  A slightly uglier alternative `books@b., b.title`, which can be read as a comma-separated empty-list, or as a comma separating two paths.  That's probably better.


### Conditions

The set of results can be constrained by conditions of the form `table.column [op] value`, where op is one of the standard SQL (in)equality operators (`<=`, `<`, `==`, `<>`, `>=`, `>`), `LIKE`, or `ILIKE`.  The field may have multiple join tables before it.  However, `value` (for now) must be a literal number or string, not another column.

```
> books.authors books.title = "Fellowship of the Rings"
SELECT scry.books.id, scry.authors.id, scry.authors.id, scry.authors.name FROM scry.books LEFT JOIN scry.authors ON scry.books.author_id = scry.authors.id  WHERE scry.books.title = 'Fellowship of the Rings' LIMIT 100
- scry.books.authors.id: 1
  scry.books.authors.name: J.R.R. Tolkien

> books.title,year books.year < 1960
SELECT scry.books.id, scry.books.title, scry.books.year FROM scry.books  WHERE scry.books.year < 1960 LIMIT 100
- scry.books.title: Fellowship of the Rings
  scry.books.year: 1954
- scry.books.title: The Two Towers
  scry.books.year: 1954
- scry.books.title: Return of the King
  scry.books.year: 1955
```

This makes it sensible to have multiple copies of a table; for example, to find all books by the author of "Fellowship of the Rings", select the book whose title is "Lord of the Rings", and then find its author and all of his books:

```
> books.authors.books@b.title books.title = "Fellowship of the Rings"
SELECT scry.books.id, scry.authors.id, b.id, b.title FROM scry.books LEFT JOIN scry.authors ON scry.books.author_id = scry.authors.id LEFT JOIN scry.books AS b ON scry.authors.id = b.author_id  WHERE scry.books.title = 'Fellowship of the Rings' LIMIT 100
- scry.books.authors.b.title: Fellowship of the Rings
- scry.books.authors.b.title: The Two Towers
- scry.books.authors.b.title: Return of the King
- scry.books.authors.b.title: Beowolf
```

However, there's another way to do this - a condition doesn't have to be on the whole path: `:` can be read as "such that":

```
> authors.books.title authors:books.title = "Fellowship of the Rings"
SELECT scry.authors.id, scry.books.id, scry.books.title FROM scry.authors LEFT JOIN scry.books ON scry.authors.id = scry.books.author_id  WHERE authors.id IN (SELECT scry.authors.id FROM scry.authors LEFT JOIN scry.books ON scry.authors.id = scry.books.author_id WHERE scry.books.title = 'Fellowship of the Rings') LIMIT
100
- scry.authors.books.title: Fellowship of the Rings
- scry.authors.books.title: The Two Towers
- scry.authors.books.title: Return of the King
- scry.authors.books.title: Beowolf
```

This condition can be read as "authors such that they join to a row of books with title 'Fellowship of the Rings'"; in other words, this finds authors that have a book named "Fellowship of the Rings", and then prints their books' titles.  Tables named after a colon are separately namespaced from the rest of the query.

Note that without the colon, this restricts to only books that have that title, so only gives back the one book:

```
> authors.books.title authors.books.title = "Fellowship of the Rings"
SELECT scry.authors.id, scry.books.id, scry.books.title FROM scry.authors LEFT JOIN scry.books ON scry.authors.id = scry.books.author_id  WHERE scry.books.title = 'Fellowship of the Rings' LIMIT 100
- scry.authors.books.title: Fellowship of the Rings
```

## Implementation

TODO

## Tests

Currently, the tests are pretty much generated, and require the database to be set up just right.  There's a TODO to fix this.

## TODO:
- use of aliases before declaration(?): "b.authors.name books@b.title"
- aggregations
- proper schema inference (cross-schema joins: track all possible schemas)
  - search\_path to limit 
- path finding (table1..table3, find join via table.table2.table3)
- config file - connection strings, aliases
- list tables in schema
- Fuller tab completion in REPL (aliases, schemas)
- schema aliases
- generally squash the bugs and clean some things up.
- make tests easier to run.

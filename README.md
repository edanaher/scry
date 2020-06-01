# Scry
### A simple language wrapping SQL

Writing SQL is a pain - all the joins, group bys, and so on.  A lot of it should be automatic - your schema is probably structurd such that a sufficiently smart tool could figure out how to get from table A to table B.

Scry is that sufficiently smart tool.

## Usage

TODO

## Language description

TODO

## Implementation

TODO

## TODO:
- use of aliases before declaration(?): "b.authors.name books@b.title"
- aggregations
- proper schema inference (cross-schema joins: track all possible schemas)
  - search\_path to limit 
- path finding (table1..table3, find join via table.table2.table3
- config file - connection strings, aliases
- list tables in schema
- Fuller tab completion in REPL (aliases, schemas)
- schema aliases

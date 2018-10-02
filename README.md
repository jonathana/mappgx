# mappgx - Go library to return postgres queries as better typed maps

There are some limitations in the db/sql package and how drivers implement it that make even packages like
sqlx's MapScan have trouble returning postgres' rich datatypes.  While the metadata is available via db/sql
interfaces, pushing that up the stack hasn't widely happened (yet) AFAICT.  pgx does offer the ability to
get richer types out, but lacks the ability to return them in a map[string]interface{} or
[]map[string]interface{}.  This package provides RowMap() and RowsMap() to do just that.

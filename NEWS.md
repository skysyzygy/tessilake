# tessilake 0.4.4
- fixed bug preventing "prefer" being used with cache_update, and limitations with date-only updates with Apache Arrow
- exported local_cache_dirs for use in other packages

# tessilake 0.4.3
- added "prefer" option to cache_update / update_table for choosing which 
table to use on conflicting date-only updates
- decoupled partitioning choice from primary key choice in write_cache

# tessilake 0.4.2
- fixed bug in update_table that was updating already existing rows in `to` on date-only updates
- update_table now works with new columns in `from`
- read_cache and write_cache both work with partitioned tables that have multiple primary keys
- write_cache now has a flag to choose whether or not to sync to other storage; default is TRUE for legacy usage

# tessilake 0.4.1
- fix for timestamp updating so that it will work with paths with spaces 
- fix for timestamp updating so that all files end up with exactly the same timestamp

# tessilake 0.4.0
- major update deprecating the old depth-specific read_cache/write_cache.
- read_cache now always reads from the most-recently updated cache
- write_cache now writes to the primary (first) depth cache and then syncs using cache_write, cache_update or file.copy to the other depths.
_ bug fixes for ... dispatch and cache_get_mtime

# tessilake 0.3.0
- New config.yml format for tessilake allowing for more flexible storage configuration
- Updates to documentation and exporting read_cache/write_cache
- Simplified/more flexible implementation of read_sql that loops through storage depths
- read_cache retries multiple times on read failure

# tessilake 0.2.4
- Reverted: Adding local timezone information to all UTC dates when loaded through read_sql, as it is not compatible with latest SQL Server and ODBC combination.

# tessilake 0.2.3
- Added the ability to incrementally update by date only, which will allow for incremental updating on the audit table among others
- Added local timezone information to all UTC dates when loaded through read_sql

# tessilake 0.2.2
- Added incremental feature to read_tessi, read_sql_table, read_sql, cache_update, and update_table

# tessilake 0.2.1
- Hot fix for read_tessi issues (delete=TRUE when date_column is set and reading from sql).

# tessilake 0.2.0

* Beta release!
- Fixed warning about ORDER_BY clause when updating table
- Fixed logic for update_table so that it only incrementally updates when there are primary keys and a date column to reference
- Fixed deadlock issue by making all loads non-locking
- Fixed file collision when multiple processes are using the library simultaneously
- Fixed race condition in update_table when table is updated quickly
- Acceptance tests to make sure that update_table is actually faster than a full download
- Enhancements to tessi_table.yml to be able to provide explicit queries and now marking which tables are not incrementally loaded 
- Creditee has group_customer_no too 

# tessilake 0.1.0

* Alpha release!

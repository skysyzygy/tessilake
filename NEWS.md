# tessilake 0.2.1
- Hot fix for read_tessi issues (delete=TRUE when date_column is set and reading from sql).

# tessilake 0.2.0

* Beta release!
- Fixed warning about ORDER_BY clause when updating table
- Fixed logic for update_table so that it only incrementally updates when there are primary keys and a date column to reference
- Fixed deadlock issue by making all loads nonlocking
- Fixed file collision when multiple processes are using the library simultaneously
- Fixed race condition in update_table when table is updated quickly
- Acceptance tests to make sure that update_table is actually faster than a full download
- Enhancements to tessi_table.yml to be able to provide explicit queries and now marking which tables are not incrementally loaded 
- Creditee has group_customer_no too 

# tessilake 0.1.0

* Alpha release!

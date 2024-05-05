withr::local_package("checkmate")
withr::local_package("mockery")
local_cache_dirs()

# sql_connect -------------------------------------------------------------
stub(sql_connect, "odbc::odbc", RSQLite::SQLite())
stub(sql_connect, "config::get", list(tessitura = ":memory:"))

test_that("sql_connect connects to the database", {
  expect_true(is.null(tessilake:::db$db))
  sql_connect()
  expect_false(is.null(tessilake:::db$db))
})

test_that("sql_connect only connects once", {
  assign("db", NULL, rlang::ns_env("tessilake")$db)
  sql_connect()
  ptr1 <- attributes(tessilake:::db$db)$ptr
  sql_connect()
  ptr2 <- attributes(tessilake:::db$db)$ptr
  expect_equal(ptr1, ptr2)
})

rm(sql_connect)
test_that("sql_connect throws an error when it can't connect", {
  sql_disconnect()
  stub(sql_connect, "config::get", list(tessitura = "not a database"))
  expect_error(suppressMessages(sql_connect()), "DSN")
})

# read_sql ----------------------------------------------------------------
data <- data.table(x = 1:1000, y = runif(1000), last_update_dt = lubridate::now(tzone = "EST") + lubridate::ddays(1))
db$db <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

test_that("read_sql preserves primary_key across runs", {
  data <- copy(data)
  stub(read_sql, "tbl", data)
  setattr(data, "primary_keys", "x")

  ignore_attr <- c("partition_key","partitioning")

  read_sql("data_with_attr")
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "deep", "tessi")), data)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "shallow", "tessi")), data)
  read_sql("data_with_attr", freshness = 0)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "deep", "tessi")), data, ignore_attr = ignore_attr)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "shallow", "tessi")), data, ignore_attr = ignore_attr)
})

test_that("read_sql passes select on to read_cache", {
  table <- data.table(id = 1:1000, y = 2:1001)
  read_cache <- mock(table, cycle = T)
  stub(read_sql, "tbl", table)
  stub(read_sql, "read_cache", read_cache)
  read_sql("tbl", freshness = 0, select = "id")
  expect_equal(mock_args(read_cache)[[1]][["select"]], "id")
})

test_that("read_sql passes incremental on to write_cache", {
  table <- data.table(id = 1:1000, y = 2:1001)
  write_cache <- mock(table, cycle = T)
  stub(read_sql, "tbl", table)
  stub(read_sql, "write_cache", write_cache)
  read_sql("tbl", freshness = 0, incremental = TRUE)
  expect_true(mock_args(write_cache)[[1]][["incremental"]])
})


test_that("read_sql works with database", {
  db$db <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  data <- dplyr::copy_to(db$db, data, "data_with_tbl")

  read_sql("select * from data_with_tbl", "data_with_tbl")
  expect_equal(collect(cache_read("data_with_tbl", "deep", "tessi")), setDT(collect(data)), ignore_attr="class")
  expect_equal(collect(cache_read("data_with_tbl", "shallow", "tessi")), setDT(collect(data)), ignore_attr="class")

  read_sql("select * from data_with_tbl", "data_with_tbl", freshness = 0)
  expect_equal(collect(cache_read("data_with_tbl", "deep", "tessi")), setDT(collect(data)), ignore_attr="class")
  expect_equal(collect(cache_read("data_with_tbl", "shallow", "tessi")), setDT(collect(data)), ignore_attr="class")
})

test_that("read_sql updates cache iff it's not fresh enough", {
  # make new caches
  stub(read_sql, "tbl", mock(data, cycle = T))
  read_sql("data_fresh", "data_fresh", primary_keys = "x", date_column = "last_update_dt")

  test_time <- lubridate::now()

  # don't update anything -- it's fresh
  read_sql("data_fresh", "data_fresh", primary_keys = "x", date_column = "last_update_dt")

  mtime_parquet <- file.mtime(file.path(tempdir(), "deep", "tessi", "data_fresh.parquet"))
  mtime_feather <- file.mtime(file.path(tempdir(), "shallow", "tessi", "data_fresh.feather"))
  expect_lt(mtime_parquet, test_time)
  expect_lt(mtime_feather, test_time)

  # don't update anything because one of the storages if fresh enough
  stub(read_sql, "cache_get_mtime", mock(lubridate::now(), lubridate::now() + lubridate::ddays(1)))
  read_sql("data_fresh", "data_fresh", primary_keys = "x", date_column = "last_update_dt", freshness = 0)

  mtime_parquet <- file.mtime(file.path(tempdir(), "deep", "tessi", "data_fresh.parquet"))
  mtime_feather <- file.mtime(file.path(tempdir(), "shallow", "tessi", "data_fresh.feather"))
  expect_lt(mtime_parquet, test_time)
  expect_lt(mtime_feather, test_time)

  # updates deep and shallow because they're now both older than data and they're stale
  stub(read_sql, "cache_get_mtime", lubridate::now() - lubridate::ddays(9))
  Sys.sleep(1)
  read_sql("data_fresh", "data_fresh", primary_keys = "x", date_column = "last_update_dt")

  mtime_parquet <- file.mtime(file.path(tempdir(), "deep", "tessi", "data_fresh.parquet"))
  mtime_feather <- file.mtime(file.path(tempdir(), "shallow", "tessi", "data_fresh.feather"))

  expect_gt(mtime_parquet, test_time)
  expect_gt(mtime_feather, test_time)

  # there aren't any other weird files around from partitioning, etc...
  expect_length(dir(tempdir(),pattern = "data_fresh",
                    recursive = T,include.dirs = T),2)
})


# read_sql_table ----------------------------------------------------------

stub(sql_connect, "odbc::odbc", RSQLite::SQLite())
stub(sql_connect, "config::get", list(tessitura = ":memory:"))
available_columns <- readRDS(test_path("available_columns.Rds"))
# stub list of tables
stub(read_sql_table, "dbListTables", mock("TR_SEASON", "VT_SEASON", cycle = TRUE))

test_that("read_sql_table throws an error when a table doesn't exist", {
  expect_error(read_sql_table("table_doesnt_exist"), "doesnt_exist doesn't exist")
})

test_that("read_sql_table complains if asked to select or pk columns that don't exist", {
  m_read <- mock(available_columns, available_columns, NULL, cycle = T)
  stub(read_sql_table, "read_sql", m_read)
  expect_error(read_sql_table("TR_SEASON", select = "columnThatDoesntExist"), "select")
  expect_silent(read_sql_table("TR_SEASON", select = "id"))
  expect_error(read_sql_table("TR_SEASON", primary_keys = "columnThatDoesntExist"), "primary_keys")
  expect_silent(read_sql_table("TR_SEASON", primary_keys = "id"))
})

test_that("read_sql_table passes select on to read_sql", {
  table <- data.table(id = 1:1000, y = 2:1001)
  m_read <- mock(available_columns, table, cycle = T)
  stub(read_sql_table, "read_sql", m_read)
  read_sql_table("TR_SEASON", freshness = 0, select = "id")
  expect_equal(mock_args(m_read)[[2]][["select"]], "id")
})

test_that("read_sql_table passes incremental on to read_sql", {
  table <- data.table(id = 1:1000, y = 2:1001)
  m_read <- mock(available_columns, table, cycle = T)
  stub(read_sql_table, "read_sql", m_read)
  read_sql_table("TR_SEASON", freshness = 0, incremental = TRUE)
  expect_equal(mock_args(m_read)[[2]][["incremental"]], TRUE)
})

test_that("read_sql_table works with database", {
  m_read <- mock(available_columns, NULL, cycle = T)
  stub(read_sql_table, "read_sql", m_read)
  read_sql_table("TR_SEASON", primary_keys = "id", date_column = "last_update_dt")
  expect_match(as.character(mock_args(m_read)[[2]][["query"]]), "select .* from dbo.TR_SEASON")
})

test_that("read_sql_table reads a table from the database in `dbo` and `BI` schemas and caches it", {
  m_read <- mock(available_columns, NULL, cycle = T)
  stub(read_sql_table, "read_sql", m_read)

  read_sql_table("TR_SEASON")
  read_sql_table("VT_SEASON", "BI")

  expect_equal(mock_args(m_read)[[2]][["name"]], "dbo.TR_SEASON")
  expect_equal(mock_args(m_read)[[4]][["name"]], "BI.VT_SEASON")
})

test_that("read_sql_table finds the primary keys and last_update_dt columns", {
  m_read <- mock(available_columns, NULL, cycle = T)
  stub(read_sql_table, "read_sql", m_read)
  setDT(available_columns)[table_name == "TR_SEASON", constraint_type := ifelse(column_name == "description", "PRIMARY KEY", NA)]

  read_sql_table("TR_SEASON")

  expect_equal(mock_args(m_read)[[2]][["primary_keys"]], "description")
  expect_equal(mock_args(m_read)[[2]][["date_column"]], "last_update_dt")

  available_columns <- data.table(
    table_schema = "dbo", table_name = "TR_SEASON",
    column_name = "blah", constraint_type = "PRIMARY KEY",
    character_maximum_length = 1
  )

  read_sql_table("TR_SEASON")

  expect_equal(mock_args(m_read)[[4]][["primary_keys"]], "blah")
  expect_equal(mock_args(m_read)[[4]][["date_column"]], NULL)

  available_columns <- data.table(
    table_schema = "dbo", table_name = "TR_SEASON",
    column_name = "blah", constraint_type = NA,
    character_maximum_length = 1
  )

  read_sql_table("TR_SEASON")

  expect_equal(mock_args(m_read)[[6]][["primary_keys"]], NULL)
  expect_equal(mock_args(m_read)[[6]][["date_column"]], NULL)
})

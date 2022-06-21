withr::local_package("checkmate")
withr::local_package("mockery")
local_cache_dirs()

# sql_connect -------------------------------------------------------------
stub(sql_connect,"odbc::odbc",RSQLite::SQLite())
stub(sql_connect,"config::get",":memory:")

test_that("sql_connect connects to the database", {
  expect_true(is.null(tessilake:::db$db))
  sql_connect()
  expect_false(is.null(tessilake:::db$db))
})

test_that("sql_connect only connects once", {
  assign("db",NULL,rlang::ns_env("tessilake")$db)
  sql_connect()
  ptr1 <- attributes(tessilake:::db$db)$ptr
  sql_connect()
  ptr2 <- attributes(tessilake:::db$db)$ptr
  expect_equal(ptr1, ptr2)
})

rm(sql_connect)
test_that("sql_connect throws an error when it can't connect", {
  sql_disconnect()
  stub(sql_connect, "config::get", "not a database")
  expect_error(suppressMessages(sql_connect()), "DSN")
})

# read_sql ----------------------------------------------------------------
data <- data.table(x = 1:1000, y = runif(1000), last_update_dt = lubridate::now(tzone = "EST") + lubridate::ddays(1))

test_that("read_sql preserves primary_key across runs", {
  data = copy(data)
  stub(read_sql, "tbl", data)
  setattr(data,"primary_keys", "x")

  read_sql("data_with_attr")
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "deep", "tessi")), data)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "shallow", "tessi")), data)

  read_sql("data_with_attr",freshness = 0)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "deep", "tessi")), data)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "shallow", "tessi")), data)
})

test_that("read_sql works with database", {
  db$db <- DBI::dbConnect(RSQLite::SQLite(),":memory:")
  data <- dplyr::copy_to(db$db,data,"data_with_tbl")

  read_sql("select * from data_with_tbl", "data_with_tbl")
  expect_equal(collect(cache_read("data_with_tbl", "deep", "tessi")), collect(data))
  expect_equal(collect(cache_read("data_with_tbl", "shallow", "tessi")), collect(data))

  read_sql("select * from data_with_tbl", "data_with_tbl", freshness=0)
  expect_equal(collect(cache_read("data_with_tbl", "deep", "tessi")), collect(data))
  expect_equal(collect(cache_read("data_with_tbl", "shallow", "tessi")), collect(data))
})

test_that("read_sql updates cache iff it's not fresh enough", {
  # make new caches
  stub(read_sql, "tbl", mock(data, cycle = T))
  read_sql("data_fresh","data_fresh",primary_keys = "x",date_column = "last_update_dt")

  test_time <- lubridate::now()

  # don't update anything -- it's fresh
  read_sql("data_fresh","data_fresh",primary_keys = "x",date_column = "last_update_dt")

  mtime_parquet <- file.mtime(file.path(tempdir(), "deep", "tessi", "data_fresh.parquet"))
  mtime_feather <- file.mtime(file.path(tempdir(), "shallow", "tessi", "data_fresh.feather"))
  expect_lt(mtime_parquet, test_time)
  expect_lt(mtime_feather, test_time)

  # updates deep with shallow but not deep because deep is fresher than last_update_dt
  stub(read_sql, "cache_get_mtime", mock(lubridate::now(), lubridate::now() - lubridate::ddays(8)))
  read_sql("data_fresh","data_fresh",primary_keys = "x",date_column = "last_update_dt", freshness=0)

  mtime_parquet <- file.mtime(file.path(tempdir(), "deep", "tessi", "data_fresh.parquet"))
  mtime_feather <- file.mtime(file.path(tempdir(), "shallow", "tessi", "data_fresh.feather"))

  expect_lt(mtime_parquet, test_time)
  expect_gt(mtime_feather, test_time)

  # updates nothing because deep and shallow are fresh enough
  rm(read_sql)
  stub(read_sql, "tbl", mock(data, cycle = T))
  data[, last_update_dt := lubridate::now() - lubridate::ddays(8)]
  read_sql("data_fresh","data_fresh",primary_keys = "x",date_column = "last_update_dt", freshness=0)

  mtime_parquet <- file.mtime(file.path(tempdir(), "deep", "tessi", "data_fresh.parquet"))
  mtime_feather <- file.mtime(file.path(tempdir(), "shallow", "tessi", "data_fresh.feather"))

  expect_lt(mtime_parquet, test_time)
  expect_gt(mtime_feather, test_time)

  # updates deep and shallow because they're now both older than data and they're stale
  stub(read_sql, "cache_get_mtime", lubridate::now() - lubridate::ddays(9))
  read_sql("data_fresh","data_fresh",primary_keys = "x",date_column = "last_update_dt")

  mtime_parquet <- file.mtime(file.path(tempdir(), "deep", "tessi", "data_fresh.parquet"))
  mtime_feather <- file.mtime(file.path(tempdir(), "shallow", "tessi", "data_fresh.feather"))

  expect_gt(mtime_parquet, test_time)
  expect_gt(mtime_feather, test_time)
})


# read_sql_table ----------------------------------------------------------

stub(sql_connect,"odbc::odbc",RSQLite::SQLite())
stub(sql_connect,"config::get",":memory:")
available_columns <- readRDS(test_path("available_columns.Rds"))
pk_table <- readRDS(test_path("pk_table.Rds"))
# stub list of tables
stub(read_sql_table,"dbListTables",mock("TR_SEASON","VT_SEASON",cycle=TRUE))

test_that("read_sql_table throws an error when a table doesn't exist",{
  expect_error(read_sql_table("table_doesnt_exist"),"doesnt_exist doesn't exist")
})

test_that("read_sql_table complains if asked to select or pk columns that don't exist", {
  m_read = mock(available_columns, available_columns, pk_table, NULL,cycle=T)
  stub(read_sql_table,"read_sql",m_read)
  expect_error(read_sql_table("TR_SEASON", select="columnThatDoesntExist"), "select")
  expect_silent(read_sql_table("TR_SEASON", select="id"))
  expect_error(read_sql_table("TR_SEASON", primary_keys="columnThatDoesntExist"), "primary_keys")
  expect_silent(read_sql_table("TR_SEASON", primary_keys="id"))
})

test_that("read_sql_table works with database", {
  m_read = mock(available_columns, pk_table,NULL,cycle=T)
  stub(read_sql_table,"read_sql",m_read)
  read_sql_table("TR_SEASON",primary_keys="id",date_column = "last_update_dt")
  expect_equal(as.character(mock_args(m_read)[[2]][["query"]]),"select * from `dbo`.`TR_SEASON`")
})

test_that("read_sql_table reads a table from the database in `dbo` and `BI` schemas and caches it",{
  m_read = mock(available_columns, pk_table,NULL,cycle=T)
  stub(read_sql_table,"read_sql",m_read)

  read_sql_table("TR_SEASON")
  read_sql_table("VT_SEASON","BI")

  expect_equal(mock_args(m_read)[[3]][["name"]],"dbo.TR_SEASON")
  expect_equal(mock_args(m_read)[[6]][["name"]],"BI.VT_SEASON")
})

test_that("read_sql_table finds the primary keys and last_update_dt columns",{
  pk_table = data.table(TABLE_SCHEMA="dbo",TABLE_NAME="TR_SEASON",COLUMN_NAME="blah")

  m_read = mock(available_columns, pk_table,NULL,cycle=T)
  stub(read_sql_table,"read_sql",m_read)

  read_sql_table("TR_SEASON")

  expect_equal(mock_args(m_read)[[3]][["primary_keys"]],"blah")
  expect_equal(mock_args(m_read)[[3]][["date_column"]],"last_update_dt")

  available_columns = data.table(TABLE_SCHEMA="dbo",TABLE_NAME="TR_SEASON",COLUMN_NAME="blah")

  read_sql_table("TR_SEASON")

  expect_equal(mock_args(m_read)[[6]][["primary_keys"]],"blah")
  expect_equal(mock_args(m_read)[[6]][["date_column"]],NULL)
})

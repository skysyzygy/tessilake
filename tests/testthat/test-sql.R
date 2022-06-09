library(mockery)
library(checkmate)
library(dittodb)

dir.create(file.path(tempdir(), "shallow"))
dir.create(file.path(tempdir(), "deep"))
withr::defer({
  gc()
  sql_disconnect()
  unlink(file.path(tempdir(), "shallow"), recursive = T)
  unlink(file.path(tempdir(), "deep"), recursive = T)
})

# sql_connect -------------------------------------------------------------

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

test_that("sql_connect throws an error when it can't connect", {
  sql_disconnect()
  stub(sql_connect, "config::get", "not a database")
  expect_error(suppressMessages(sql_connect()), "DSN")
})


# read_sql ----------------------------------------------------------------
data <- data.table(x = 1:1000, y = runif(1000), last_update_dt = lubridate::now(tzone = "EST"))

test_that("read_sql preserves attributes", {
  setattr(data, "key", "value")
  stub(read_sql, "tbl", mock(data, cycle = T))

  read_sql("data_with_attr")
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "deep", "tessi")), data)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "shallow", "tessi")), data)

  read_sql("data_with_attr",freshness = 0)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "deep", "tessi")), data)
  expect_equal(collect(cache_read(digest::sha1("data_with_attr"), "shallow", "tessi")), data)
})

test_that("read_sql works with dplyr::tbl", {
  con <- DBI::dbConnect(RSQLite::SQLite(),":memory:")
  data <- dplyr::copy_to(con,data)
  stub(read_sql, "tbl", data)

  read_sql("data_with_tbl", "data_with_tbl")
  expect_equal(collect(cache_read("data_with_tbl", "deep", "tessi")), collect(data))
  expect_equal(collect(cache_read("data_with_tbl", "shallow", "tessi")), collect(data))

  read_sql("data_with_tbl",freshness=0)
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

test_that("read_sql_table throws an error when a table doesn't exist",{
  expect_error(read_sql_table("table_doesnt_exist"),"doesnt_exist doesn't exist")
  expect_error(read_sql_table("VT_SEASON"),"VT_SEASON doesn't exist")
})


test_that("read_sql_table reads a table from the database in `dbo` and `BI` schemas and caches it",{
  read_sql_table("TR_SEASON")
  read_sql_table("VT_SEASON","BI")

  expect_true(cache_exists("dbo.TR_SEASON","deep","tessi"))
  expect_true(cache_exists("dbo.TR_SEASON","shallow","tessi"))

  expect_true(cache_exists("BI.VT_SEASON","shallow","tessi"))
  expect_true(cache_exists("BI.VT_SEASON","shallow","tessi"))
})


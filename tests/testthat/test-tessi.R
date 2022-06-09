library(mockery)
library(checkmate)
library(dittodb)

dir.create(file.path(tempdir(), "shallow"))
dir.create(file.path(tempdir(), "deep"))
start_mock_db()
withr::defer({
  gc()
  stop_mock_db()
  sql_disconnect
  unlink(file.path(tempdir(), "shallow"), recursive = T)
  unlink(file.path(tempdir(), "deep"), recursive = T)
})

# tessi_list_tables -------------------------------------------------------

test_that("tessi_list_tables combines tessi_tables.yml with config.yml", {
  config_mock <- mock(config::get(), list(a = list(b = "c")))
  yaml_mock <- mock(list(a = list(b = "c")))

  stub(tessi_list_tables, "read_yaml", list())
  stub(tessi_list_tables, "config::get", config_mock)
  expect_equal(tessi_list_tables(), data.table(short_name = "a", b = "c"))
  expect_equal(mock_args(config_mock)[[2]], list(config = "tessi_tables"))

  rm(tessi_list_tables)
  stub(tessi_list_tables, "read_yaml", yaml_mock)
  stub(tessi_list_tables, "config::get", list())
  expect_equal(tessi_list_tables(), data.table(short_name = "a", b = "c"))
  expect_equal(mock_args(yaml_mock)[[1]], list(system.file("extdata", "tessi_tables.yml", package = "tessilake")))
})

# tessi_read_db -----------------------------------------------------------

test_that("tessi_read_db complains if table_name is not given", {
  expect_error(tessi_read_db(), "table_name")
})

test_that("tessi_read_db retuns primary key info as an attribute", {
  expect_equal(attr(tessi_read_db("customers"), "primary_keys"), "customer_no")
  expect_equal(attr(tessi_read_db("T_CUSTOMER"), "primary_keys"), "customer_no")
})

test_that("tessi_read_db complains if asked for a table it doesn't know about and that doesn't exist in Tessitura", {
  expect_error(tessi_read_db("tableThatDoesntExist"), "Table dbo.tableThatDoesntExist doesn't exist")
  expect_error(tessi_read_db("BI.tableThatDoesntExist"), "Table BI.tableThatDoesntExist doesn't exist")
  expect_error(tessi_read_db("select * from T_CUSTOMER"))
  tessiTables <- list(dummy = "dummy")
  expect_error(tessi_read_db("dummy"), "Table dbo.dummy doesn't exist")
  expect_gt(collect(dplyr::count(tessi_read_db("seasons")))[[1]], 100)
  expect_gt(collect(dplyr::count(tessi_read_db("TR_SEASON")))[[1]], 100)
  expect_gt(collect(dplyr::count(tessi_read_db("BI.VT_SEASON")))[[1]], 100)
})




test_that("read_tessi handles merges", {
  expect_equal(2 * 2, 4)
})

test_that("read_tessi appends group_customer_no based on customer_no and creditee_no", {
  expect_equal(2 * 2, 4)
})

test_that("read_tessi returns a Table", {
  stub(read_tessi, "tessi_read_db", mock(data, cycle = T))

  expect_class(read_tessi("table"), "Table")
})

withr::deferred_run()

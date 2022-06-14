withr::local_package("checkmate")
withr::local_package("mockery")
local_cache_dirs()

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

# read_tessi -----------------------------------------------------------

m = mock(NULL)
stub(read_tessi,"read_sql_table",m)
test_that("read_tessi complains if table_name is not given", {
  expect_error(read_tessi(), "table_name")
})

test_that("read_tessi complains if asked for a table it doesn't know about", {
  expect_error(read_tessi("tableThatDoesntExist"), "table_name")
  expect_silent(read_tessi("seasons", freshness = 0))
})

test_that("read_tessi passes all arguments on to read_sql_table", {
  expect_equal(mock_args(m)[[1]],list(table_name = "VT_SEASON", schema = "BI",
                                      primary_keys = "season_no",
                                      freshness = 0))
})

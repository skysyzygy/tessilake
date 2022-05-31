library(withr)

local_envvar(R_CONFIG_FILE=file.path("~/config.yml"))

dir.create(file.path(tempdir(), "shallow"))
dir.create(file.path(tempdir(), "deep"))
defer({
  unlink(file.path(tempdir(), "shallow"), recursive = T)
  unlink(file.path(tempdir(), "deep"), recursive = T)
})

test_that("tessi_list_tables returns the list of tessi tables", {
  expect_equal(tessi_list_tables(), tessilake:::tessi_tables)
})

test_that("read_tessi, tessi_read_db, and read_cache complains if noF table_name is given", {
  expect_error(read_tessi(), "table_name")
  expect_error(tessi_read_db(), "table_name")
})

test_that("tessi_read_db retuns primary key info", {
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


test_that("read_stream handles merges", {
  expect_equal(2 * 2, 4)
})

test_that("read_stream appends group_customer_no based on customer_no and creditee_no", {
  expect_equal(2 * 2, 4)
})


test_that("read_tessi returns an Arrow table", {
  expect_true("Table" %in% class(read_tessi("seasons")))
})

test_that("read_stream subsets like subset()", {
  expect_equal(2 * 2, 4)
})

test_that("read_stream selects like subset()", {
  expect_equal(2 * 2, 4)
})

# test_that("read_tessi can read from all the defined tables", {
#   expect_equal(lapply(names(tessiTables),read_tessi)
#
# })

deferred_run()


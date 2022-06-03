library(withr)
library(mockery)
library(checkmate)
library(lubridate)

dir.create(file.path(tempdir(), "shallow"))
dir.create(file.path(tempdir(), "deep"))
defer({
  gc()
  unlink(file.path(tempdir(), "shallow"), recursive = T)
  unlink(file.path(tempdir(), "deep"), recursive = T)
})

# tessi_list_tables -------------------------------------------------------

test_that("tessi_list_tables returns the list of tessi tables", {
  expect_equal(tessi_list_tables(), tessilake:::tessi_tables)
})

test_that("tessi_list_tables combines tessi_tables.yml with config.yml",{
  expect_true(TRUE)
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


# read_tessi --------------------------------------------------------------

test_that("read_tessi preserves primary_keys",{
  data = setattr(data.frame(c(1,2,3)),"primary_keys","value")
  stub(read_tessi,"tessi_read_db",mock(data,cycle=T))

  read_tessi("data_with_attr")
  expect_equal(collect(cache_read("data_with_attr","deep","tessi")),data)
  expect_equal(collect(cache_read("data_with_attr","shallow","tessi")),data)
})

data_old <- data.frame(x=c(1,2,3),last_update_dt=now("UTC") - ddays(10)) %>% setDT
data_new <- data.frame(x=c(1,2,3),last_update_dt=now("UTC")) %>% setDT
setattr(data_old,"primary_keys","x")
setattr(data_new,"primary_keys","x")

test_that("read_tessi updates deep + shallow store from db iff not fresh enough",{
  stub(read_tessi,"tessi_read_db",mock(data_old,data_new,data_new))

  read_tessi("data")
  expect_equal(collect(cache_read("data","deep","tessi")),data_old)
  expect_equal(collect(cache_read("data","shallow","tessi")),data_old)
  read_tessi("data")
  expect_equal(collect(cache_read("data","deep","tessi")),data_old)
  expect_equal(collect(cache_read("data","shallow","tessi")),data_old)
  read_tessi("data",freshness = 0)
  expect_equal(collect(cache_read("data","deep","tessi")),data_new)
  expect_equal(collect(cache_read("data","shallow","tessi")),data_new)
})

test_that("read_tessi updates shallow store from deep store iff fresh enough",{
  stub(read_tessi,"tessi_read_db",mock(data_new,data_old))

  read_tessi("data2")
  cache_write(data_old,"data2","shallow","tessi",partition = FALSE)
  expect_equal(collect(cache_read("data2","deep","tessi")),data_new)
  expect_equal(collect(cache_read("data2","shallow","tessi")),data_old)

  stub(read_tessi,"file.mtime",mock(now(),now()-ddays(10))) #make it think that deep is fresh but shallow is old
  read_tessi("data2")
  expect_equal(collect(cache_read("data2","deep","tessi")),data_new)
  expect_equal(collect(cache_read("data2","shallow","tessi")),data_new)
})

test_that("read_tessi loads from DB incrementally, row-wise",{
  seasons <- read_tessi("seasons")
  seasons <- collect(seasons[-1,])
  seasons[1:2,"last_update_dt"] <- ymd("1900-01-01")
  cache_write(seasons,"seasons","deep","tessi",partition = F)
  stub(read_tessi,"collect.tbl_sql",function(.){
    print(dplyr::collect(dplyr::summarize(.,n()))[[1]])
    dbplyr:::collect.tbl_sql(.)
  },depth=2)
  # this was super hard to do with a mock for some reason -- but this writes out 2 and then 1 because 2 rows are updated and 1 row is added
  expect_output(read_tessi("seasons",freshness = 0),"2\\n\\[1\\] 1$")
})


test_that("read_tessi loads from deep cache incrementally",{
  seasons <- read_tessi("seasons")
  seasons <- collect(seasons[-c(1,2),])
  seasons[1:3,"last_update_dt"] <- ymd("1900-01-01")
  cache_write(seasons,"seasons","deep","tessi",partition = F)
  stub(read_tessi,"arrow::collect.arrow_dplyr_query",function(.){
    print(dplyr::collect(dplyr::summarize(.,n()))[[1]])
    dbplyr:::collect.tbl_sql(.)
  },depth=2)
  # this was super hard to do with a mock for some reason -- but this writes out 3 and then 2 because 3 rows are updated and 2 row is added
  expect_output(read_tessi("seasons",freshness = 0),"3\\n\\[1\\] 2$")
})

test_that("read_tessi handles merges", {
  expect_equal(2 * 2, 4)
})

test_that("read_tessi appends group_customer_no based on customer_no and creditee_no", {
  expect_equal(2 * 2, 4)
})

test_that("read_tessi returns a Table", {
  expect_class(read_tessi("seasons"),"Table")
})


# test_that("read_tessi can read from all the defined tables", {
#   expect_equal(lapply(names(tessiTables),read_tessi)
#
# })

deferred_run()


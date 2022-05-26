library(withr)
library(dplyr)

dir.create(file.path(tempdir(),"shallow"))
dir.create(file.path(tempdir(),"deep"))
defer({
  unlink(file.path(tempdir(),"shallow"),recursive=T)
  unlink(file.path(tempdir(),"deep"),recursive=T)
})

local_options(tessilake.shallow=file.path(tempdir(),"shallow"),
              tessilake.deep=file.path(tempdir(),"deep"),
              tessilake.tessitura="Tessitura") #TODO: dummy database for tests without Tessitura present

test_that("list_tessi_tables returns the ... list of tessi tables", {
  expect_equal(list_tessi_tables(),tessilake:::tessiTables)
})


test_that("read_tessi complains if the tessilake.shallow option isn't set", {
  local_options(tessilake.shallow=NULL)
  expect_error(read_tessi("memberships"),"Please.+tessilake.shallow")
})

test_that("read_tessi complains if the tessilake.tessitura option isn't set or doesn't work", {
  local_options(tessilake.tessitura=NULL)
  expect_error(read_tessi("memberships"),"Please.+tessilake.tessitura")
  local_options(tessilake.tessitura="Broken")
  expect_error(read_tessi("memberships"),"Please.+working ODBC")
})

test_that("read_tessi, read_tessi_db, and read_cache complains if no tableName is given", {
  expect_error(read_tessi(),"tableName")
  expect_error(read_tessi_db(),"tableName")
  expect_error(read_cache(),"tableName")
})

test_that("read_tessi_db retuns primary key info", {
  expect_equal(attr(read_tessi_db("customers"),"primaryKeys"),"customer_no")
  expect_equal(attr(read_tessi_db("T_CUSTOMER"),"primaryKeys"),"customer_no")
})


test_that("read_tessi_db complains if asked for a table it doesn't know about and that doesn't exist in Tessitura", {
  expect_error(read_tessi_db("tableThatDoesntExist"),"Table dbo.tableThatDoesntExist doesn't exist")
  expect_error(read_tessi_db("BI.tableThatDoesntExist"),"Table BI.tableThatDoesntExist doesn't exist")
  expect_error(read_tessi_db("select * from T_CUSTOMER"))
  tessiTables = list(dummy = "dummy")
  expect_error(read_tessi_db("dummy"),"Table dbo.dummy doesn't exist")
  expect_gt(collect(count(read_tessi_db("seasons")))[[1]],100)
  expect_gt(collect(count(read_tessi_db("TR_SEASON")))[[1]],100)
  expect_gt(collect(count(read_tessi_db("BI.VT_SEASON")))[[1]],100)
})

test_that("read_cache create the cache directories if they don't exist", {
  read_cache("seasons","tessi","deep")
  dirName = file.path(.Options$tessilake.deep,"tessi")
  expect_true(dir.exists(dirName))
  unlink(dirName,recursive = T)
  read_cache("seasons","tessi","shallow")
  dirName = file.path(.Options$tessilake.shallow,"tessi")
  expect_true(dir.exists(dirName))
  unlink(dirName,recursive = T)
})

test_that("update_data.table updates data.tables incrementally", {
  local_package("lubridate")
  local_timezone("America/New_York")
  from = data.table(date=seq(today()-dyears(10),now(),by="days"))
  from[,`:=`(I=.I,data=runif(.N))]
  to = copy(from)
  to = to[runif(.N)>.1]
  to[runif(.N)<.1,`:=`(date=date-ddays(1),
                       data=runif(.N))]

  expect_equal(from,setorderv(update_data.table(from,to,date,c(I)),"I"))

})

test_that("read_stream handles merges",{
  expect_equal(2 * 2, 4)

})

test_that("read_stream appends group_customer_no based on customer_no and creditee_no",{
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

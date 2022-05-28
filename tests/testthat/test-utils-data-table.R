library(data.table)
library(withr)

# setleftjoin -------------------------------------------------------------

setleftjoin = function(...) { suppressMessages(tessilake::setleftjoin(...))}

l = data.table(a=seq(1:10),b=sample(1:100,10))
r = data.table(b=seq(1:100),c=sample(1:1000,100))
r1 = r

merge.data.table = function(...) {setkey(data.table::merge.data.table(...),NULL)}
expect_no_side_effects_on_r = function() {
  expect_true(rlang::is_reference(r,r1))
  expect_equal(r,r1)
}

test_that("setleftjoin merges in place", {
  expect_true(rlang::is_reference(setleftjoin(l,r),l))
  expect_no_side_effects_on_r()
})

l = data.table(a=seq(1:10),b=sample(1:100,10))
r = data.table(b=seq(1:100),c=sample(1:1000,100))
r1 = r

test_that("setleftjoin handles missing 'by' argument", {
  expect_mapequal(setleftjoin(copy(l),r),merge(l,r,all.x=T))
  expect_no_side_effects_on_r()
})

test_that("setleftjoin handles character 'by' argument", {
  expect_mapequal(setleftjoin(copy(l),r,by="b"),merge(l,r,by="b",all.x=T))
  expect_no_side_effects_on_r()
})

test_that("setleftjoin avoids column collision", {
  colnames(l) = c("b","c")
  expect_mapequal(setleftjoin(copy(l),r,by="b"),merge(l,r,by="b",all.x=T))
  expect_no_side_effects_on_r()
})

test_that("setleftjoin handles named vector 'by' argument", {
  colnames(l) = c("f","g")
  expect_mapequal(setleftjoin(copy(l),r,by=c("f"="b")),merge(l,r,by.x="f",by.y="b",all.x=T))
  expect_no_side_effects_on_r()
})

test_that("setleftjoin fails when there's more than one matching row in r", {
  r2 = r[c(1,1:.N)]
  expect_error(setleftjoin(copy(l),r2))
})

test_that("setleftjoin succeeds when there's more or less than one matching row in l", {
  l0 = l[-1,]
  l2 = l[c(1,1:.N)]

  expect_mapequal(setleftjoin(copy(l0),r),merge(l0,r,all.x=T))
  expect_mapequal(setleftjoin(copy(l2),r),merge(l2,r,all.x=T))
})

test_that("setleftjoin succeeds when there's NAs in l or r", {
  l1 = copy(l)[1,"b":=NA]
  r1 = copy(r)[1,"b":=NA]

  expect_mapequal(setleftjoin(copy(l1),r),merge(l1,r,all.x=T))
  expect_mapequal(setleftjoin(copy(l),r1),merge(l,r1,all.x=T))

})


# update_table ------------------------------------------------------------

test_that("update_table requires that from and to are given",{

})

test_that("update_table requires that primaryKeys and dateColumn refer to columns in from and to",{

})

test_that("update_table works with tidy-selected columns in primaryKeys and dateColumn",{

})

test_that("update_table simply copies from into to when dateColumn and primaryKeys are missing", {
  expect = data.table(expand.grid(x=1:100,y=1:100))[,data:=runif(.N)]
  # divide the data
  from = copy(expect)[1:9000]
  to = copy(expect)[1000:10000]
  # and mung it up
  to[1:5000,data:=runif(.N)]

  expect_equal(expect,update_table(from,to))
})

test_that("update_table updates data.tables incrementally when given primaryKeys", {
  expect = data.table(expand.grid(x=1:100,y=1:100))[,data:=runif(.N)]
  # divide the data
  from = copy(expect)[1:9000]
  to = copy(expect)[1000:10000]
  # and mung it up
  to[1:5000,data:=runif(.N)]

  expect_equal(expect,setorderv(update_table(from,to,primaryKeys=c("x","y")),c("y","x")))
  expect_equal(expect,setorderv(update_table(from,to,primaryKeys=c(x,y)),c("y","x")))
})

test_that("update_table complains when dateColumn is given but not primaryKeys", {
  local_package("lubridate")
  local_timezone("America/New_York")
  from = data.table(date=seq(today()-dyears(10),now(),by="days"))
  from[,`:=`(I=.I,data=runif(.N))]
  to = copy(from)
  to = to[runif(.N)>.1]
  to[runif(.N)<.1,`:=`(date=date-ddays(1),
                       data=runif(.N))]

  expect_error(from,update_table(from,to,dateColumn=date),"dateColumn")
})

test_that("update_table updates data.tables incrementally when given dateColumn and primaryKeys", {
  local_package("lubridate")
  local_timezone("America/New_York")
  from = data.table(date=seq(today()-dyears(10),now(),by="days"))
  from[,`:=`(I=.I,data=runif(.N))]
  to = copy(from)
  to = to[runif(.N)>.1]
  to[runif(.N)<.1,`:=`(date=date-ddays(1),
                       data=runif(.N))]

  expect_equal(from,setorderv(update_table(from,to,dateColumn=date,primaryKeys=c(I)),"I"))
})

test_that("update_table doesn't copy from when from is a data.table",{
  expect = data.table(expand.grid(x=1:100,y=1:100))[,data:=runif(.N)]
  # divide the data
  from = copy(expect)[1:9000]
  to = copy(expect)[1000:10000]
  # and mung it up
  to[1:5000,data:=runif(.N)]

  tracemem(from)

  expect_silent(update_table(from,to))
  expect_silent(update_table(from,to,primaryKeys=c(x,y)))
})

test_that("update_table doesn't copy to when to is a data.table",{
  expect = data.table(expand.grid(x=1:100,y=1:100))[,data:=runif(.N)]
  # divide the data
  from = copy(expect)[1:9000]
  to = copy(expect)[1001:10000]
  # and mung it up
  to[1:5000,data:=runif(.N)]

  tracemem(to)

  expect_silent(update_table(from,to))
  expect_silent(update_table(from,to,primaryKeys=c(x,y)))
})

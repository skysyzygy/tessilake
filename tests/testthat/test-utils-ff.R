
library(ffbase)
library(data.table)

# as.ram2 -----------------------------------------------------------------


test_that("as.ram2 returns ram objects", {
  test.ff = ff(1:5)
  expect_equal(as.ram2(test.ff),1:5)

  test.ffdf = as.ffdf(data.frame(a=1:5,b=6:10))
  expect_equal(as.ram2(test.ffdf),data.table(a=1:5,b=6:10))

  test.ffdf = as.ffdf(data.frame(a=1:5,b=6:10))
  expect_true(is.data.table(as.ram2(test.ffdf)))
})

test_that("as.ram2 leaves nothing behind", {
  test.ff = ff(1:5)
  as.ram2(test.ff)
  expect_equal(file.exists(filename(test.ff)),FALSE)

  test.ffdf = as.ffdf(data.frame(a=1:5,b=6:10))
  as.ram2(test.ffdf)
  expect_mapequal(sapply(filename(test.ffdf),file.exists),c(a=FALSE,b=FALSE))
})

# fix_vmode ---------------------------------------------------------------

test_that("fix_vmode throws an error when given an ff",{
  test.ff = ff(1:5)
  expect_error(fix_vmode(test.ff))
})

test_that("fix_vmode converts character to factor and leaves factors alone",{
  vec.char = c("a","b")
  expect_equal(fix_vmode(vec.char),as.factor(vec.char))
  vec.fact = as.factor(vec.char)
  expect_equal(fix_vmode(vec.fact),vec.fact)
})

vec.bool = c(0,2^0)
vec.quad = c(0,2^(0:1))
vec.nibble = c(0,2^(0:3))
vec.ubyte = c(0,2^(0:7))
vec.ushort = c(0,2^(0:15))
vec.integer = c(0,2^(0:31))
vec.double = c(0,2^(0:63))

test_that("fix_vmode sets vmode on integer values based on the number of bits required",{
  expect_equal(vmode(fix_vmode(vec.bool)),"boolean")
  expect_equal(vmode(fix_vmode(vec.quad)),"quad")
  expect_equal(vmode(fix_vmode(vec.nibble)),"nibble")
  expect_equal(vmode(fix_vmode(vec.ubyte)),"ubyte")
  expect_equal(vmode(fix_vmode(vec.ushort)),"ushort")
  expect_equal(vmode(fix_vmode(vec.double)),"double")
})

test_that("fix_vmode upgrades vmode on integer values when there are NA or signed values",{
  expect_equal(vmode(fix_vmode(c(NA,vec.bool))),"logical")
  expect_equal(vmode(fix_vmode(c(NA,vec.quad))),"byte")
  expect_equal(vmode(fix_vmode(c(NA,vec.nibble))),"byte")
  expect_equal(vmode(fix_vmode(c(NA,vec.ubyte))),"short")
  expect_equal(vmode(fix_vmode(c(NA,vec.ushort))),"integer")
  expect_equal(vmode(fix_vmode(c(NA,vec.double))),"double")
  expect_equal(vmode(fix_vmode(c(-1,vec.bool))),"byte")
  expect_equal(vmode(fix_vmode(c(-1,vec.quad))),"byte")
  expect_equal(vmode(fix_vmode(c(-1,vec.nibble))),"byte")
  expect_equal(vmode(fix_vmode(c(-1,vec.ubyte))),"short")
  expect_equal(vmode(fix_vmode(c(-1,vec.ushort))),"integer")
  expect_equal(vmode(fix_vmode(c(-1,vec.double))),"double")
})

test_that("fix_vmode doesn't destroy any values when passing through ff",{
  expect_equal(as.ram2(ff(fix_vmode(vec.bool)))+0,vec.bool)
  expect_equal(as.ram2(ff(fix_vmode(vec.quad)))+0,vec.quad)
  expect_equal(as.ram2(ff(fix_vmode(vec.nibble)))+0,vec.nibble)
  expect_equal(as.ram2(ff(fix_vmode(vec.ubyte)))+0,vec.ubyte)
  expect_equal(as.ram2(ff(fix_vmode(vec.ushort)))+0,vec.ushort)
  expect_equal(as.ram2(ff(fix_vmode(vec.double)))+0,vec.double)

  expect_equal(as.ram2(ff(fix_vmode(c(NA,vec.bool))))+0,c(NA,vec.bool))
  expect_equal(as.ram2(ff(fix_vmode(c(NA,vec.quad))))+0,c(NA,vec.quad))
  expect_equal(as.ram2(ff(fix_vmode(c(NA,vec.nibble))))+0,c(NA,vec.nibble))
  expect_equal(as.ram2(ff(fix_vmode(c(NA,vec.ubyte))))+0,c(NA,vec.ubyte))
  expect_equal(as.ram2(ff(fix_vmode(c(NA,vec.ushort))))+0,c(NA,vec.ushort))
  expect_equal(as.ram2(ff(fix_vmode(c(NA,vec.double))))+0,c(NA,vec.double))

  expect_equal(as.ram2(ff(fix_vmode(c(-1,vec.bool))))+0,c(-1,vec.bool))
  expect_equal(as.ram2(ff(fix_vmode(c(-1,vec.quad))))+0,c(-1,vec.quad))
  expect_equal(as.ram2(ff(fix_vmode(c(-1,vec.nibble))))+0,c(-1,vec.nibble))
  expect_equal(as.ram2(ff(fix_vmode(c(-1,vec.ubyte))))+0,c(-1,vec.ubyte))
  expect_equal(as.ram2(ff(fix_vmode(c(-1,vec.ushort))))+0,c(-1,vec.ushort))
  expect_equal(as.ram2(ff(fix_vmode(c(-1,vec.double))))+0,c(-1,vec.double))
})

as.Date.numeric = function(x,...) {base::as.Date.numeric(x,origin=as.Date("1970-01-01"))}
test_that("fix_vmode doesn't destroy dates when passing through ff", {
  expect_equal(as.ram2(ff(fix_vmode(as.Date(vec.bool)))),as.Date(vec.bool))
  expect_equal(as.ram2(ff(fix_vmode(as.Date(vec.quad)))),as.Date(vec.quad))
  expect_equal(as.ram2(ff(fix_vmode(as.Date(vec.nibble)))),as.Date(vec.nibble))
  expect_equal(as.ram2(ff(fix_vmode(as.Date(vec.ubyte)))),as.Date(vec.ubyte))
  expect_equal(as.ram2(ff(fix_vmode(as.Date(vec.ushort)))),as.Date(vec.ushort))
  expect_equal(as.ram2(ff(fix_vmode(as.Date(vec.double)))),as.Date(vec.double))
})

as.POSIXct.numeric = function(x,...) {base::as.POSIXct.numeric(x,origin=as.POSIXct("1970-01-01 00:00:00"))}
test_that("fix_vmode doesn't destroy POSIXct when passing through ff", {
  expect_equal(as.ram2(ff(fix_vmode(as.POSIXct(vec.bool+.1)))),as.POSIXct(vec.bool+.1))
  expect_equal(as.ram2(ff(fix_vmode(as.POSIXct(vec.quad+.1)))),as.POSIXct(vec.quad+.1))
  expect_equal(as.ram2(ff(fix_vmode(as.POSIXct(vec.nibble+.1)))),as.POSIXct(vec.nibble+.1))
  expect_equal(as.ram2(ff(fix_vmode(as.POSIXct(vec.ubyte+.1)))),as.POSIXct(vec.ubyte+.1))
  expect_equal(as.ram2(ff(fix_vmode(as.POSIXct(vec.ushort+.1)))),as.POSIXct(vec.ushort+.1))
  expect_equal(as.ram2(ff(fix_vmode(as.POSIXct(vec.double+.1)))),as.POSIXct(vec.double+.1))
})

test_that("fix_vmode doesn't destroy doubles when passing through ff", {
  expect_equal(as.ram2(ff(fix_vmode(vec.bool+.1))),(vec.bool+.1))
  expect_equal(as.ram2(ff(fix_vmode(vec.quad+.1))),(vec.quad+.1))
  expect_equal(as.ram2(ff(fix_vmode(vec.nibble+.1))),(vec.nibble+.1))
  expect_equal(as.ram2(ff(fix_vmode(vec.ubyte+.1))),(vec.ubyte+.1))
  expect_equal(as.ram2(ff(fix_vmode(vec.ushort+.1))),(vec.ushort+.1))
  expect_equal(as.ram2(ff(fix_vmode(vec.double+.1))),(vec.double+.1))
})

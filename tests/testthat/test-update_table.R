
# expr_get_names ----------------------------------------------------------

test_that("expr_get_names throws error with anything that doesn't work",{
  expect_error(expr_get_names(expr(mean(x))),"list or vector")
  expect_error(expr_get_names(1),"language")
})

test_that("expr_get_names works with strings and symbols", {
  expect_equal(expr_get_names(expr("a")),expr_get_names(expr(a)))
})

test_that("expr_get_names works with vectors and lists",{
  expect_equal(expr_get_names(expr(c(a,b="c"))),c("a",b="c"))
  expect_equal(expr_get_names(expr(list("a",b=c))),c("a",b="c"))
})

# update_table ------------------------------------------------------------

test_that("update_table requires that from and to are given", {
  from <- data.table(x = c(1, 2, 3))
  to <- data.table(y = c(1, 2, 3))
  expect_error(update_table(), "from")
  expect_error(update_table(from), "to")
})

test_that("update_table requires that primary_keys and date_column refer to columns in from and to", {
  from <- data.table(x = c(1, 2, 3))
  to <- data.table(y = c(1, 2, 3))
  expect_error(update_table(from, to, primary_keys = x), "colnames")
  expect_error(update_table(from, to, date_column = y), "colnames")
})

test_that("update_table works with tidy-selected columns in primary_keys and date_column", {
  from <- data.table(x = c(1, 2, 3), y = c(4, 5, 6))
  to <- copy(from)
  expect_equal(update_table(from, to, primary_keys = x), from)
  expect_equal(update_table(from, to, primary_keys = c(x)), from)
  expect_equal(update_table(from, to, primary_keys = "x"), from)
  expect_equal(update_table(from, to, primary_keys = c("x")), from)
  expect_equal(update_table(from, to, primary_keys = c(x, y)), from)
  expect_equal(update_table(from, to, primary_keys = c("x", "y")), from)
  expect_equal(update_table(from, to, primary_keys = c(x, "y")), from)
  expect_equal(update_table(from, to, primary_keys = c("x", y)), from)
})

test_that("update_table simply copies from into to when date_column and primary_keys are missing", {
  expect <- data.table(expand.grid(x = 1:100, y = 1:100))[, data := runif(.N)]
  # divide the data
  from <- copy(expect)[1:9000]
  to <- copy(expect)[1000:10000]
  # and mung it up
  to[1:5000, data := runif(.N)]

  expect_equal(from, update_table(from, to))
})

test_that("update_table updates data.tables incrementally when given primary_keys", {
  expect <- data.table(expand.grid(x = 1:100, y = 1:100))[, data := runif(.N)]
  # divide the data
  from <- copy(expect)[1:9000]
  to <- copy(expect)[1000:10000]
  # and mung it up
  to[1:5000, data := runif(.N)]

  expect_equal(expect, setorderv(update_table(from, to, primary_keys = c("x", "y")), c("y", "x")))
  expect_equal(expect, setorderv(update_table(from, to, primary_keys = c(x, y)), c("y", "x")))
})

test_that("update_table updates data.tables incrementally when given date_column and primary_keys", {
  withr::local_package("lubridate")
  withr::local_timezone("America/New_York")
  from <- data.table(date = seq(today() - dyears(10), now(), by = "days"))
  from[, `:=`(I = .I, data = runif(.N))]
  to <- copy(from)
  to <- to[runif(.N) > .1]
  to[runif(.N) < .1, `:=`(
    date = date - ddays(1),
    data = runif(.N)
  )]

  expect_equal(from, setorderv(update_table(from, to, date_column = date, primary_keys = c(I)), "I"))
})

test_that("update_table doesn't copy from when from is a data.table", {
  expect <- data.table(expand.grid(x = 1:100, y = 1:100))[, data := runif(.N)]
  # divide the data
  from <- copy(expect)[1:9000]
  to <- copy(expect)[1000:10000]
  # and mung it up
  to[1:5000, data := runif(.N)]

  tracemem(from)

  expect_silent(update_table(from, to))
  expect_silent(update_table(from, to, primary_keys = c(x, y)))
})

test_that("update_table doesn't copy to when to is a data.table", {
  expect <- data.table(expand.grid(x = 1:100, y = 1:100))[, data := runif(.N)]
  # divide the data
  from <- copy(expect)[1:9000]
  to <- copy(expect)[1001:10000]
  # and mung it up
  to[1:5000, data := runif(.N)]

  tracemem(to)

  expect_silent(update_table(from, to))
  expect_silent(update_table(from, to, primary_keys = c(x, y)))
})

withr::local_package("checkmate")

# cache_make_partition -----------------------------------------------------

test_that("cache_make_partitioning requires a data.frame with a valid primary key", {
  x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
  expect_error(cache_make_partitioning(), "x")
  expect_error(cache_make_partitioning(x), "primary_key")
  expect_error(cache_make_partitioning(x, primary_keys = "c"), "names")
  y <- rbind(x, NA)
  expect_error(cache_make_partitioning(y, primary_keys = "a"), "missing")
})

test_that("cache_make_partitioning suggests partitioning information for numeric primary keys", {
  x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
  expect_class(cache_make_partitioning(x, "a"), "call")
  expect_equal(cache_make_partitioning(x, "a"), rlang::expr(floor(a / 10000)))
  x$a <- x$a * 2
  expect_equal(cache_make_partitioning(x, "a"), rlang::expr(floor(a / 20000)))
  x$a <- x$a * .01
  expect_equal(cache_make_partitioning(x, "a"), rlang::expr(floor(a / 200)))
})

test_that("cache_make_partitioning suggests partitioning information for character primary keys", {
  x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
  expect_class(cache_make_partitioning(x, "b"), "call")
  expect_equal(cache_make_partitioning(x, "b"), rlang::expr(substr(tolower(b), 1, 2)))
})

test_that("cache_make_partitioning doesn't copy x", {
  x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
  tracemem(x)
  expect_silent(cache_make_partitioning(x, "b"))
})

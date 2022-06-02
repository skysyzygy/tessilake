library(checkmate)

dir.create(file.path(tempdir(), "deep"))
dir.create(file.path(tempdir(), "shallow"))
withr::defer({
  unlink(file.path(tempdir(), "deep"), recursive = T)
  unlink(file.path(tempdir(), "shallow"), recursive = T)
})

# cache_make_partition -----------------------------------------------------

test_that("cache_make_partitioning requires a data.frame with a valid primary key", {
  x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
  expect_error(cache_make_partitioning(), "x")
  expect_error(cache_make_partitioning(x), "primary_key")
  expect_error(cache_make_partitioning(x, primary_keys = "c"), "colnames")
  y <- rbind(x, NA)
  expect_error(cache_make_partitioning(y, primary_keys = "a"), "missing")
  y <- rbind(x, list(a = 3, b = "c"))
  expect_error(cache_make_partitioning(y, primary_keys = "a"), "duplicated")
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

# cache_get_attributes -----------------------------------------------------

test_that("cache_get_attributes returns the attributes of the original object minus names and row.names", {
  x <- data.frame()
  attributes <- attributes(x)
  attributes[c("names","row.names")] <- NULL
  cache_write(x, "x", "deep", "tessi")
  dataset <- cache_read("x", "deep", "tessi")
  expect_class(cache_get_attributes(dataset), "list")
  expect_equal(cache_get_attributes(dataset), attributes)
})

test_that("cache_get_attributes reads partitioning information from Arrow Dataset", {
  x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
  setattr(x, "primary_keys", "a")
  cache_write(x, "x", "deep", "tessi")
  dataset <- cache_read("x", "deep", "tessi")
  expect_class(cache_get_attributes(dataset), "list")
  expect_class(cache_get_attributes(dataset)$primary_keys, "character")
  expect_class(cache_get_attributes(dataset)$partitioning, "character")
  expect_equal(cache_get_attributes(dataset)$partitioning, rlang::expr_deparse(cache_make_partitioning(x)))
})

withr::deferred_run()

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
  expect_error(cache_make_partitioning(x, primary_keys = "c"), "names")
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

test_that("cache_get_attributes returns attributes of a data.frame", {
  x <- data.frame(c(1, 2, 3))
  setattr(x, "a", "test")
  attributes <- attributes(x)
  expect_class(cache_get_attributes(x), "list")
  expect_equal(cache_get_attributes(x), attributes)
})


test_that("cache_get_attributes returns attributes of an Arrow Table (except rownames)", {
  x <- data.frame(c(1, 2, 3))
  setattr(x, "a", "test")
  expect_class(cache_get_attributes(arrow_table(x)), "list")
  expect_mapequal(cache_get_attributes(arrow_table(x)), attributes(x))
})


test_that("cache_get_attributes reads partitioning information from Arrow Dataset", {
  x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
  setattr(x, "a", "test")
  filename <- tempfile()
  write_dataset(x, filename)
  dataset <- open_dataset(filename)
  expect_class(cache_get_attributes(dataset), "list")
  expect_mapequal(cache_get_attributes(dataset), attributes(x))
})


# cache_set_attributes ----------------------------------------------------

test_that("cache_set_attributes updates attributes of a data.frame", {
  x <- data.frame(c(1, 2, 3))
  y <- copy(x)
  cache_set_attributes(x, list(names = "test", b = "another"))
  setattributes(y, list(names = "test", b = "another"))
  expect_mapequal(attributes(x), attributes(y))
})

test_that("cache_set_attributes updates attributes of an Arrow Table", {
  x <- data.frame(c(1, 2, 3))
  y <- copy(x)
  x <- arrow_table(x)
  cache_set_attributes(x, list(names = "test", b = "another"))
  setattributes(y, list(names = "test", b = "another"))
  expect_mapequal(cache_get_attributes(x), attributes(y))
})

# cache_get_mtime ---------------------------------------------------------
test_read_write <- setattr(data.table(x = 1:100000, y = runif(1000)), "primary_keys", "x")

test_that("cache_get_mtime works with cache_write", {
  timeA <- Sys.time()
  cache_write(test_read_write, "test_read_write", "deep", "tessi", partition = FALSE)
  cache_write(test_read_write, "test_read_write", "shallow", "tessi", partition = FALSE)
  cache_write(test_read_write, "test_partitioning", "deep", "tessi")
  cache_write(test_read_write, "test_partitioning", "shallow", "tessi")
  timeB <- Sys.time()

  expect_gt(cache_get_mtime("test_read_write", "deep", "tessi"), timeA)
  expect_gt(cache_get_mtime("test_read_write", "shallow", "tessi"), timeA)
  expect_gt(cache_get_mtime("test_partitioning", "deep", "tessi"), timeA)
  expect_gt(cache_get_mtime("test_partitioning", "shallow", "tessi"), timeA)

  expect_lt(cache_get_mtime("test_read_write", "deep", "tessi"), timeB)
  expect_lt(cache_get_mtime("test_read_write", "shallow", "tessi"), timeB)
  expect_lt(cache_get_mtime("test_partitioning", "deep", "tessi"), timeB)
  expect_lt(cache_get_mtime("test_partitioning", "shallow", "tessi"), timeB)
})

withr::deferred_run()

withr::local_package("checkmate")
local_cache_dirs()

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

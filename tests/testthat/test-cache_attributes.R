withr::local_package("checkmate")
local_cache_dirs()

# cache_get_attributes -----------------------------------------------------

attributes <- function(x) {
  a <- base::attributes(x)
  a[which(!names(a) %in% c("names", "row.names"))]
}
test_that("cache_get_attributes returns attributes of a data.frame", {
  x <- data.frame(c(1, 2, 3))
  setattr(x, "a", "test")
  attributes <- attributes(x)
  expect_class(cache_get_attributes(x), "list")
  expect_equal(cache_get_attributes(x), attributes)
})


test_that("cache_get_attributes returns attributes of an Arrow Table (except rownames and class)", {
  x <- data.frame(c(1, 2, 3))
  setattr(x, "a", "test")
  expect_class(cache_get_attributes(arrow_table(x)), "list")
  expect_mapequal(cache_get_attributes(arrow_table(x)), attributes(x) %>% .[which(names(.) != "class")])
})

test_that("cache_get_attributes returns attributes of an arrow_dplyr_query", {
  x <- data.frame(y = c(1, 2, 3), z = c(4, 5, 6))
  setattr(x, "a", "test")
  query1 <- filter(arrow_table(x), y < 3)
  query2 <- left_join(query1, query1)
  expect_class(cache_get_attributes(query1), "list")
  expect_class(cache_get_attributes(query2), "list")
  expect_mapequal(cache_get_attributes(query1), attributes(x) %>% .[which(names(.) != "class")])
  expect_mapequal(cache_get_attributes(query2), attributes(x) %>% .[which(names(.) != "class")])
})


test_that("cache_get_attributes reads partitioning information from Arrow Dataset", {
  x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
  setattr(x, "a", "test")
  filename <- tempfile()
  write_dataset(x, filename)
  dataset <- open_dataset(filename)
  expect_class(cache_get_attributes(dataset), "list")
  expect_mapequal(cache_get_attributes(dataset), attributes(x) %>% .[which(names(.) != "class")])
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
  x <- arrow_table(x = c(1,2,3))
  cache_set_attributes(x, list(names = "test", b = "another"))
  expect_mapequal(cache_get_attributes(x), list(b = "another"))

  x <- data.frame(c(1, 2, 3))
  y <- copy(x)
  x <- arrow_table(x)
  cache_set_attributes(x, list(names = "test", b = "another"))
  setattributes(y, list(names = "test", b = "another"))
  expect_mapequal(cache_get_attributes(x), attributes(y) %>% .[which(names(.) != "class")])
})

test_that("cache_get_attributes updates attributes of an arrow_dplyr_query", {
  x <- data.frame(y = c(1, 2, 3), z = c(4, 5, 6))
  y <- copy(x)
  query1 <- filter(arrow_table(x), y < 3)
  query2 <- left_join(query1, query1)
  cache_set_attributes(query1, list(names = "test", b = "another"))
  cache_set_attributes(query2, list(names = "test", b = "another"))
  setattributes(y, list(names = "test", b = "another"))
  expect_mapequal(cache_get_attributes(query1), attributes(y) %>% .[which(names(.) != "class")])
  expect_mapequal(cache_get_attributes(query2), attributes(y) %>% .[which(names(.) != "class")])
})

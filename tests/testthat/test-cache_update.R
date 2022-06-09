library(checkmate)

dir.create(file.path(tempdir(), "deep"))
dir.create(file.path(tempdir(), "shallow"))
withr::defer({
  gc()
  unlink(file.path(tempdir(), "deep"), recursive = T)
  unlink(file.path(tempdir(), "shallow"), recursive = T)
})

test_read_write <- setattr(data.table(x = 1:100000, y = runif(1000)), "key", "value")

# cache_update -------------------------------------------------------------

test_new <- data.table(x = 1:100000, y = runif(1000))

test_that("cache_update writes the whole table when there's nothing there to begin with", {
  cache_update(test_new, "test_new", "deep", "tessi")
  expect_equal(collect(cache_read("test_new", "deep", "tessi")), test_new)
})

test_that("cache_update updates the whole table when there is no partitioning", {
  cache_update(test_read_write, "test_new2", "deep", "tessi")
  expect_equal(collect(cache_read("test_new2", "deep", "tessi")), test_read_write)
})

test_that("cache_update updates the whole table when there is no partitioning and doesn't copy from", {
  tracemem(test_new)
  expect_silent(cache_update(test_new, "test_new3", "deep", "tessi"))
  untracemem(test_new)
})

test_that("cache_update refuses to update a partitioned table with one with different primary keys", {
  test_partitioning <- copy(test_read_write)
  data.table::setattr(test_partitioning, "primary_keys", "x")
  cache_write(test_partitioning, "test_partitioning", "deep", "tessi")
  expect_error(cache_update(test_read_write, "test_partitioning", "deep", "tessi"), "primary keys")
  setattr(test_read_write, "primary_keys", "y")
  expect_error(cache_update(test_read_write, "test_partitioning", "deep", "tessi"), "primary keys")
})

test_incremental <- data.table(x = 1:100000, y = runif(1000))
update_incremental <- data.table(x = 1000:1999, y = runif(1000))
test_that("cache_update updates rows incrementally, and only in the required partitions", {
  cache_write(test_incremental, "test_incremental", "deep", "tessi", primary_keys = "x")
  time <- Sys.time()
  cache_update(update_incremental, "test_incremental", "deep", "tessi", primary_keys = "x")

  updated_cache <- collect(cache_read("test_incremental", "deep", "tessi")) %>%
    setorderv("x") %>%
    setattr("partitioning", NULL) %>%
    setattr("primary_keys", NULL)
  updated_table <- update_table(update_incremental, test_incremental, primary_keys = c("x"))

  expect_equal(updated_cache, updated_table)

  cache_files <- sapply(dir(cache_path("test_incremental", "deep", "tessi"), recursive = T, full.names = T), file.mtime)
  updated_cache_files <- purrr::keep(cache_files, ~ . > time)
  expect_equal(length(updated_cache_files), 1)
  expect_equal(length(cache_files) - length(updated_cache_files), 10)
})

test_that("cache_update updates rows incrementally and doesn't copy from", {
  tracemem(update_incremental)
  cache_write(test_incremental, "test_incremental2", "deep", "tessi", primary_keys = "x")
  expect_silent(cache_update(update_incremental, "test_incremental2", "deep", "tessi", primary_keys = "x"))
  untracemem(update_incremental)
})

test_that("cache_update returns nothing, invisibly", {
  cache_write(test_incremental, "test_incremental3", "deep", "tessi")
  cache_update(update_incremental, "test_incremental3", "deep", "tessi", primary_keys = "x")

  expect_equal(cache_update(update_incremental, "test_incremental3", "deep", "tessi", primary_keys = "x"), NULL)
  expect_invisible(cache_update(update_incremental, "test_incremental3", "deep", "tessi", primary_keys = "x"))
})


withr::deferred_run()

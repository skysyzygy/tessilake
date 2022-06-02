library(checkmate)

dir.create(file.path(tempdir(), "deep"))
dir.create(file.path(tempdir(), "shallow"))
withr::defer({
  gc()
  unlink(file.path(tempdir(), "deep"), recursive = T)
  unlink(file.path(tempdir(), "shallow"), recursive = T)
})

# cache_path ------------------------------------------------------------

test_that("cache_path complains if arguments incorrect", {
  expect_error(cache_path(), "table_name")
  expect_error(cache_path("seasons", "deep", "blah"), "type")
  expect_error(cache_path("seasons", "blah", "tessi"), "depth")
})

test_that("cache_path complains if the root directory doesn't exist or isn't writeable", {
  cache_root <- config::get(paste0("tessilake.", "shallow"))
  mockery::stub(cache_path, "config::get", file.path(tempdir(), "doesntexist"))
  expect_error(cache_path("seasons", "shallow", "stream"), "cache path")
})

# cache_write -------------------------------------------------------------

test_read_write <- setattr(data.table(x = 1:100000, y = runif(1000)),"key","value")

test_that("cache_write with partitioning=FALSE creates parquet and feather files", {
  cache_write(test_read_write, "test_read_write", "deep", "tessi", partition = FALSE)
  expect_true(file.exists(paste0(cache_path("test_read_write", "deep", "tessi"),".parquet")))
  cache_write(test_read_write, "test_read_write", "shallow", "tessi", partition = FALSE)
  expect_true(file.exists(paste0(cache_path("test_read_write", "shallow", "tessi"),".feather")))
})

test_that("cache_write with partitioning=TRUE creates datasets", {
  test_partitioning = copy(test_read_write)
  data.table::setattr(test_partitioning, "primary_keys", "x")
  cache_write(test_partitioning, "test_partitioning", "deep", "tessi")
  expect_equal(length(dir(cache_path("test_partitioning", "deep", "tessi"))), 11)
  cache_write(test_partitioning, "test_partitioning", "shallow", "tessi")
  expect_equal(length(dir(cache_path("test_partitioning", "shallow", "tessi"))), 11)
})

test_that("cache_write doesn't copy x", {
  tracemem(test_read_write)
  expect_silent(cache_write(test_read_write, "test_copy", "deep", "tessi"))
  untracemem(test_read_write)
})

test_that("cache_write returns nothing, invisibly", {
  expect_invisible(cache_write(test_read_write, "test_silent", "deep", "tessi"))
  expect_equal(cache_write(test_read_write, "test_silent", "deep", "tessi"), NULL)
})

# cache_read --------------------------------------------------------------

test_that("cache_read returns FALSE for non-existent caches", {
  expect_false(cache_read("blah", "deep", "tessi"))
})

test_that("cache_read returns data to the original form including attributes", {
  expect_equal(cache_read("test_read_write", "deep", "tessi") %>% collect, test_read_write)
  expect_equal(cache_read("test_read_write", "shallow", "tessi") %>% collect, test_read_write)
  expect_equal(cache_read("test_partitioning", "deep", "tessi") %>% collect %>% setorderv("x") %>%
                 setattr("partitioning",NULL) %>% setattr("primary_keys",NULL), test_read_write)
  expect_equal(cache_read("test_partitioning", "shallow", "tessi") %>% collect %>% setorderv("x") %>%
                 setattr("partitioning",NULL) %>% setattr("primary_keys",NULL), test_read_write)
})

# cache_update -------------------------------------------------------------

test_new <- data.table(x = 1:100000, y = runif(1000))

test_that("cache_update writes the whole table when there's nothing there to begin with", {
  cache_update(test_new, "test_new", "deep", "tessi")
  expect_equal(collect(cache_read("test_new", "deep", "tessi")), test_new)
})

test_that("cache_update updates the whole table when there is no partitioning", {
  cache_update(test_read_write, "test_new", "deep", "tessi")
  expect_equal(collect(cache_read("test_new", "deep", "tessi")), test_read_write)
})

test_that("cache_update updates the whole table when there is no partitioning and doesn't copy from", {
  tracemem(test_new)
  expect_silent(cache_update(test_new, "test_new", "deep", "tessi"))
  untracemem(test_new)
})

test_that("cache_update refuses to update a partitioned table with one with different primary keys", {
  expect_error(cache_update(test_read_write, "test_partitioning", "deep", "tessi"), "primary keys")
  setattr(test_read_write,"primary_keys","y")
  expect_error(cache_update(test_read_write, "test_partitioning", "deep", "tessi"), "primary keys")
})

test_incremental <- data.table(x = 1:100000, y = runif(1000))
update_incremental <- data.table(x = 1000:1999, y = runif(1000))
test_that("cache_update updates rows incrementally, and only in the required partitions", {
  cache_write(test_incremental, "test_incremental", "deep", "tessi", primary_keys = "x")
  time <- Sys.time()
  cache_update(update_incremental, "test_incremental", "deep", "tessi", primary_keys = "x")

  updated_cache <- setorderv(collect(cache_read("test_incremental", "deep", "tessi")), "x") %>%
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
  cache_write(test_incremental, "test_incremental", "deep", "tessi", primary_keys = "x")
  expect_silent(cache_update(update_incremental, "test_incremental", "deep", "tessi", primary_keys = "x"))
  untracemem(update_incremental)
})

test_that("cache_update returns nothing, invisibly", {
  cache_write(test_incremental, "test_incremental", "deep", "tessi")
  cache_update(update_incremental, "test_incremental", "deep", "tessi", primary_keys = "x")

  expect_equal(cache_update(update_incremental, "test_incremental", "deep", "tessi", primary_keys = "x"), NULL)
  expect_invisible(cache_update(update_incremental, "test_incremental", "deep", "tessi", primary_keys = "x"))
})

withr::deferred_run()

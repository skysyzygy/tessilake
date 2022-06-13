withr::local_package("checkmate")
local_cache_dirs()

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

# cache_path ------------------------------------------------------------

test_that("cache_path complains if arguments incorrect", {
  expect_error(cache_path(), "table_name")
  expect_error(cache_path("seasons", "deep", "blah"), "type")
  expect_error(cache_path("seasons", "blah", "tessi"), "depth")
})

test_that("cache_path complains if the root directory doesn't exist or isn't writeable", {
  mockery::stub(cache_path, "config::get", file.path(tempdir(), "doesntexist"))
  expect_error(cache_path("seasons", "shallow", "stream"), "cache path")
})

test_that("cache_path returns a path", {
  cache_root <- config::get(paste0("tessilake.", "shallow"))
  expect_equal(cache_path("seasons", "shallow", "stream"), file.path(cache_root,"stream","seasons"))
})

# cache_exists ------------------------------------------------------------

test_that("cache_exists complains if arguments incorrect", {
  expect_error(cache_exists(), "table_name")
  expect_error(cache_exists("seasons", "deep", "blah"), "type")
  expect_error(cache_exists("seasons", "blah", "tessi"), "depth")
})

test_that("cache_exists looks for directories, feather and parquet files", {
  file.exists <- mockery::mock(FALSE, FALSE, FALSE, FALSE)
  dir.exists <- mockery::mock(TRUE, FALSE)
  mockery::stub(cache_exists, "file.exists", file.exists)
  mockery::stub(cache_exists, "dir.exists", dir.exists)
  expect_true(cache_exists("season", "deep", "tessi"))
  expect_false(cache_exists("season", "deep", "tessi"))
  expect_equal(mockery::mock_args(dir.exists)[[1]][[1]], file.path(tempdir(), "deep", "tessi", "season"))
  expect_true(any(grepl("season\\.feather", mockery::mock_args(file.exists))))
  expect_true(any(grepl("season\\.parquet", mockery::mock_args(file.exists))))
})

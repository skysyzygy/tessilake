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
  expect_true(any(grepl("feather", mockery::mock_args(file.exists))))
  expect_true(any(grepl("parquet", mockery::mock_args(file.exists))))
})

withr::deferred_run()

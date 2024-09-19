withr::local_package("checkmate")
local_cache_dirs()

# cache_get_mtime ---------------------------------------------------------
test_read_write <- setattr(data.table(x = 1:100000, y = runif(1000)), "primary_keys", "x")

test_that("cache_get_mtime works with cache_write", {
  timeA <- Sys.time()
  cache_write(test_read_write, "test_read_write", "deep", "tessi", partition = FALSE)
  cache_write(test_read_write, "test_read_write", "shallow", "tessi", partition = FALSE)
  timeB <- Sys.time()

  cache_write(test_read_write, "test_partitioning", "deep", "tessi")
  cache_write(test_read_write, "test_partitioning", "shallow", "tessi")
  timeC <- Sys.time()

  expect_gt(cache_get_mtime("test_read_write", "deep", "tessi"), timeA)
  expect_gt(cache_get_mtime("test_read_write", "shallow", "tessi"), timeA)
  expect_lt(cache_get_mtime("test_read_write", "deep", "tessi"), timeB)
  expect_lt(cache_get_mtime("test_read_write", "shallow", "tessi"), timeB)

  expect_gt(cache_get_mtime("test_partitioning", "deep", "tessi"), timeB)
  expect_gt(cache_get_mtime("test_partitioning", "shallow", "tessi"), timeB)
  expect_lt(cache_get_mtime("test_partitioning", "deep", "tessi"), timeC)
  expect_lt(cache_get_mtime("test_partitioning", "shallow", "tessi"), timeC)
})

test_that("cache_get_mtime works with cache_update", {
  timeA <- Sys.time()
  cache_update(test_read_write[1,], "test_read_write", "deep", "tessi", partition = FALSE)
  cache_update(test_read_write[1,], "test_read_write", "shallow", "tessi", partition = FALSE)
  timeB <- Sys.time()

  cache_update(test_read_write[1,], "test_partitioning", "deep", "tessi")
  cache_update(test_read_write[1,], "test_partitioning", "shallow", "tessi")
  timeC <- Sys.time()

  expect_gt(cache_get_mtime("test_read_write", "deep", "tessi"), timeA)
  expect_gt(cache_get_mtime("test_read_write", "shallow", "tessi"), timeA)
  expect_lt(cache_get_mtime("test_read_write", "deep", "tessi"), timeB)
  expect_lt(cache_get_mtime("test_read_write", "shallow", "tessi"), timeB)

  expect_gt(cache_get_mtime("test_partitioning", "deep", "tessi"), timeB)
  expect_gt(cache_get_mtime("test_partitioning", "shallow", "tessi"), timeB)
  expect_lt(cache_get_mtime("test_partitioning", "deep", "tessi"), timeC)
  expect_lt(cache_get_mtime("test_partitioning", "shallow", "tessi"), timeC)
})

test_that("cache_get_mtime doesn't get confused about similarly named caches", {
  cache_write(test_read_write, "test_read_write", "deep", "tessi", partition = FALSE, overwrite = TRUE)
  timeA <- Sys.time()
  cache_write(test_read_write, "test_read_write2", "deep", "tessi", partition = FALSE)

  expect_lt(cache_get_mtime("test_read_write", "deep", "tessi"), timeA)
  expect_gt(cache_get_mtime("test_read_write2", "deep", "tessi"), timeA)

})


# cache_files -------------------------------------------------------------

test_that("cache_files returns named file, directory, and/or subfiles", {
  root_path <- cache_path("","shallow","tessi")

  expect_length(cache_files("test","shallow","tessi"),0)

  file.create(test_file <- file.path(root_path,"test.file"))
  expect_equal(cache_files("test","shallow","tessi"),test_file)
  expect_equal(cache_files("test.file","shallow","tessi"),test_file)

  dir.create(test_dir <- file.path(root_path,"test_dir"))
  expect_equal(cache_files("test","shallow","tessi"),test_file)
  expect_equal(cache_files("test.file","shallow","tessi"),test_file)
  expect_equal(cache_files("test_dir","shallow","tessi"),test_dir)

  file.create(test_file2 <- file.path(test_dir,"test.file"))
  expect_equal(cache_files("test","shallow","tessi"),test_file)
  expect_equal(cache_files("test.file","shallow","tessi"),test_file)
  expect_equal(cache_files("test_dir","shallow","tessi"),c(test_dir,test_file2))

})

test_that("cache_files handles filename regex collisions", {
  root_path <- cache_path("","shallow","tessi")

  file.create(test_file <- file.path(root_path,"test.file"))
  file.create(test_file2 <- file.path(root_path,"test+file.file"))
  file.create(test_file3 <- file.path(root_path,"a_test_file.file"))
  expect_equal(cache_files("test.file","shallow","tessi"),test_file)
  expect_equal(cache_files("test+file","shallow","tessi"),test_file2)
  expect_equal(cache_files("a_test_file","shallow","tessi"),test_file3)

})


# cache_delete ------------------------------------------------------------

test_that("cache_delete complains if arguments incorrect", {
  expect_error(cache_delete(), "table_name")
  expect_error(cache_delete("seasons", "deep", "blah"), "doesn't exist")
  expect_error(cache_delete("seasons", "blah", "tessi"), "Must be element of set")
})

test_that("cache_delete complains if cache doesn't exist", {
  expect_error(cache_delete("test", "deep", "tessi"), "doesn't exist")
})

test_that("cache_delete works with cache_write", {
  expect_true(file.exists(paste0(cache_path("test_read_write", "deep", "tessi"), ".parquet")))
  expect_true(file.exists(paste0(cache_path("test_read_write", "shallow", "tessi"), ".feather")))
  expect_length(dir(cache_path("test_partitioning", "deep", "tessi"), recursive = TRUE), 11)
  expect_length(dir(cache_path("test_partitioning", "shallow", "tessi"), recursive = TRUE), 11)

  cache_delete("test_read_write", "deep", "tessi")
  cache_delete("test_read_write", "shallow", "tessi")

  expect_false(file.exists(paste0(cache_path("test_read_write", "deep", "tessi"), ".parquet")))
  expect_false(file.exists(paste0(cache_path("test_read_write", "shallow", "tessi"), ".feather")))

  cache_delete("test_partitioning", "deep", "tessi", partitions = 0)
  cache_delete("test_partitioning", "shallow", "tessi", partitions = c(8, 9, 10))

  expect_length(dir(cache_path("test_partitioning", "deep", "tessi"), recursive = TRUE), 10)
  expect_length(dir(cache_path("test_partitioning", "shallow", "tessi"), recursive = TRUE), 8)

  cache_delete("test_partitioning", "deep", "tessi")
  cache_delete("test_partitioning", "shallow", "tessi")

  expect_false(dir.exists(cache_path("test_read_write", "deep", "tessi")))
  expect_false(dir.exists(cache_path("test_read_write", "shallow", "tessi")))
})

# cache_path ------------------------------------------------------------

test_that("cache_path complains if arguments incorrect", {
  expect_error(cache_path(), "table_name")
  expect_error(cache_path("seasons", "blah", "tessi"), "Must be element of set")
})

test_that("cache_path complains if the root directory doesn't exist or isn't writeable", {
  mockery::stub(cache_path, "config::get", list(depths = list(shallow = list(path = file.path(tempdir(), "doesntexist")))))
  expect_error(cache_path("seasons", "shallow", "stream"), "cache path")
})

test_that("cache_path returns a path", {
  cache_root <- config::get("tessilake")[["depths"]][["shallow"]][["path"]]
  expect_equal(cache_path("seasons", "shallow", "stream"), file.path(cache_root, "stream", "seasons"))
})

# cache_exists ------------------------------------------------------------

test_that("cache_exists complains if arguments incorrect", {
  expect_error(cache_exists(), "table_name")
  expect_error(cache_exists("seasons", "blah", "tessi"), "Must be element of set")
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

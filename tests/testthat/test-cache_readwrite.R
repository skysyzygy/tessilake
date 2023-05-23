withr::local_package("checkmate")
local_cache_dirs()

# cache_write -------------------------------------------------------------

test_read_write <- setattr(data.table(x = 1:100000, y = runif(1000)), "key", "value")

test_that("cache_write with partitioning=FALSE creates parquet and feather files", {
  cache_write(test_read_write, "test_read_write", "deep", "tessi", partition = FALSE)
  expect_true(file.exists(paste0(cache_path("test_read_write", "deep", "tessi"), ".parquet")))
  cache_write(test_read_write, "test_read_write", "shallow", "tessi", partition = FALSE)
  expect_true(file.exists(paste0(cache_path("test_read_write", "shallow", "tessi"), ".feather")))
})

test_that("cache_write with primary keys creates datasets", {
  test_partitioning <- copy(test_read_write)
  data.table::setattr(test_partitioning, "primary_keys", "x")
  cache_write(test_partitioning, "test_partitioning", "deep", "tessi")
  expect_equal(length(dir(cache_path("test_partitioning", "deep", "tessi"))), 11)
  cache_write(test_partitioning, "test_partitioning", "shallow", "tessi")
  expect_equal(length(dir(cache_path("test_partitioning", "shallow", "tessi"))), 11)
})

test_that("cache_write with primary keys doesn't have side effects on x", {
  cache_write(test_read_write, "test_read_write2", "deep", "tessi", primary_keys = "x")
  expect_false("primary_keys" %in% names(attributes(test_read_write)))
  expect_true("primary_keys" %in% names(cache_get_attributes(cache_read("test_read_write2", "deep", "tessi"))))
})

test_that("cache_write refuses to overwrite an existing cache unless overwrite==TRUE", {
  expect_error(cache_write(test_read_write, "test_read_write", "deep", "tessi", partition = FALSE), "overwrite")
  expect_error(cache_write(test_read_write, "test_read_write", "deep", "tessi"), "overwrite")
  expect_silent(cache_write(test_read_write, "test_read_write", "deep", "tessi", partition = FALSE, overwrite = TRUE))
})

test_that("cache_write with works with anything data.frameish", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  test_read_write_table <- dplyr::copy_to(con, test_read_write)

  cache_write(arrow_table(test_read_write), "test_read_write_arrow", "deep", "tessi")
  expect_true(file.exists(paste0(cache_path("test_read_write_arrow", "deep", "tessi"), ".parquet")))
  cache_write(test_read_write, "test_read_write_tbl", "deep", "tessi")
  expect_true(file.exists(paste0(cache_path("test_read_write_tbl", "deep", "tessi"), ".parquet")))

  DBI::dbDisconnect(con)
})

test_that("cache_write doesn't copy x", {
  tracemem(test_read_write)
  expect_silent(cache_write(test_read_write, "test_copy", "deep", "tessi"))
  untracemem(test_read_write)
})

test_that("cache_write returns nothing, invisibly", {
  expect_invisible(cache_write(test_read_write, "test_silent", "deep", "tessi"))
  expect_equal(cache_write(test_read_write, "test_silent2", "deep", "tessi"), NULL)
})

# cache_read --------------------------------------------------------------

test_that("cache_read returns FALSE for non-existent caches", {
  expect_false(suppressMessages(cache_read("blah", "deep", "tessi")))
})

test_that("cache_read returns data to the original form including attributes", {
  expect_equal(cache_read("test_read_write", "deep", "tessi") %>% collect(), test_read_write)
  expect_equal(cache_read("test_read_write", "shallow", "tessi") %>% collect(), test_read_write)
  expect_equal(cache_read("test_read_write_arrow", "deep", "tessi") %>% collect(), test_read_write)
  expect_equal(cache_read("test_read_write_tbl", "deep", "tessi") %>% collect(), test_read_write)
  expect_equal(cache_read("test_partitioning", "deep", "tessi") %>% collect() %>% setorderv("x") %>%
    setattr("partitioning", NULL) %>% setattr("primary_keys", NULL), test_read_write)
  expect_equal(cache_read("test_partitioning", "shallow", "tessi") %>% collect() %>% setorderv("x") %>%
    setattr("partitioning", NULL) %>% setattr("primary_keys", NULL), test_read_write)
})

test_that("cache_read with include_partition = TRUE returns the hidden partition column", {
  expect_equal(cache_read("test_partitioning", "deep", "tessi", include_partition = TRUE) %>% names(), c("x", "y", "partition_x"))
  expect_equal(cache_read("test_partitioning", "shallow", "tessi", include_partition = TRUE) %>% names(), c("x", "y", "partition_x"))
})

test_that("cache_read can select particular columns", {
  expect_equal(cache_read("test_read_write", "deep", "tessi", select = "y") %>% collect() %>% .[[1]], test_read_write[, y])
  expect_equal(cache_read("test_read_write", "shallow", "tessi", select = "y") %>% collect() %>% .[[1]], test_read_write[, y])
  expect_equal(cache_read("test_read_write_arrow", "deep", "tessi", select = "y") %>% collect() %>% .[[1]], test_read_write[, y])
  expect_equal(cache_read("test_read_write_tbl", "deep", "tessi", select = "y") %>% collect() %>% .[[1]], test_read_write[, y])
  expect_equal(cache_read("test_partitioning", "deep", "tessi", select = "y") %>% collect() %>% .[[1]] %>% sort(), test_read_write[, y] %>% sort())
  expect_equal(cache_read("test_partitioning", "shallow", "tessi", select = "y") %>% collect() %>% .[[1]] %>% sort(), test_read_write[, y] %>% sort())
})

test_that("cache_read is failure resistant", {
  test_read_write <- data.table(x = runif(1000000))
  path <- cache_path("test_read_write","deep","tessi")
  # point child process to parent tempdir
  mockery::stub(cache_write,"cache_path",path)
  mockery::stub(cache_exists,"cache_path",path)
  mockery::stub(cache_write,"cache_exists",cache_exists)
  # set up a write storm
  r <- callr::r_bg(function(){
    while(T){cache_write(test_read_write, "test_read_write", "deep", "tessi", overwrite = TRUE)}
    }, package = T)
  # and try to read it
  while(ncol(ret <- cache_read("test_read_write", "deep", "tessi"))>1)
    Sys.sleep(1)
  expect_equal(test_read_write,collect(ret))
})

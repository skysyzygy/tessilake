library(checkmate)

dir.create(file.path(tempdir(), "deep"))
dir.create(file.path(tempdir(), "shallow"))
withr::defer({
  unlink(file.path(tempdir(), "deep"), recursive = T)
  unlink(file.path(tempdir(), "shallow"), recursive = T)
})

# cache_create ------------------------------------------------------------

test_that("cache_create complains if arguments incorrect", {
  expect_error(cache_create(), "tableName")
  expect_error(cache_create("seasons", "deep", "blah"), "type")
  expect_error(cache_create("seasons", "blah", "tessi"), "depth")
})

test_that("cache_create creates the cache directories if they don't exist, but only if the root directory exists", {
  cachePath <- cache_create("seasons", "deep", "tessi")
  expect_true(!is.null(cachePath))
  expect_true(dir.exists(cachePath))
  unlink(cachePath, recursive = T)

  mockery::stub(cache_create,"config::get",file.path(tempdir(), "doesntexist"))
  expect_error(cache_create("seasons", "shallow", "stream"), "cache path")
})

# cache_read --------------------------------------------------------------

test_that("cache_read returns an Arrow Dataset", {
  test.read_write <- data.table(x = 1:100000, y = runif(1000))
  cache_write(test.read_write, "test.read_write", "deep", "tessi")
  expect_s3_class(cache_read("test.read_write", "deep", "tessi"), "Dataset")
})

# cache_write -------------------------------------------------------------

test_that("cache_write followed by cache_read returns data to the original form", {
  test.read_write <- data.table(x = 1:100000, y = runif(1000))
  cache_write(test.read_write, "test.read_write", "deep", "tessi")
  expect_equal(length(dir(cache_create("test.read_write", "deep", "tessi"))), 1)
  expect_equal(collect(cache_read("test.read_write", "deep", "tessi")), test.read_write)
})

test_that("cache_write followed by cache_read returns data to the original form when partitioning", {
  test.partitioning <- data.table(x = 1:100000, y = runif(1000), z = rep(1:100, each = 1000))
  data.table::setattr(test.partitioning, "primaryKeys", "x")
  cache_write(test.partitioning, "test.partitioning", "deep", "tessi")
  expect_equal(length(dir(cache_create("test.partitioning", "deep", "tessi"))), 11)
  expect_equal(collect(cache_read("test.partitioning", "deep", "tessi")) %>% setorderv("x"), test.partitioning)
})

test_that("cache_write doesn't copy x", {
  test.read_write <- data.table(x = 1:100000, y = runif(1000))
  tracemem(test.read_write)
  expect_silent(cache_write(test.read_write, "test.read_write", "deep", "tessi"))
})

test_that("cache_write returns nothing, invisibly", {
  test.read_write <- data.table(x = 1:100000, y = runif(1000))
  expect_invisible(cache_write(test.read_write, "test.read_write", "deep", "tessi"))
  expect_equal(cache_write(test.read_write, "test.read_write", "deep", "tessi"), NULL)
})

# cache_make_partition -----------------------------------------------------

test_that("cache_make_partitioning requires a data.frame with a valid primary key",{
  x = data.frame(a=c(1,2,3),b=c("a","b","c"))
  expect_error(cache_make_partitioning(),"x")
  expect_error(cache_make_partitioning(x),"primaryKey")
  expect_error(cache_make_partitioning(x,primaryKey="c"),"colnames")
  y = rbind(x,NA)
  expect_error(cache_make_partitioning(y,primaryKey="a"),"missing")
  y = rbind(x,list(a=3,b="c"))
  expect_error(cache_make_partitioning(y,primaryKey="a"),"duplicated")
})

test_that("cache_make_partitioning suggests partitioning information for numeric primary keys",{
  x = data.frame(a=c(1,2,3),b=c("a","b","c"))
  expect_class(cache_make_partitioning(x,"a"),"call")
  expect_equal(cache_make_partitioning(x,"a"),rlang::expr(floor(a/10000)))
  x$a = x$a*2
  expect_equal(cache_make_partitioning(x,"a"),rlang::expr(floor(a/20000)))
  x$a = x$a*.01
  expect_equal(cache_make_partitioning(x,"a"),rlang::expr(floor(a/200)))
})

test_that("cache_make_partitioning suggests partitioning information for character primary keys",{
  x = data.frame(a=c(1,2,3),b=c("a","b","c"))
  expect_class(cache_make_partitioning(x,"b"),"call")
  expect_equal(cache_make_partitioning(x,"b"),rlang::expr(substr(tolower(b),1,2)))
})

test_that("cache_make_partitioning doesn't copy x",{
  x = data.frame(a=c(1,2,3),b=c("a","b","c"))
  tracemem(x)
  expect_silent(cache_make_partitioning(x,"b"))
})

# cache_get_partition -----------------------------------------------------

test_that("cache_get_partition reads partitioning information from Arrow Dataset",{
  x = data.frame(a=c(1,2,3),b=c("a","b","c"))
  setattr(x,"primaryKeys","a")
  cache_write(x, "x", "deep", "tessi")
  dataset = cache_read("x", "deep", "tessi")
  expect_class(cache_get_partitioning(dataset),"call")
  expect_equal(cache_get_partitioning(dataset),cache_make_partitioning(x))
})

# cache_update -------------------------------------------------------------

test_that("cache_update refuses to update a partitioned table with one without a primary key", {
  test.read_write <- data.table(x = 1:100000, y = runif(1000))
  test.partitioning <- data.table(x = 1:100000, y = runif(1000))
  data.table::setattr(test.partitioning, "primaryKeys", "x")

  cache_write(test.partitioning, "test.partitioning", "deep", "tessi")

  expect_error(cache_update(test.read_write, "test.partitioning", "deep", "tessi"), "partitioning")
})

test_that("cache_update updates the whole table when there is no partitioning", {
  test.read_write <- data.table(x = 1:100000, y = runif(1000))
  cache_update(test.read_write, "test.read_write", "deep", "tessi")
  expect_equal(collect(cache_read("test.read_write", "deep", "tessi")), test.read_write)
})

test_that("cache_update updates the whole table when there is no partitioning and doesn't copy from", {
  test.read_write <- data.table(x = 1:100000, y = runif(1000))
  tracemem(test.read_write)
  expect_silent(cache_update(test.read_write, "test.read_write", "deep", "tessi"))
})

test_that("cache_update updates rows incrementally, and only in the required partitions", {
  test.incremental <- data.table(x = 1:100000, y = runif(1000))
  update.incremental <- data.table(x = 1000:1999, y = runif(1000))

  cache_write(test.incremental, "test.incremental", "deep", "tessi", primaryKeys = "x")
  time <- Sys.time()
  cache_update(update.incremental, "test.incremental", "deep", "tessi", primaryKeys = "x")

  updated_cache <- setorderv(collect(cache_read("test.incremental", "deep", "tessi")), "x") %>%
    setattr("partitioning",NULL)
  updated_table <- update_table(update.incremental, test.incremental, primaryKeys = c("x"))

  expect_equal(updated_cache, updated_table)

  cache_files <- sapply(dir(cache_create("test.incremental", "deep", "tessi"), recursive = T, full.names = T), file.mtime)
  updated_cache_files <- purrr::keep(cache_files, ~ . > time)
  expect_equal(length(updated_cache_files), 1)
  expect_equal(length(cache_files) - length(updated_cache_files), 10)
})

test_that("cache_update updates rows incrementally and doesn't copy from", {
  test.incremental <- data.table(x = 1:100000, y = runif(1000))
  update.incremental <- data.table(x = 1000:1999, y = runif(1000))
  tracemem(update.incremental)
  cache_write(test.incremental, "test.incremental", "deep", "tessi", primaryKeys = "x")
  expect_silent(cache_update(update.incremental, "test.incremental", "deep", "tessi", primaryKeys = "x"))
})

test_that("cache_update returns nothing, invisibly", {
  test.incremental <- data.table(x = 1:100000, y = runif(1000))
  update.incremental <- data.table(x = 1000:1999, y = runif(1000))

  cache_write(test.incremental, "test.incremental", "deep", "tessi")
  cache_update(update.incremental, "test.incremental", "deep", "tessi", primaryKeys = "x")

  expect_equal(cache_update(update.incremental, "test.incremental", "deep", "tessi", primaryKeys = "x"), NULL)
  expect_invisible(cache_update(update.incremental, "test.incremental", "deep", "tessi", primaryKeys = "x"))
})

withr::deferred_run()

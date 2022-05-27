withr::local_options(tessilake.shallow=file.path(tempdir(),"shallow"),
              tessilake.deep=file.path(tempdir(),"deep"),
              tessilake.tessitura="Tessitura") #TODO: dummy database for tests without Tessitura present

dir.create(file.path(tempdir(),"deep"))
dir.create(file.path(tempdir(),"shallow"))
withr::defer({
  unlink(file.path(tempdir(),"deep"),recursive=T)
  unlink(file.path(tempdir(),"shallow"),recursive=T)})

# cache_create ------------------------------------------------------------

test_that("cache_create complains if arguments incorrect",{
  expect_error(cache_create(),"tableName")
  expect_error(cache_create("seasons","deep","blah"),"type")
  expect_error(cache_create("seasons","blah","tessi"),"depth")
})

test_that("cache_create creates the cache directories if they don't exist, but only if the root directory exists", {
  withr::local_options(tessilake.shallow=file.path(tempdir(),"doesntexist"))

  cachePath = cache_create("seasons","deep","tessi")
  expect_true(!is.null(cachePath))
  expect_true(dir.exists(cachePath))
  unlink(cachePath,recursive = T)

  expect_error(cache_create("seasons","shallow","stream"),"cache path")
})

# cache_read --------------------------------------------------------------

test_that("cache_read returns an Arrow Dataset",{
  test.read_write = data.table(x=1:100000,y=runif(1000))
  cache_write(test.read_write,"test.read_write","deep","tessi")
  expect_s3_class(cache_read("test.read_write","deep","tessi"),"Dataset")
})

# cache_write -------------------------------------------------------------

test_that("cache_write followed by cache_read returns data to the original form", {
  test.read_write = data.table(x=1:100000,y=runif(1000))
  cache_write(test.read_write,"test.read_write","deep","tessi")
  expect_equal(length(dir(cache_create("test.read_write","deep","tessi"))),1)
  expect_equal(collect(cache_read("test.read_write","deep","tessi")),test.read_write)
})

test_that("cache_write followed by cache_read returns data to the original form when partitioning",{
  test.partitioning = data.table(x=1:100000,y=runif(1000),z=rep(1:100,each=1000))
  data.table::setattr(test.partitioning,"primaryKeys","x")
  cache_write(test.partitioning,"test.partitioning","deep","tessi")
  expect_gt(length(dir(cache_create("test.partitioning","deep","tessi"))),1)
  expect_equal(collect(cache_read("test.partitioning","deep","tessi")) %>% setorderv("x"),test.partitioning)
})

test_that("cache_write returns nothing, invisibly",{
  test.read_write = data.table(x=1:100000,y=runif(1000))
  expect_invisible(cache_write(test.read_write,"test.read_write","deep","tessi"))
  expect_equal(cache_write(test.read_write,"test.read_write","deep","tessi"),NULL)
})

# cache_update -------------------------------------------------------------

test_that("cache_update refuses to update a partitioned table with one without a primary key", {
  test.read_write = data.table(x=1:100000,y=runif(1000))
  test.partitioning = data.table(x=1:100000,y=runif(1000))
  data.table::setattr(test.partitioning,"primaryKeys","x")

  cache_write(test.partitioning,"test.partitioning","deep","tessi")

  expect_error(cache_update(test.read_write,"test.partitioning","deep","tessi"),"partitioning")
})

test_that("cache_update updates the whole table when there is no partitioning",{
  test.read_write = data.table(x=1:100000,y=runif(1000))
  cache_update(test.read_write,"test.read_write","deep","tessi")
  expect_equal(collect(cache_read("test.read_write","deep","tessi")),test.read_write)
})

test_that("cache_update updates rows incrementally, and only in the required partitions", {
  test.incremental = data.table(x=1:100000,y=runif(1000))
  update.incremental = data.table(x=1000:1999,y=runif(1000))

  cache_write(test.incremental,"test.incremental","deep","tessi",primaryKeys="x")
  time = Sys.time()
  cache_update(update.incremental,"test.incremental","deep","tessi",primaryKeys="x")

  updated_cache = setorderv(collect(cache_read("test.incremental","deep","tessi")),"x")
  updated_table = update_table(update.incremental,test.incremental,primaryKeys=c("x"))

  expect_equal(updated_cache,updated_table)

  cache_files = sapply(dir(cache_create("test.incremental","deep","tessi"),recursive = T,full.names = T),file.mtime)
  updated_cache_files = purrr::keep(cache_files,~.>time)
  expect_equal(length(updated_cache_files),100)
  expect_equal(length(cache_files)-length(updated_cache_files),899)
})

test_that("cache_update returns nothing, invisibly", {
  test.incremental = data.table(x=1:100000,y=runif(1000))
  update.incremental = data.table(x=1000:1999,y=runif(1000))

  cache_write(test.incremental,"test.incremental","deep","tessi")
  cache_update(update.incremental,"test.incremental","deep","tessi",primaryKeys="x")

  expect_equal(cache_update(update.incremental,"test.incremental","deep","tessi",primaryKeys="x"),NULL)
  expect_invisible(cache_update(update.incremental,"test.incremental","deep","tessi",primaryKeys="x"))

})
withr::deferred_run()

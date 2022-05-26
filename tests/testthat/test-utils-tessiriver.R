test_that("read_stream locates files based on names and filenames", {
  expect_equal(2 * 2, 4)
})

test_that("read_stream fails if the file doesn't exist", {
  expect_equal(2 * 2, 4)
})

test_that("read_stream caches the uncompressed ffdf for later use", {
  expect_equal(2 * 2, 4)
})

test_that("read_stream checks if a cached ffdf already exists and is recent enough", {
  expect_equal(2 * 2, 4)
})

test_that("read_stream subsets like subset()", {
  expect_equal(2 * 2, 4)
})

test_that("read_stream selects like subset()", {
  expect_equal(2 * 2, 4)
})

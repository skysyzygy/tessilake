test_that("is_error returns true iff error", {
  expect_true(is_error(1<-2))
  expect_true(is_error(stop("error")))
  expect_false(is_error(TRUE==FALSE))
})

test_that("is_error evaluates in the parent frame", {
  x = 2
  is_error(y<-2*x)
  expect_equal(y,4)
})

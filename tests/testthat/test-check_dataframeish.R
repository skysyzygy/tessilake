test_that("assert_dataframeish returns an error for non-dataframeish objects", {
  expect_error(assert_dataframeish(1),"select, filter, mutate, collect")
  expect_error(assert_dataframeish(NULL),"select, filter, mutate, collect")
  expect_error(assert_dataframeish("a"),"select, filter, mutate, collect")
  expect_error(assert_dataframeish(list()),"filter, mutate, collect")
})

test_that("assert_dataframeish passes dataframeish objects", {
  expect_silent(assert_dataframeish(data.frame()))
  expect_silent(assert_dataframeish(arrow_table(data.frame())))
  expect_silent(assert_dataframeish(filter(arrow_table(data.frame(x=1)),x==1)))
})

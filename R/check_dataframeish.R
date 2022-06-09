#' check_dataframeish
#'
#' Tests if an object has data.frame methods `select`, `filter`, `mutate`, and `collect` defined so that we can treat it
#' like a data.frame.
#'
#' @param obj object to test
#'
#' @return see the [checkmate] package for more information on check/assert/test functions
#' @importFrom utils methods
#' @export
#'
#' @examples
#' check_dataframeish(list())
#' # FALSE
check_dataframeish <- function(obj) {
  dataframeish_classes <-
    list(
      select = methods(select),
      filter = methods(filter),
      mutate = methods(mutate),
      collect = methods(collect)
    ) %>%
    lapply(as.vector) %>%
    lapply(gsub, pattern = "^.+?\\.", replacement = "", perl = T)

  missing_methods <- purrr::map(dataframeish_classes, ~ any(. %in% class(obj))) %>%
    purrr::keep(~ . == FALSE)

  if (length(missing_methods) > 0) {
    return(paste("Must implement the data.frameish methods:", paste(names(missing_methods), collapse = ", ")))
  }

  return(TRUE)
}

#' @describeIn check_dataframeish checkmate assert function for check_dataframeish
assert_dataframeish <- checkmate::makeAssertionFunction(check_dataframeish)

#' @describeIn check_dataframeish checkmate test function for check_dataframeish
test_dataframeish <- checkmate::makeTestFunction(check_dataframeish)

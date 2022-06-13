
#' cache_make_partitioning
#'
#' Builds suggested partitioning from the primary keys of the table.
#'
#' @param x data.frame
#' @param primary_keys primary keys to set partitioning with
#'
#' @return an unevaluated expression determining the partitioning
#' @export
#' @importFrom checkmate assert_character check_character assert_names check_numeric assert test_numeric test_character
#' @importFrom rlang expr
#' @importFrom stats median
#' @examples
#' x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
#'
#' cache_make_partitioning(x, primary_keys = "a")
#' # expression a/10000
cache_make_partitioning <- function(x, primary_keys = attr(x, "primary_keys")) {
  . <- NULL

  assert_dataframeish(x)
  assert_character(primary_keys, min.len = 1)
  assert_names(names(x), must.include = primary_keys)

  if (is.data.table(x)) {
    primary_key <- x[, primary_keys[[1]], with = F][[1]]
  } else {
    primary_key <- select(x, !!primary_keys[[1]]) %>%
      collect() %>%
      .[[1]]
  }

  assert(
    check_character(primary_key, any.missing = F, unique = T),
    check_numeric(primary_key, any.missing = F, unique = T)
  )

  primary_key <- sort(primary_key)

  partitioning <- NULL

  if (test_numeric(primary_key, any.missing = F, unique = T)) {
    offset <- 10000 * median(primary_key[-1] - primary_key[-length(primary_key)])
    partitioning <- expr(floor(!!sym(primary_keys[[1]]) / !!offset))
  } else if (test_character(primary_key, any.missing = F, unique = T)) {
    partitioning <- expr(substr(tolower(!!sym(primary_keys[[1]])), 1, 2))
  }

  partitioning
}

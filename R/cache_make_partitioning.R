
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
#' @importFrom dplyr arrange
#' @importFrom rlang expr
#' @importFrom stats median
#' @examples
#' x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
#'
#' cache_make_partitioning(x, primary_keys = "a")
#' # expression a/10000
cache_make_partitioning <- function(x, primary_keys = cache_get_attributes(x)$primary_keys) {
  . <- NULL

  assert_dataframeish(x)
  assert_character(primary_keys, min.len = 1)
  assert_names(names(x), must.include = primary_keys)
  pk <- primary_keys[[1]]

  if (is.data.table(x)) {
    primary_key <- x[, pk, with = F]
    setkeyv(primary_key,pk)[[1]]
  } else {
    primary_key <- select(x, !!pk) %>%
      arrange(!!sym(pk)) %>%
      collect() %>% .[[pk]]
  }

  assert(
    check_character(primary_key, any.missing = F),
    check_numeric(primary_key, any.missing = F)
  )

  partitioning <- NULL

  if (test_numeric(primary_key, any.missing = F)) {
    # make sure offset is greater than 1 and is an integer
    offset <- max(ceiling(
      10000 * mean(primary_key[-1] - primary_key[-length(primary_key)])),
      1)
    partitioning <- expr(floor(!!sym(pk) / !!offset))
  } else if (test_character(primary_key, any.missing = F)) {
    partitioning <- expr(substr(tolower(!!sym(pk)), 1, 2))
  }

  partitioning
}

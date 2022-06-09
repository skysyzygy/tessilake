#' cache_get_attributes
#'
#' Internal function to read R attributes on an Arrow Dataset/arrow_dplyr_query or other object
#'
#' @param x Arrow Object
#'
#' @return a named vector of attributes
#' @importFrom checkmate assert test_class
#' @examples
#' \dontrun{
#' cache_get_attributes(cache_read("test", "deep", "stream"))
#' }
cache_get_attributes <- function(x) {
  if (!test_class(x, "ArrowObject") && !test_class(x, "arrow_dplyr_query")) {
    return(attributes(x))
  }

  dataset <- (if (inherits(x, "arrow_dplyr_query")) x$.data else x)

  attributes <- NULL

  if (!is.null(dataset$metadata$r)) {
    attributes <- unserialize(charToRaw(dataset$metadata$r))$attributes
    attributes$names <- attributes$names %||% names(dataset)
    attributes$row.names <- attributes$row.names %||% 1:nrow(dataset)
  }

  attributes
}

#' cache_set_attributes
#'
#' Internal function to set R attributes from an Arrow Dataset/arrow_dplyr_query
#'
#' @param x Arrow Object
#' @param attributes list of attributes to set
#'
#' @return NULL
#' @importFrom checkmate assert test_class
#' @importFrom bit setattributes
#' @examples
#' \dontrun{
#' cache_set_attributes(cache_read("test", "deep", "stream"), list(primary_keys = "a"))
#' }
cache_set_attributes <- function(x, attributes) {
  if (!test_class(x, "ArrowObject") && !test_class(x, "arrow_dplyr_query")) {
    return(setattributes(x, attributes))
  }

  dataset <- (if (inherits(x, "arrow_dplyr_query")) x$.data else x)

  if (!is.null(dataset$metadata$r)) {
    r <- unserialize(charToRaw(dataset$metadata$r))
    r$attributes[names(attributes)] <- attributes[names(attributes)]
    dataset$metadata$r <- rawToChar(serialize(r, NULL, ascii = TRUE))
  }

  invisible(NULL)
}


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


#' cache_get_mtime
#'
#' Get the last modification time of the cache by querying the mtime of all of the files that make up the cache
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#'
#' @return POSIXct modification time
#'
cache_get_mtime <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream")) {
  cache_files <- c(
    dir(cache_path(table_name, depth, type), full.names = TRUE, recursive = TRUE),
    dir(dirname(cache_path(table_name, depth, type)), full.names = TRUE, recursive = TRUE)
  )

  cache_mtime <- max(file.mtime(cache_files), na.rm = TRUE)
}

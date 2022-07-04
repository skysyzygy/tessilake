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
  attributes <- NULL

  if (!test_class(x, "ArrowObject") && !test_class(x, "arrow_dplyr_query")) {
    attributes <- attributes(x)
  } else {
    if (inherits(x, "arrow_dplyr_query"))
      return(cache_get_attributes(x$.data))

    if (!is.null(x$metadata$r)) {
      attributes <- unserialize(charToRaw(x$metadata$r))$attributes
    }
  }

  attributes[!names(attributes) %in% c("names", "row.names")]
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
  attributes <- attributes[!names(attributes) %in% c("names", "row.names")]

  if (!test_class(x, "ArrowObject") && !test_class(x, "arrow_dplyr_query")) {
    return(setattributes(x, attributes))
  }

  if (inherits(x, "arrow_dplyr_query"))
    return(cache_set_attributes(x$.data, attributes))

  if (!is.null(x$metadata$r)) {
    r <- unserialize(charToRaw(x$metadata$r))
    r$attributes[names(attributes)] <- attributes[names(attributes)]
    tryCatch(
      x$schema$metadata <- list(r = rawToChar(serialize(r, NULL, ascii = TRUE))),
      error = function(e) {
        x$metadata <- list(r = rawToChar(serialize(r, NULL, ascii = TRUE)))
      }
    )
  }

  invisible(NULL)
}

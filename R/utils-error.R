#' is.error
#'
#' Helper function to use error-throwing expressions in stopifnot
#'
#' @param expr expression to run (in the parent frame)
#'
#' @return boolean, TRUE if the expression causes an error, FALSE if not
#'
#' @examples
#' is.error(stop("Error"))
#'
is.error = function(expr) {
  inherits(try(expr,silent=T),"try-error")
}

#' is_error
#'
#' Helper function to use error-throwing expressions in tests
#'
#' @param expr expression to run (in the parent frame)
#'
#' @return boolean, TRUE if the expression causes an error, FALSE if not
#' @importFrom rlang enexpr
#'
#' @examples
#' is_error(stop("Error"))
#'
is_error <- function(expr) {
  expr <- enexpr(expr)
  inherits(try(eval(expr, envir = parent.frame()), silent = T), "try-error")
}

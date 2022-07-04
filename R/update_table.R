#' expr_get_names
#'
#' Extract (symbol) names from an expression. The expression must be a list or vector of strings and/or symbols
#'
#' @param expr expression to extract names from
#'
#' @return vector of strings, the names referred to in the expression
#' @importFrom checkmate assert check_true
#' @importFrom rlang call_args as_name call_name
#' @examples
#' \dontrun{
#' expr_get_names(expr("a")) == expr_get_names(expr(a))
#' expr_get_names(expr(c("a", b)))
#' # c("a","b")
#' expr_get_names(expr(list("a", b = c)))
#' # c("a",b="c")
#' }
expr_get_names <- function(expr) {
  assert(check_true(is.language(expr) || is.character(expr) || is.null(expr)))

  expr <- if (is.character(expr) || is.null(expr)) {
    expr
  } else if (is.call(expr) && call_name(expr) %in% c("c", "list")) {
    sapply(call_args(expr), as_name)
  } else if (is.name(expr)) {
    as_name(expr)
  } else {
    stop(sprintf("Can't parse expression (%s), must be a list or vector of strings and/or symbols", as.character(expr)))
  }

  expr
}

#' update_table
#'
#' updates the data.table-like object `to` based on the data.frame-like `from` object,
#' using `date_column` to determine which rows have been updated and updating only those with matching `primary_keys`.
#'
#' @param from the source object
#' @param to the object to be updated
#' @param date_column string or tidyselected column identifying the date column to use to determine which rows to update
#' @param primary_keys vector of strings or tidyselected columns identifying the primary keys to determine which rows to update
#' @param delete whether to delete rows in `to` missing from `from`, default is not to delete the rows
#'
#' @importFrom dplyr collect semi_join select
#' @importFrom rlang as_name call_args eval_tidy
#' @importFrom bit setattributes
#' @importFrom checkmate assert_class assert_subset check_subset assert
#' @return an updated data.table updated in-place
#' @export
#'
#' @examples
#' library(data.table)
#' expect <- data.table(expand.grid(x = 1:100, y = 1:100))[, data := runif(.N)]
#' # divide the data
#' from <- copy(expect)[1:9000]
#' to <- copy(expect)[1000:10000]
#' # and mung it up
#' to[1:5000, data := runif(.N)]
#'
#' update_table(from, to, primary_keys = c(x, y)) == expect
#' # TRUE
update_table <- function(from, to, date_column = NULL, primary_keys = NULL, delete = FALSE) {
  assert_dataframeish(from)
  assert_class(to, "data.table")
  assert_subset(colnames(from), colnames(to))

  primary_keys <- expr_get_names(rlang::enexpr(primary_keys))
  date_column <- expr_get_names(rlang::enexpr(date_column))

  if (is.null(primary_keys)) {
    if (!is.null(date_column)) {
      stop(sprintf("primary_keys must be given if date_column is given"))
    }
    return(to = collect(from))
  } # just copy everything from from into to

  assert(check_subset(primary_keys, colnames(from)),
    check_subset(primary_keys, colnames(to)),
    check_subset(date_column, colnames(from)),
    check_subset(date_column, colnames(to)),
    combine = "and"
  )

  if (is.data.table(from)) {
    cols_from <- from[, c(date_column, primary_keys), with = F]
  } else {
    cols_from <- select(from, !!c(date_column, primary_keys)) %>%
      collect() %>%
      setDT()
  }
  cols_to <- to[, mget(c(date_column, primary_keys))]

  # rows that match in from and to
  update <- cols_to[cols_from, on = primary_keys]
  # optionally filtered by date
  if (!is.null(date_column)) {
    update <- update[get(date_column) != get(paste0("i.", date_column))]
  }

  # rows that don't yet exist in to
  new <- cols_from[!cols_to, on = primary_keys]

  # rows that are missing in from
  if (delete == TRUE) {
    remove <- cols_to[!cols_from, on = primary_keys]
    to <- to[!remove, on = primary_keys]
  }

  if (is.data.table(from)) {
    from_update <- from[update, colnames(from), on = primary_keys, with = F]
    from_new <- from[new, colnames(from), on = primary_keys, with = F]
  } else {
    from_update <- semi_join(from, update, by = primary_keys, copy = T) %>% collect()
    from_new <- semi_join(from, new, by = primary_keys, copy = T) %>% collect()
  }

  to[update, (colnames(from)) := from_update, on = primary_keys]
  to <- rbindlist(list(to, from_new), use.names = TRUE)
  setorderv(to, primary_keys)

  to
}

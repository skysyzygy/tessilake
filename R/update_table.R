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
#' @importFrom dplyr collect left_join select
#' @importFrom rlang as_name call_args eval_tidy
#' @importFrom bit setattributes
#' @importFrom checkmate assert_class assert_subset check_subset assert
#' @return a data.table updated in-place
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
#'
update_table <- function(from, to, date_column = NULL, primary_keys = NULL, delete = FALSE) {
  UseMethod("update_table",from)
}

#' @export
#' @rdname update_table
update_table.default <- function(from, to, date_column = NULL, primary_keys = NULL, delete = FALSE) {
  assert_dataframeish(from)
  assert_class(to, "data.table")
  assert_subset(colnames(from), colnames(to))

  primary_keys <- expr_get_names(rlang::enexpr(primary_keys))
  date_column <- expr_get_names(rlang::enexpr(date_column))

  if (is.null(primary_keys) &&  !is.null(date_column))
    stop(sprintf("primary_keys must be given if date_column is given"))

  if (is.null(primary_keys) || delete == TRUE && is.null(date_column))
    return(to = collect(from))
   # just copy everything from from into to

  assert(check_subset(primary_keys, colnames(from)),
    check_subset(primary_keys, colnames(to)),
    check_subset(date_column, colnames(from)),
    check_subset(date_column, colnames(to)),
    combine = "and"
  )

  all <- left_join(from,
                  mutate(select(to, !!c(date_column, primary_keys)),
                         to = TRUE),
                  by = primary_keys,
                  suffix = c("",".to"),
                  copy = TRUE)

  if(!is.null(date_column)) {
    all <- filter(all, is.na(to) | !!sym(date_column) != !!sym(paste0(date_column,".to"))) %>%
    select(-!!paste0(date_column,".to"))
  }

  all <- collect(all) %>% setDT

  # rows that don't yet exist in to
  new <- all[is.na(to)][,to:=NULL]
  # rows that exist in both
  update <- all[!is.na(to)][,to:=NULL]
  # rows that are missing in from
  if (delete == TRUE) {
    to <- to[!all,on = primary_keys]
  }

  to[update, (colnames(from)) := update, on = primary_keys]
  to <- rbindlist(list(to, new), use.names = TRUE)
  setorderv(to, primary_keys)

  to
}

#' @export
#' @rdname update_table
update_table.data.table <- function(from, to, date_column = NULL, primary_keys = NULL, delete = FALSE) {
  assert_class(from, "data.table")
  assert_class(to, "data.table")
  assert_subset(colnames(from), colnames(to))

  primary_keys <- expr_get_names(rlang::enexpr(primary_keys))
  date_column <- expr_get_names(rlang::enexpr(date_column))

  if (is.null(primary_keys) &&  !is.null(date_column))
    stop(sprintf("primary_keys must be given if date_column is given"))

  if (is.null(primary_keys) || delete == TRUE && is.null(date_column))
    return(to = from)
   # just copy everything from from into to

  assert(check_subset(primary_keys, colnames(from)),
         check_subset(primary_keys, colnames(to)),
         check_subset(date_column, colnames(from)),
         check_subset(date_column, colnames(to)),
         combine = "and"
  )

  # rows that match in from and to
  to_rows <- na.omit(to[from, which = TRUE, on = primary_keys])

  # rows that need to be updated
  if (!is.null(date_column)) {
    update <- from[!to[to_rows,c(primary_keys,date_column),with = FALSE],
                   on = c(primary_keys,date_column)]
  } else {
    update <- from[to[to_rows,c(primary_keys),with = FALSE],
                   on = c(primary_keys)]
  }

  # rows that don't yet exist in to
  new <- from[!to, on = primary_keys]

  # rows that are missing in from
  if (delete == TRUE) {
    to <- to[to_rows]
  }

  to[update, (colnames(from)) := mget(paste0("i.",colnames(from))), on = primary_keys]
  to <- rbindlist(list(to, new), use.names = TRUE)
  setorderv(to, primary_keys)

  to
}

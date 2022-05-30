# data.table is generally careful to minimize the scope for namespace
# conflicts (i.e., functions with the same name as in other packages);
# a more conservative approach using @importFrom should be careful to
# import any needed data.table special symbols as well, e.g., if you
# run DT[ , .N, by='grp'] in your package, you'll need to add
# @importFrom data.table .N to prevent the NOTE from R CMD check.
# See ?data.table::`special-symbols` for the list of such symbols
# data.table defines; see the 'Importing data.table' vignette for more
# advice (vignette('datatable-importing', 'data.table')).
#
#' @import data.table
NULL

#' setleftjoin
#' Fast left join for data.table that simply adds r's columns to l by reference.
#  Requires that r has at most one item of each join group in order to be able to simply add to l.
#' @param l left side of the join (data.table)
#' @param r right side of the join (data.table)
#' @param by character vector for matching columns. Names, if they exist, refer to columns in the
#' left table, values to columns in the right table
#'
#' @return a joined data.table, invisibly
#' @importFrom glue glue
#' @importFrom  rlang %||%
#' @importFrom stats setNames
#' @import data.table
#' @export
#'
#' @examples
#'
#' library(data.table)
#'
#' l <- data.table(a = c(1, 2, 3), b = c(3, 4, NA))
#' r <- data.table(b = c(3, 7, NA), c = c(1, 2, 4))
#'
#' setleftjoin(l, r, by = "b")
#' setleftjoin(l, r, by = c("b" = "c"))
#'
setleftjoin <- function(l, r, by = NULL) {
  N <- NULL
  stopifnot(is.data.frame(l), is.data.frame(r))

  setDT(r)
  setDT(l)
  index.r <- indices(r)
  if (is.null(by)) {
    by <- intersect(colnames(l), colnames(r))
    message(glue("Joining on {paste(by,collapse=',')}"))
  }

  # build by.l and by.r from by
  by.l <- names(by) %||% by
  by.r <- unname(by)
  by.l[by.l == ""] <- by.r[by.l == ""]
  setkeyv(l, by.l)
  setindexv(r, by.r)

  # fail if there's too many matching rows in r
  if (r[, .N, by = by.r][N > 1, .N] > 0) stop(glue("{deparse(substitute(r))} must have no more than one row per join column."))

  # build out column names
  on <- setNames(by.l, by.r)
  collisions <- intersect(
    setdiff(colnames(l), by.l),
    setdiff(colnames(r), by.r)
  )
  cols.r <- setdiff(colnames(r), by.r)
  cols.l <- colnames(l)
  cols.r[cols.r %in% collisions] <- paste0(cols.r[cols.r %in% collisions], ".y")
  cols.l[cols.l %in% collisions] <- paste0(cols.l[cols.l %in% collisions], ".x")

  if (length(collisions)) {
    # rename columns in l
    l[, (cols.l) := .SD]
    l[, (setdiff(colnames(l), cols.l)) := NULL]
  }
  # and set columns from r
  l[, (cols.r) := r[l, setdiff(colnames(r), by.r), on = on, with = F]]

  setindexv(r, index.r)
  setkeyv(l, by.l)
  l
}


#' update_table
#'
#' updates the data.table-like object `to` based on the data.frame-like `from` object,
#' using `dateColumn` to determine which rows have been updated and updating only those with matching `primaryKeys`.
#'
#' @param from the source object
#' @param to the object to be updated
#' @param dateColumn string or tidyselected column identifying the date column to use to determine which rows to update
#' @param primaryKeys vector of strings or tidyselected columns identifying the primary keys to determine which rows to update
#'
#' @importFrom dplyr collect semi_join select
#' @importFrom rlang as_name call_args eval_tidy
#' @importFrom bit setattributes
#' @importFrom checkmate assert_data_frame assert_subset
#' @return an updated data.table updated in-place
#' @export
#'
#' @examples
#' from <- data.frame()
#'
update_table <- function(from, to, dateColumn = NULL, primaryKeys = NULL) {
  assert_data_frame(from)
  assert_data_frame(to)
  assert_subset(colnames(from),colnames(to))

  if (missing(primaryKeys)) {
    return(to = collect(from))
  } # just copy everything from from into to

  if (!missing(dateColumn)) {
    dateColumn <- as_name(enquo(dateColumn))
    if (!dateColumn %in% colnames(from) || !dateColumn %in% colnames(to)) {
      stop(sprintf("dateColumn (%s) must exist in 'from' and 'to'.", dateColumn))
    }
  }
  if (!missing(primaryKeys)) {
    primaryKeys <- enquo(primaryKeys)
    if (!is.error(call_args(primaryKeys)) && is.error(primaryKeys <- sapply(call_args(primaryKeys), as_name))) {
      stop(sprintf("primaryKeys must be a vector of strings or tidy-selected columns"))
    }
    if (!is.vector(primaryKeys) && is.error(primaryKeys <- as_name(primaryKeys))) {
      stop(sprintf("primaryKeys must be a vector of strings or tidy-selected columns"))
    }
    if (!all(primaryKeys %in% colnames(from)) || !all(primaryKeys %in% colnames(to))) {
      stop(sprintf("primaryKeys (%s) must exist in 'from' and 'to'.", primaryKeys))
    }
  }
  if(missing(primaryKeys) && !is.null(dateColumn)) {
    stop(sprintf("primaryKeys must be given if dateColumn is given"))
  }

  if(is.data.table(from)) {
    cols.from = from[,c(dateColumn,primaryKeys),with=F]
  } else {
    cols.from <- select(from, !!c(dateColumn, primaryKeys)) %>%
      collect() %>%
      setDT()
  }
  cols.to <- to[, mget(c(dateColumn, primaryKeys))]

  # rows that match in from and to
  update <- cols.to[cols.from, on = primaryKeys]
  # optionally filtered by date
  if (!missing(dateColumn)) {
    update <- update[get(dateColumn) != get(paste0("i.", dateColumn))]
  }

  # rows that don't yet exist in to
  new <- cols.from[!cols.to, on = primaryKeys]

  if(is.data.table(from)) {
    from.update = from[update,colnames(from),on = primaryKeys,with=F]
    from.new = from[new,colnames(from),on=primaryKeys,with=F]
  } else {
    from.update = semi_join(from, update, by = primaryKeys, copy = T) %>% collect()
    from.new = semi_join(from, new, by = primaryKeys, copy = T) %>% collect()
  }

  to[update,(colnames(from)):=from.update,on=primaryKeys]
  to <- rbindlist(list(to, from.new))

  to
}

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
#' l = data.table(a=c(1,2,3),b=c(3,4,NA))
#' r = data.table(b=c(3,7,NA),c=c(1,2,4))
#'
#' setleftjoin(l,r,by="b")
#' setleftjoin(l,r,by=c("b"="c"))
#'
setleftjoin = function(l,r,by=NULL) {
  N <- NULL
  stopifnot(is.data.frame(l),is.data.frame(r))

  setDT(r)
  setDT(l)
  index.r = indices(r)
  if(is.null(by)) {
    by = intersect(colnames(l),colnames(r))
    message(glue("Joining on {paste(by,collapse=',')}"))
  }

  # build by.l and by.r from by
  by.l=names(by) %||% by
  by.r=unname(by)
  by.l[by.l == ""] <- by.r[by.l == ""]
  setkeyv(l,by.l)
  setindexv(r,by.r)

  # fail if there's too many matching rows in r
  if( r[,.N,by=by.r][N>1,.N] > 0 ) stop(glue("{deparse(substitute(r))} must have no more than one row per join column."))

  # build out column names
  on=setNames(by.l,by.r)
  collisions = intersect(setdiff(colnames(l),by.l),
                         setdiff(colnames(r),by.r))
  cols.r = setdiff(colnames(r),by.r)
  cols.l = colnames(l)
  cols.r[cols.r %in% collisions] = paste0(cols.r[cols.r %in% collisions],".y")
  cols.l[cols.l %in% collisions] = paste0(cols.l[cols.l %in% collisions],".x")

  if(length(collisions)) {
    # rename columns in l
    l[,(cols.l):=.SD]
    l[,(setdiff(colnames(l),cols.l)):=NULL]
  }
  # and set columns from r
  l[,(cols.r):=r[l,setdiff(colnames(r),by.r),on=on,with=F]]

  setindexv(r,index.r)
  setkeyv(l,by.l)
  l
}

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

setleftjoin = function(l,r,by=NULL) {
  # Fast left join that simply adds r columns to l.
  # Requires that r has at most one item of each join group in order to be able to simply add to l.
  if(is.null(by)) {
    by = intersect(colnames(l),colnames(r))
    message(glue::glue("Joining on {paste(by,sep=',')}"))
  }
  setDT(r)
  setDT(l)
  by.l=rlang::`%||%`(names(by),by)
  by.r=unname(by)
  by.l[by.l == ""] <- by.r[by.l == ""]
  setindexv(l,by.l)
  setkeyv(r,by.r)
  if( r[,.N,by=by.r][N>1,] %>% nrow > 0 ) stop(glue::glue("{deparse(substitute(r))} must have no more than one row per join column."))
  on=setNames(by.l,by.r)
  l[,(colnames(r)):=r[l,colnames(r),on=on,with=F]]
}

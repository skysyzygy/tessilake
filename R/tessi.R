#' tessi_list_tables
#'
#' The list of tessitura tables is configured in the extdata/tessi_tables.yml file in the package directory
#' supplemented by the tessi_tables dictionary in config.yml
#'
#' ## yml format
#' ```
#' {short_name}:
#'    long_name: {name of table/view to be loaded}
#'    base_table: {the underlying table being queried that has primary_keys}
#'    primary_keys: {the primary key(s) as a value or a list of values}
#' ````
#'
#' @return  data.table of configured tessitura tables with columns short_name, long_name, baseTable and primary_keys
#' @importFrom yaml read_yaml
#' @export
#' @examples
#' # customers:
#' #   long_name: BI.VT_CUSTOMER
#' #   base_table: T_CUSTOMER
#' #   primary_keys: customer_no
#'
#' tessi_list_tables()[short_name == "customers"]
#'
tessi_list_tables <- function() {
  config_tessi_tables <- NULL

  try(
    {
      config_default <- config::get()
      config_tessi_tables <- setdiff(config::get(config = "tessi_tables"), config_default)
    },
    silent = TRUE
  )

  c(
    read_yaml(system.file("extdata", "tessi_tables.yml", package = "tessilake")),
    config_tessi_tables
  ) %>%
    rbindlist(idcol = "short_name")
}


#' read_tessi
#'
#' Thin wrapper on read_sql_table using the tables configured in `list_tessi_tables`
#'
#' @param table_name character name of the table to read from Tessitura, either one of the available tables (see [tessi_list_tables()]) or the
#' name of a SQL table that exists in Tessitura. The default SQL table schema is `dbo`.
#' @param select vector of strings or symbols indicating columns to select from stream file
#' @param freshness the returned data will be at least this fresh
#' @param ... further arguments to be passed to or from other methods
#'
#' @return an Apache Arrow Table, see the [arrow::arrow-package] package for more information.
#' @export
#'
#' @examples
#' \dontrun{
#' read_tessi("memberships",
#'   select = c("memb_level", "customer_no")
#' )
#' }
#'
read_tessi <- function(table_name, select = NULL,
                       freshness = as.difftime(7, units = "days"), ...) {

  short_name <- NULL

  select <- enquo(select)
  assert_character(table_name)
  assert_names(table_name, subset.of=tessi_list_tables()$short_name)

  table_data = tessi_list_tables()[short_name==table_name] %>% as.list

  table_data$long_name = strsplit(table_data$long_name,".",fixed = TRUE)[[1]]

  if(length(table_data$long_name)==1)
    return(read_sql_table(table_name = table_data$long_name[[1]],
                          primary_keys=table_data$primary_keys, freshness = freshness))

  return(read_sql_table(table_name = table_data$long_name[[2]], schema = table_data$long_name[[1]],
                        primary_keys=table_data$primary_keys, freshness = freshness))

}


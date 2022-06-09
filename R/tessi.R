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
#' Read a table from Tessitura and cache it locally as a Feather file and remotely as a Parquet file.
#' Cache storage locations are managed by `tessilake.shallow` and `tessilake.deep` options.
#' Tessitura database connection defined by an ODBC profile with the name set by the `tessilake.tessitura` option.
#'
#' @param table_name character name of the table to read from Tessitura, either one of the available tables (see [tessi_list_tables()]) or the
#' name of a SQL table that exists in Tessitura. The default SQL table schema is `dbo`.
#' @param select vector of strings or symbols indicating columns to select from stream file
#' @param freshness the returned data will be at least this fresh
#' @param ... further arguments to be passed to or from other methods
#'
#' @return an Apache Arrow Table, see the [arrow::arrow-package] package for more information.
#' @importFrom dplyr summarise
#' @importFrom arrow arrow_table
#' @export
#'
#' @examples
#' \dontrun{
#' read_tessi("memberships",
#'   subset = init_dt >= as.Date("2021-07-01"),
#'   select = c("memb_level", "customer_no")
#' )
#' }
#'
read_tessi <- function(table_name, select = NULL,
                       freshness = as.difftime(7, units = "days"), ...) {
  . <- last_update_dt <- NULL

  select <- enquo(select)
  assert_character(table_name)

  tessi_mtime <- tessi_read_db(table_name) %>%
    summarise(max(last_update_dt)) %>%
    collect() %>%
    .[[1]]

  test_mtime <- Sys.time() - freshness

  if (!cache_exists(table_name, "deep", "tessi")) {
    cache_write(tessi_read_db(table_name), table_name, "deep", "tessi", partition = FALSE)
    deep_mtime <- cache_get_mtime(table_name, "deep", "tessi")
  } else if ((deep_mtime <- cache_get_mtime(table_name, "deep", "tessi")) < test_mtime &&
    tessi_mtime > deep_mtime) {
    cache_update(tessi_read_db(table_name),
      table_name, "deep", "tessi",
      date_column = "last_update_dt"
    )
  }

  if (!cache_exists(table_name, "shallow", "tessi")) {
    cache_write(cache_read(table_name, "deep", "tessi"), table_name, "shallow", "tessi", partition = FALSE)
  } else if ((shallow_mtime <- cache_get_mtime(table_name, "shallow", "tessi")) < test_mtime &&
    deep_mtime > shallow_mtime) {
    cache_update(cache_read(table_name, "deep", "tessi"),
      table_name, "shallow", "tessi",
      date_column = "last_update_dt"
    )
  }

  cache_read(table_name, "shallow", "tessi")
}

#' tessi_read_db
#' internal function to return a Tessitura table based on a name
#' @param table_name string
#'
#' @return dplyr database query
#' @importFrom dplyr tbl filter
#' @importFrom dbplyr in_schema
#' @examples
#' \dontrun{
#' tessi_read_db("seasons")
#' }
tessi_read_db <- function(table_name) {

}

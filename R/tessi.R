#' tessi_list_tables
#'
#' The list of tessitura tables is configured in the extdata/tessi_tables.yml file in the package directory
#'
#' ## yml format
#' ```
#' {short_name}:
#'    long_name: {name of table/view to be loaded}
#'    baseTable: {the underlying table being queried that has primary_keys}
#'    primary_keys: {the primary key(s) as a value or a list of values}
#' ````
#' @return  data.table of configured tessitura tables with columns short_name, long_name, baseTable and primary_keys
#' @examples
#' # customers:
#' #   long_name: BI.VT_CUSTOMER
#' #   baseTable: T_CUSTOMER
#' #   primary_keys: customer_no
#'
#' list_tessi_tables()[short_name == "customers"]
#'
tessi_list_tables <- function() {
  tessi_tables
}

#' @rdname tessi_list_tables
tessi_tables <- yaml::read_yaml(system.file("extdata", "tessi_tables.yml", package = "tessilake")) %>% rbindlist(idcol = "short_name")

#' read_tessi
#'
#' Read a table from Tessitura and cache it locally as a Feather file and remotely as a Parquet file.
#' Cache storage locations are managed by `tessilake.shallow` and `tessilake.deep` options.
#' Tessitura database connection defined by an ODBC profile with the name set by the `tessilake.tessitura` option.
#'
#' @param table_name character name of the table to read from Tessitura, either one of the available tables (see [list_tessi_tables()]) or the
#' name of a SQL table that exists in Tessitura. The default SQL table schema is `dbo`.
#' @param subset logical expression indicating elements or rows to keep
#' @param select vector of strings or symbols indicating columns to select from stream file
#' @param ... further arguments to be passed to or from other methods
#'
#' @return an Apache Arrow Table, see the [arrow::arrow-package] package for more information.
#' @export
#'
#' @examples
#' \dontrun{
#'   read_tessi("memberships",
#'     subset = init_dt >= as.Date("2021-07-01"),
#'     select = c("memb_level", "customer_no")
#'   )
#' }
#'
read_tessi <- function(table_name, subset = NULL, select = NULL, ...) {
  subset <- enquo(subset)
  select <- expr_get_names(select)

  assert_character(table_name)

  table <- tessi_read_db(table_name)
  cache_deep <- cache_read(table_name, "deep", "tessi")
  cache_shallow <- cache_read(table_name, "shallow", "tessi")

  head(table, 1000) %>% collect()
}

#' tessi_read_db
#' internal function to return a Tessitura table based on a name
#' @param table_name string
#'
#' @return dplyr database query
#' @import odbc DBI
#' @importFrom dplyr tbl filter
#' @importFrom dbplyr in_schema
#' @examples
tessi_read_db <- function(table_name) {
  short_name <- TABLE_SCHEMA <- TABLE_NAME <- NULL

  stopifnot(
    "table_name is required" = !missing(table_name),
    "Please set the tessilake.tessitura option to define the Tessitura ODBC DSN" =
      !is.null(config::get("tessilake.tessitura")),
    "Please define an working ODBC data source to connect to Tessitura" =
      !is.error(db <- DBI::dbConnect(odbc::odbc(), config::get("tessilake.tessitura"), encoding = "latin1"))
  )

  # map string table_name to SQL table_name
  if (table_name %in% tessi_tables$short_name) {
    long_name <- tessi_tables[short_name == table_name, long_name[1]]
    primary_keys <- tessi_tables[short_name == table_name, primary_keys]
  } else {
    long_name <- table_name
  }
  # add dbo schema if no schema present
  long_name <- strsplit(long_name, "\\.")[[1]]
  if (length(long_name) == 1) long_name <- c("dbo", long_name)

  # check that table exists
  available_tables <- DBI::dbGetQuery(db, "select TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.VIEWS
                                        union
                                        select TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.TABLES")
  if (!nrow(filter(
    available_tables,
    TABLE_SCHEMA == long_name[[1]] &
      TABLE_NAME == long_name[[2]]
  ))) {
    stop(paste("Table", paste(long_name, collapse = "."), "doesn't exist."))
  }

  # get primary key if we don't know it yet
  if (!exists("primary_keys")) {
    primary_keys <- DBI::dbGetQuery(db, sprintf("select column_name from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cc
                                   join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc on tc.CONSTRAINT_NAME=cc.CONSTRAINT_name and
                                   CONSTRAINT_TYPE='PRIMARY KEY' and
                                   cc.TABLE_SCHEMA like '%s' and cc.TABLE_NAME like '%s'", long_name[[1]], long_name[[2]]))[[1]]
  }

  # build the table query with dplyr
  table <- tbl(db, in_schema(long_name[[1]], long_name[[2]]))
  attr(table, "primary_keys") <- primary_keys
  table
}

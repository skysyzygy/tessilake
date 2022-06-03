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
#'
#' @return  data.table of configured tessitura tables with columns short_name, long_name, baseTable and primary_keys
#' @examples
#' # customers:
#' #   long_name: BI.VT_CUSTOMER
#' #   baseTable: T_CUSTOMER
#' #   primary_keys: customer_no
#'
#' tessi_list_tables()[short_name == "customers"]
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
#'   read_tessi("memberships",
#'     subset = init_dt >= as.Date("2021-07-01"),
#'     select = c("memb_level", "customer_no")
#'   )
#' }
#'
read_tessi <- function(table_name, subset = NULL, select = NULL,
                       freshness = as.difftime(7,units="days"), ...) {

  subset <- enquo(subset)
  select <- enquo(select)

  assert_character(table_name)

  sources = list(table = expr(tessi_read_db(table_name)),
                 deep = expr(cache_read(table_name, "deep", "tessi")),
                 shallow = expr(cache_read(table_name, "shallow", "tessi")))

  test_time = Sys.time() - freshness

  for(i in seq_along(sources[-1])) {
    source = eval(sources[[i]])
    cache_name = names(sources[i+1])
    cache = suppressMessages(eval(sources[[i+1]]))

    if(is.logical(cache) && cache==FALSE) {
      # new cache!
      primary_keys = if(inherits(source,"ArrowObject") || inherits(source,"arrow_dplyr_query")) {
        cache_get_attributes(source)$primary_keys
      } else {
        attr(source,"primary_keys")
      }
      cache_write(setattr(collect(source),"primary_keys",primary_keys),
                  table_name,cache_name,"tessi",partition=FALSE)
    } else {
      if(!is.null(cache$files)) {
        cache_mtime = max(file.mtime(cache$files))
      } else {
        cache_mtime = max(file.mtime(paste0(cache_path(table_name,cache_name,"tessi"),c(".parquet",".feather"))),na.rm = TRUE)
      }
      if(cache_mtime < test_time) {
        date_column = NULL
        if("last_update_dt" %in% colnames(source) && "last_update_dt" %in% colnames(cache))
          date_column = "last_update_dt"

        cache_update(source,table_name,cache_name,"tessi",date_column = date_column)
      }
    }
  }

  cache_read(table_name,"shallow","tessi")
}

#' tessi_read_db
#' internal function to return a Tessitura table based on a name
#' @param table_name string
#'
#' @return dplyr database query
#' @importFrom dplyr tbl filter
#' @importFrom dbplyr in_schema
#' @examples
tessi_read_db <- function(table_name) {

  assert_character(table_name)
  assert_character(config::get("tessilake.tessitura"))

  if(is_error(sql_connect())) {
    stop("Please define an working ODBC data source to connect to Tessitura")
  }

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


#' sql_connect
#'
#' Connects and disconnects from the SQL database using the `tessilake.tessitura` connection string in config.yml and
#' updates the Rstudio connection pane
#'
#' @return invisible NULL
#' @importFrom rlang ns_env env_bind
#'
#' @examples
#' \dontrun{
#' tessilake:::db$db
#' sql_connect()
#' tessilake:::db$db
#' sql_disconnect()
#' }
sql_connect <- function() {
  if (is.null(db$db)) {
    tryCatch({
      db_expr <- expr(DBI::dbConnect(odbc::odbc(), !!config::get("tessilake.tessitura"), encoding = "latin1"))

      callbacks <- getTaskCallbackNames()
      db$db <- eval(db_expr)
      removeTaskCallback(setdiff(getTaskCallbackNames(), callbacks))

      if("odbc.version" %in% names(attributes(db$db)))
        odbc:::on_connection_opened(db$db, deparse(db_expr))
    },error=function(e) {
      message(e$message)
      stop("Database connection failed, please set the `tessilake.tessitura` configuration in config.yml to a valid database DSN.")
    })
  }
  invisible(NULL)
}

#' @describeIn sql_connect Tear down the SQL connection
sql_disconnect <- function() {
  DBI::dbDisconnect(db$db)
  db$db <- NULL
}

db <- new.env(parent = emptyenv())

#' read_sql
#'
#' Execute a database query and cache it locally as a Feather file and remotely as a Parquet file.
#' Cache storage locations are managed by `tessilake.shallow` and `tessilake.deep` options.
#' Database connection defined by an ODBC profile with the name set by the `tessilake.tessitura` option.
#'
#' @param query character query to run on the database.
#' @param name name of the query, defaults to the SHA1 hash of the query.
#' @param select vector of strings indicating columns to select from database
#' @param primary_keys primary keys of the query to be used for incremental updates
#' @param date_column update date column of the query to be used for incremental updates
#' @param freshness the returned data will be at least this fresh
#'
#' @return an Apache Arrow Table, see the [arrow::arrow-package] package for more information.
#' @importFrom arrow arrow_table
#' @importFrom checkmate assert_character
#' @importFrom dplyr tbl sql across summarise
#' @importFrom dbplyr tbl_sql
#' @importFrom digest sha1
#' @export
#'
#' @examples
#' \dontrun{
#' read_sql("select * from T_CUSTOMER","t_customer",
#'   primary_keys = "customer_no",
#'   date_column = "last_update_dt"
#' )}
read_sql <- function(query, name = digest::sha1(query),
                     select = NULL,
                     primary_keys = NULL, date_column = NULL,
                     freshness = as.difftime(7, units = "days")) {
  . <- NULL

  assert_character(query)
  if(!is.null(date_column)) assert_character(date_column,max.len = 1)
  if(!is.null(primary_keys)) assert_character(primary_keys,min.len = length(date_column))

  sql_connect()
  # build the query with dplyr
  table <- tbl(db$db, sql(query))

  sql_mtime <- if(!is.null(date_column)) {
    table %>% summarise(across(!!date_column,max)) %>% collect() %>% .[[1]]
  } else {
    Sys.time()
  }

  test_mtime <- Sys.time() - freshness

  if (!cache_exists(name, "deep", "tessi")) {
    cache_write(collect(table), name, "deep", "tessi", partition = FALSE, primary_keys = primary_keys)
    deep_mtime <- cache_get_mtime(name, "deep", "tessi")
  } else if ((deep_mtime <- cache_get_mtime(name, "deep", "tessi")) < test_mtime &&
             sql_mtime > deep_mtime) {
    cache_update(table, name, "deep", "tessi", primary_keys = primary_keys,
                 date_column = date_column
    )
  }

  if (!cache_exists(name, "shallow", "tessi")) {
    cache_write(cache_read(name, "deep", "tessi"), name, "shallow", "tessi", partition = FALSE)
  } else if ((shallow_mtime <- cache_get_mtime(name, "shallow", "tessi")) < test_mtime &&
             deep_mtime > shallow_mtime) {
    cache_update(cache_read(name, "deep", "tessi"),
                 name, "shallow", "tessi",
                 date_column = "last_update_dt"
    )
  }

  cache_read(name, "shallow", "tessi", select = select)

}

#' read_sql_table
#'
#' @param schema character, database schema. Default is `dbo`
#' @param table_name character, table name without schema.
#' @param select vector of strings indicating columns to select from database
#' @param primary_keys character vector, primary keys of the table. `read_sql_table` will attempt to identify the primary keys using
#' SQL metadata.
#' @param date_column character, date column of the table showing the last date the row was updated.
#' Defaults to "last_update_dt" if it exists in the table.
#' @param freshness the returned data will be at least this fresh
#' @describeIn read_sql Reads a table or view from a SQL database and caches it locally using read_sql.
#' @importFrom dplyr filter select collect
#' @return an Apache Arrow Table, see the [arrow::arrow-package] package for more information.
#' @export
#'
#' @examples
#' \dontrun{
#' read_sql_table("T_CUSTOMER")
#' }
read_sql_table <- function(table_name, schema="dbo",
                           select = NULL,
                           primary_keys = NULL, date_column = NULL,
                           freshness = as.difftime(7, units = "days")) {

  . <- TABLE_SCHEMA <- TABLE_NAME <- COLUMN_NAME <- NULL

  assert_character(table_name,max.len=1)
  assert_character(schema,max.len=1,null.ok=TRUE)
  sql_connect()

  available_tables <- list(dbo = list(table_name = DBI::dbListTables(db$db,schema_name="dbo")),
                            BI = list(table_name = DBI::dbListTables(db$db,schema_name="BI"))) %>%
    rbindlist(idcol="schema")

  if(available_tables[
    eval(expr(schema == !!schema & table_name == !!table_name)),
    .N]==0)
    stop(paste("Table", paste(schema, table_name, collapse = "."), "doesn't exist."))

  available_columns <- read_sql("select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS",
                                "available_columns",freshness=freshness) %>%
    filter(TABLE_SCHEMA==schema & TABLE_NAME==table_name) %>%
    select(COLUMN_NAME) %>% collect() %>% .[[1]]

  if (!is.null(select)) assert_names(select,subset.of = available_columns)

  # get primary key info
  if (is.null(primary_keys)) {
    pk_table = read_sql("select cc.TABLE_SCHEMA, cc.TABLE_NAME, COLUMN_NAME from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cc
                                   join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc on tc.CONSTRAINT_NAME=cc.CONSTRAINT_name and
                                   CONSTRAINT_TYPE='PRIMARY KEY'","primary_keys", freshness=freshness)
    primary_keys = filter(pk_table,TABLE_SCHEMA==schema & TABLE_NAME==table_name) %>%
      select(COLUMN_NAME) %>% collect() %>% .[[1]]
  }

  if (is.null(date_column) & length(primary_keys)>0) {
    if("last_update_dt" %in% available_columns)
      date_column = "last_update_dt"
  }

  # build the table query
  read_sql(query = paste("select * from",DBI::dbQuoteIdentifier(db$db,DBI::Id(schema = schema, table = table_name))),
           name = paste(c(schema,table_name),collapse="."),
           select = select,
           primary_keys = primary_keys,
           date_column = date_column,
           freshness = freshness)
}

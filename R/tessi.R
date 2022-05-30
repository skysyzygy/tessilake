#' tessi_list_tables
#'
#' The list of tessitura tables is configured in the extdata/tessiTables.yml file in the package directory
#'
#' ## yml format
#' ```
#' {shortName}:
#'    longName: {name of table/view to be loaded}
#'    baseTable: {the underlying table being queried that has primaryKeys}
#'    primaryKeys: {the primary key(s) as a value or a list of values}
#' ````
#' @return  data.table of configured tessitura tables with columns shortName, longName, baseTable and primaryKeys
#' @examples
#' #customers:
#' #   longName: BI.VT_CUSTOMER
#' #   baseTable: T_CUSTOMER
#' #   primaryKeys: customer_no
#'
#' list_tessi_tables()[shortName=="customers"]
#'
tessi_list_tables = function() { tessiTables }

#' @rdname tessi_list_tables
tessiTables = yaml::read_yaml(system.file("extdata","tessiTables.yml",package="tessilake")) %>% rbindlist(idcol="shortName")

#' read_tessi
#'
#' Read a table from Tessitura and cache it locally as a Feather file and remotely as a Parquet file.
#' Cache storage locations are managed by `tessilake.shallow` and `tessilake.deep` options.
#' Tessitura database connection defined by an ODBC profile with the name set by the `tessilake.tessitura` option.
#'
#' @param tableName character name of the table to read from Tessitura, either one of the available tables (see [list_tessi_tables()]) or the
#' name of a SQL table that exists in Tessitura. The default SQL table schema is `dbo`.
#' @param subset logical expression indicating elements or rows to keep
#' @param select character vector indicating columns to select from stream file
#' @param ... further arguments to be passed to or from other methods
#'
#' @return an Apache Arrow Table, see the [arrow::arrow-package] package for more information.
#' @export
#'
#' @examples
#'
#' if(FALSE){read_tessi("memberships",
#'          init_dt>=as.Date("2021-07-01"),
#'          c("memb_level","customer_no")}
#'
read_tessi = function(tableName,subset,select,...) {

  subset = enquo(subset)

  stopifnot("tableName is required" = !missing("tableName"))

  table = read_tessi_db(tableName)
  cache.deep = read_cache(tableName,"tessi","deep")
  cache.shallow = read_cache(tableName,"tessi","shallow")

  head(table,1000) %>% collect
}

#' tessi_read_db
#' internal function to return a Tessitura table based on a name
#' @param tableName string
#'
#' @return dplyr database query
#' @import odbc DBI
#' @importFrom dplyr tbl filter
#' @importFrom dbplyr in_schema
#' @examples
#'
tessi_read_db = function(tableName) {
  shortName = TABLE_SCHEMA = TABLE_NAME = NULL

  stopifnot(
    "tableName is required" = !missing(tableName),
    "Please set the tessilake.tessitura option to define the Tessitura ODBC DSN" =
      !is.null(getOption("tessilake.tessitura")),
    "Please define an working ODBC data source to connect to Tessitura" =
      !is.error(db <- DBI::dbConnect(odbc::odbc(), .Options$tessilake.tessitura, encoding = "latin1"))
  )

  # map string tableName to SQL tableName
  if(tableName %in% tessiTables$shortName) {
    longName = tessiTables[shortName==tableName,longName[1]]
    primaryKeys = tessiTables[shortName==tableName,primaryKeys]
  } else {longName = tableName}
  # add dbo schema if no schema present
  longName = strsplit(longName,"\\.")[[1]]
  if(length(longName)==1) longName = c("dbo",longName)

  # check that table exists
  availableTables = DBI::dbGetQuery(db,"select TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.VIEWS
                                        union
                                        select TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.TABLES")
  if(!nrow(filter(availableTables,
                  TABLE_SCHEMA==longName[[1]] &
                  TABLE_NAME==longName[[2]]))) stop(paste("Table",paste(longName,collapse="."),"doesn't exist."))

  # get primary key if we don't know it yet
  if(!exists("primaryKeys")) {
    primaryKeys = DBI::dbGetQuery(db,sprintf("select column_name from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cc
                                   join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc on tc.CONSTRAINT_NAME=cc.CONSTRAINT_name and
                                   CONSTRAINT_TYPE='PRIMARY KEY' and
                                   cc.TABLE_SCHEMA like '%s' and cc.TABLE_NAME like '%s'",longName[[1]],longName[[2]]))[[1]]
  }

  # build the table query with dplyr
  table = tbl(db,in_schema(longName[[1]],longName[[2]]))
  attr(table,"primaryKeys") = primaryKeys
  table
}


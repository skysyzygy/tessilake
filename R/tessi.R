tessiTables = read_yaml(system.file("extdata","tessiTables.yml",package="tessilake")) %>% rbindlist(idcol="shortName")

#' is.error
#' helper function to use errorable expressions in stopifnot
#' @param expr expression to run (in the parent frame)
#'
#' @return boolean, TRUE if the expression causes an error, FALSE if not
#'
#' @examples
is.error = function(expr) {
  inherits(try(expr,silent=T),"try-error")
}


#' list_tessi_tables
#'
#' @return named list of tables. Tables are defined in a yaml file stored in extdata/tessiTables.yml
#' @export
#'
#' @examples
#'
#' list_tessi_tables()
#'
list_tessi_tables = function() { tessiTables }

#' read_tessi
#'
#' Read a table from Tessitura and cache it locally as a Parquet file.
#' Cache storage is managed by "tessilake.shallow" option.
#' Tessitura database connection defined by an ODBC profile with the name set by the "tessilake.Tessitura" option.
#'
#' @param tableName character name of the table to read from Tessitura, either one of the available tables (see "list_tessi_tables") or a
#' table that exists in Tessitura. The default schema is "dbo"
#' @param subset logical expression indicating elements or rows to keep
#' @param select character vector indicating columns to select from stream file
#' @param ... further arguments to be passed to or from other methods
#'
#' @return an Apache Arrow Table, see the "arrow" package for more information
#' @export
#'
#' @examples
#'
#' \dontrun{read_tessi("memberships",
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

#' read_tessi_db
#' internal function to return a Tessitura table based on a name
#' @param tableName string
#'
#' @return dplyr database query
#' @import odbc DBI
#' @importFrom dplyr tbl filter
#' @importFrom dbplyr in_schema
#' @examples
#'
read_tessi_db = function(tableName) {
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

#' read_tessi_cache
#' internal function to read cached tessi files
#' @param tableName string
#' @param type string, either "tessi" or "stream"
#' @param depth string, either "deep" or "shallow"
#'
#' @return environment containing the deep and shallow caches as arrow Datasets
#' @importFrom arrow open_dataset write_dataset
#' @examples
read_cache = function(tableName,type=c("tessi","stream"),depth=c("deep","shallow")) {


  stopifnot("tableName is required" = !missing(tableName),
            "'depth' must be either 'deep' or 'shallow'" = depth %in% c("deep","shallow"))
  tessilake.option = paste0("tessilake.",depth)
  if(is.null(getOption(tessilake.option)) || !dir.exists(getOption(tessilake.option)))
    stop(paste0("Please set the tessilake.",depth," option to define the local cache location"))

  # build the cache query with arrow
  cachePath = file.path(getOption(tessilake.option),type,paste(tableName,collapse="."))
  if(!dir.exists(cachePath)) dir.create(cachePath,recursive=T)

  cache = open_dataset(cachePath,format=ifelse(depth=="deep","parquet","arrow"))

}

#' update_data.table
#' updates the data.table-like object "to" based on the data.frame-like "from" object,
#' using "dateColumn" to determine which rows have been updated
#' and updating only those with matching "primaryKey".
#'
#' @param from the source object
#' @param to the object to be updated
#' @param dateColumn string or tidyselected column identifying the date column to use to determine which rows to update
#' @param primaryKeys vector of strings or tidyselected columns identifying the primary keys to determine which rows to update
#'
#' @importFrom dplyr collect semi_join select_at
#' @importFrom rlang as_name call_args
#' @return an updated data.table updated in-place
#' @export
#'
#' @examples
update_data.table = function(from,to,dateColumn,primaryKeys) {
   # convert to a string

  stopifnot("'from' and 'to' are required" = !missing(from) & !missing(to),
            "Column names in 'from' must be in 'to'" = all(colnames(from) %in% colnames(to)),
            "'to' must be a data.table" = inherits(to,"data.table"),
            "'from' must be data.frame-like" = inherits(to,"data.frame"))

  if(missing(dateColumn) || missing(primaryKeys)) {to[] = collect(from[])} # just copy everything from from into to

  if(!missing(dateColumn)) {
    stopifnot(
            "dateColumn must be a string or a tidy-selected column" =
              !is.error(dateColumn <- as_name(enquo(dateColumn))),
            "dateColumn must exist in 'from' and 'to'" =
              dateColumn %in% colnames(from) && dateColumn %in% colnames(to))
  }
  if(!missing(primaryKeys)) {
    stopifnot(
            "primaryKeys must be a vector of strings or a vector of tidy-selected columns" =
              !is.error(primaryKeys <- sapply(call_args(enquo(primaryKeys)),as_name)),
            "primaryKeys must exist in 'from' and 'to'" =
              all(primaryKeys %in% colnames(from)) && all(primaryKeys %in% colnames(to)))
  }

  cols.from = select_at(from,c(dateColumn,primaryKeys)) %>% collect %>% setDT
  cols.to = to[,mget(c(dateColumn,primaryKeys))]
  cols.diff = cols.from[!cols.to,on=c(dateColumn,primaryKeys)]

  if(cols.diff[,.N]>0) {
    update = semi_join(from,cols.diff,by=primaryKeys,copy=T) %>% collect %>% setDT
    to[update,(colnames(to)):=mget(paste0("i.",colnames(to))),on=primaryKeys]
    to = rbind(to,update[!to,on=primaryKeys])
  }

  to
}

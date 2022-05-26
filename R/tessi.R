tessiTables =
  rbindlist(list(
        list(shortName="accounts",  longName="T_ACCOUNT_DATA",	baseTable="T_ACCOUNT_DATA",	primaryKeys=c("id")),
        list(shortName="activities",	longName="BI.VT_CUST_ACTIVITY",	baseTable="T_CUST_ACTIVITY",	primaryKeys=c("activity_no")),
        list(shortName="addresses",	longName="BI.VT_ADDRESS",	baseTable="T_ADDRESS",	primaryKeys=c("address_no")),
        list(shortName="appeals",	longName="BI.VT_APPEAL",	baseTable="T_APPEAL",	primaryKeys=c("appeal_no")),
        list(shortName="audit",	longName="TA_AUDIT_TRAIL",	baseTable="TA_AUDIT_TRAIL",	primaryKeys=c("")),
        list(shortName="emails",	longName="T_EADDRESS",	baseTable="T_EADDRESS",	primaryKeys=c("eaddress_no")),
        list(shortName="affiliations",	longName="BI.VT_AFFILIATION",	baseTable="T_AFFILIATION",	primaryKeys=c("affiliation_no")),
        list(shortName="attributes",	longName="BI.VT_ATTRIBUTE",	baseTable="TX_CUST_KEYWORD",	primaryKeys=c("cust_keyword_no")),
        list(shortName="campaigns",	longName="BI.VT_CAMPAIGN",	baseTable="T_CAMPAIGN",	primaryKeys=c("campaign_no")),
        list(shortName="constituencies",	longName="TX_CONST_CUST",	baseTable="TX_CONST_CUST",	primaryKeys=c("constituency",	"customer_no")),
        list(shortName="contributions",	longName="BI.VT_CONTRIBUTION",	baseTable="T_CONTRIBUTION",	primaryKeys=c("ref_no")),
        list(shortName="creditees",	longName="T_CREDITEE",	baseTable="T_CREDITEE",	primaryKeys=c("creditee_no",	"ref_no")),
        list(shortName="customers",	longName="BI.VT_CUSTOMER",	baseTable="T_CUSTOMER",	primaryKeys=c("customer_no")),
        list(shortName="facility",	longName="BI.VT_FACILITY",	baseTable="T_FACILITY",	primaryKeys=c("facil_no")),
        list(shortName="fee",	longName="BI.VT_FEE",	baseTable="T_FEE",	primaryKeys=c("fee_no")),
        list(shortName="feeDetail",	longName="BI.VT_FEE_DETAIL",	baseTable="T_SLI_FEE",	primaryKeys=c("id")),
        list(shortName="funds",	longName="BI.VT_FUND",	baseTable="T_FUND",	primaryKeys=c("fund_no")),
        list(shortName="holds",	longName="T_HC",	baseTable="T_HC",	primaryKeys=c("hc_no")),
        list(shortName="iwave",	longName="LT_IWAVE_CUSTOMER_SCORE_DATA",	baseTable="LT_IWAVE_CUSTOMER_SCORE_DATA",	primaryKeys=c("Id")),
        list(shortName="listContents",	longName="T_LIST_CONTENTS",	baseTable="T_LIST_CONTENTS",	primaryKeys=c("customer_no",	"list_no")),
        list(shortName="lists",	longName="T_LIST",	baseTable="T_LIST",	primaryKeys=c("list_no")),
        list(shortName="memberships",	longName="BI.VT_MEMBERSHIP",	baseTable="TX_CUST_MEMBERSHIP",	primaryKeys=c("cust_memb_no")),
        list(shortName="orders",	longName="BI.VT_ORDER",	baseTable="T_ORDER",	primaryKeys=c("order_no")),
        list(shortName="orderDetail",	longName="BI.VT_ORDER_DETAIL_AT_PRICE_LAYER",	baseTable="T_LINEITEM",	primaryKeys=c("li_seq_no")),
        list(shortName="payments",	longName="BI.VT_PAYMENT",	baseTable="T_PAYMENT",	primaryKeys=c("payment_no",	"sequence_no")),
        list(shortName="performances",	longName="BI.VT_PERFORMANCE_DETAIL",	baseTable="",	primaryKeys=c("")),
        list(shortName="performanceKeywords",	longName="BI.VT_DW_PERFORMANCE_KEYWORDS",	baseTable="",	primaryKeys=c("")),
        list(shortName="plans",	longName="BI.VT_PLAN",	baseTable="T_PLAN",	primaryKeys=c("plan_no")),
        list(shortName="planStatus",	longName="TR_PLAN_STATUS",	baseTable="TR_PLAN_STATUS",	primaryKeys=c("id")),
        list(shortName="planWorkers",	longName="BI.VT_PLAN_WORKER",	baseTable="TX_CUST_PLAN",	primaryKeys=c("id")),
        list(shortName="pricing",	longName="BI.VT_PERFORMANCE_PRICING_AT_PRICE_LAYER",	baseTable="T_PERF_PRICE_TYPE",	primaryKeys=c("id")),
        list(shortName="promotions",	longName="T_PROMOTION",	baseTable="T_PROMOTION",	primaryKeys=c("customer_no",   "source_no")),
        list(shortName="promotionResponses",	longName="T_EPROMOTION_RESPONSE_HISTORY",	baseTable="T_EPROMOTION_RESPONSE_HISTORY",	primaryKeys=c("ID")),
        list(shortName="seasons",	longName="BI.VT_SEASON",	baseTable="TR_SEASON",	primaryKeys=c("id")),
        list(shortName="seats",	longName="BI.VT_PERFORMANCE_SEATING",	baseTable="TX_PERF_SEAT",	primaryKeys=c("perf_no",	"pkg_no",	"seat_no")),
        list(shortName="seatHistory",	longName="T_ORDER_SEAT_HIST",	baseTable="T_ORDER_SEAT_HIST",	primaryKeys=c("")),
        list(shortName="sources",	longName="BI.VT_SOURCE",	baseTable="TX_APPEAL_MEDIA_TYPE",	primaryKeys=c("source_no")),
        list(shortName="specialActivities",	longName="BI.VT_SPECIAL_ACTIVITY",	baseTable="T_SPECIAL_ACTIVITY",	primaryKeys=c("sa_no")),
        list(shortName="steps",	longName="BI.VT_STEP",	baseTable="T_STEP",	primaryKeys=c("step_no")),
        list(shortName="stepTypes",	longName="TR_STEP_TYPE",	baseTable="TR_STEP_TYPE",	primaryKeys=c("id")),
        list(shortName="subscriptions",	longName="BI.VT_DW_SUBSCRIPTION_SUMMARY",	baseTable="T_CUST_SUBSCRIPTION_SUMMARY",	primaryKeys=c("customer_no",	"season")),
        list(shortName="tickets",	longName="BI.VT_TICKET_HISTORY",	baseTable="T_TICKET_HISTORY",	primaryKeys=c("tck_hist_no")),
        list(shortName="transactions",	longName="T_TRANSACTION",	baseTable="T_TRANSACTION",	primaryKeys=c("sequence_no")),
        list(shortName="transactionTypes",	longName="TR_TRANSACTION_TYPE",	baseTable="TR_TRANSACTION_TYPE",	primaryKeys=c("id"))
))

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
#' @return named list of tables
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
    longName = tessiTables[shortName==tableName,longName]
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

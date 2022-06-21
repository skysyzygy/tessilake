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
    rbindlist(idcol = "short_name") %>%
    lapply(function(.){gsub("^$",NA,.)}) %>% setDT

}


#' read_tessi
#'
#' Thin wrapper on read_sql_table using the tables configured in `list_tessi_tables` that additionally updates `customer_no` and adds
#' `group_customer_no` based on [read_tessi_customer_no_map()]
#'
#' @param table_name character name of the table to read from Tessitura, either one of the available tables (see [tessi_list_tables()]) or the
#' name of a SQL table that exists in Tessitura. The default SQL table schema is `dbo`.
#' @param select vector of strings indicating columns to select from database
#' @param freshness the returned data will be at least this fresh
#' @param ... further arguments to be passed to other methods
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

  short_name <- customer_no <- kept_customer_no <- NULL

  select <- enquo(select)
  assert_character(table_name)
  assert_names(table_name, subset.of=tessi_list_tables()$short_name)

  table_data = tessi_list_tables()[short_name==table_name] %>% as.list

  table_data$long_name = strsplit(table_data$long_name,".",fixed = TRUE)[[1]]

  if(length(table_data$long_name)==1) {
    table <- read_sql_table(table_name = table_data$long_name[[1]],
                          primary_keys=table_data$primary_keys, freshness = freshness)
  } else {
    table <- read_sql_table(table_name = table_data$long_name[[2]], schema = table_data$long_name[[1]],
                          primary_keys=table_data$primary_keys, freshness = freshness)
  }

  if("customer_no" %in% names(table))
    table <- table %>%
      left_join(read_tessi_customer_no_map(),by="customer_no") %>%
      select(-customer_no) %>%
      rename(customer_no = kept_customer_no) %>%
      collect(as_data_frame = FALSE)

  return(table)

}

#' read_tessi_customer_no_map
#'
#' Return an Arrow table of customer numbers `customer_no_old` mapped to merged `customer_no` and household/primary group
#' customer numbers `group_customer_no`.
#'
#' @param freshness the returned data will be at least this fresh
#' @importFrom dplyr full_join left_join filter select mutate collect rename coalesce
#' @importFrom arrow is_in
#' @return [arrow::Table]
#' @export
read_tessi_customer_no_map <- function(freshness = as.difftime(7, units = "days")) {

  kept_id <- kept_id.old <- kept_customer_no <- customer_no <- group_customer_no <- NULL

  customers = read_sql("select customer_no from T_CUSTOMER", freshness = freshness)

  merges = read_sql("select kept_id, delete_id from T_MERGED where status='S' and kept_id<>delete_id", freshness = freshness) %>%
    distinct %>% collect(as_data_frame = FALSE)

  affiliations = read_sql("select individual_customer_no, group_customer_no from T_AFFILIATION where primary_ind='Y' and inactive='N'",
                                freshness = freshness)

  merge_recursive = function(m) {
    if(!any(m$kept_id %>% is_in(m$delete_id))$as_vector()) {
      return(m)
    } else {
      m %>%
        rename(kept_id.old = kept_id) %>%
        left_join(merges,by=c("kept_id.old"="delete_id")) %>%
        mutate(kept_id = coalesce(kept_id,kept_id.old)) %>%
        select(-kept_id.old) %>%
        collect(as_data_frame = FALSE) %>%
        merge_recursive
    }
  }

  map = customers %>%
    full_join(merge_recursive(merges),by=c("customer_no"="delete_id")) %>%
    rename(kept_customer_no = kept_id) %>%
    mutate(kept_customer_no = coalesce(kept_customer_no,customer_no)) %>%
    left_join(affiliations,by=c("kept_customer_no"="individual_customer_no")) %>%
    mutate(group_customer_no = coalesce(group_customer_no,kept_customer_no)) %>%
    collect(as_data_frame = FALSE)

}

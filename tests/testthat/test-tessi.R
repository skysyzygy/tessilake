withr::local_package("checkmate")
withr::local_package("mockery")
local_cache_dirs()

# tessi_list_tables -------------------------------------------------------

test_that("tessi_list_tables combines tessi_tables.yml with config.yml", {
  config_mock <- mock(config::get(), list(a = list(b = "c")))
  yaml_mock <- mock(list(a = list(b = "c")))

  stub(tessi_list_tables, "read_yaml", list())
  stub(tessi_list_tables, "config::get", config_mock)
  expect_equal(tessi_list_tables(), data.table(short_name = "a", b = "c"))
  expect_equal(mock_args(config_mock)[[2]], list(config = "tessi_tables"))

  rm(tessi_list_tables)
  stub(tessi_list_tables, "read_yaml", yaml_mock)
  stub(tessi_list_tables, "config::get", list())
  expect_equal(tessi_list_tables(), data.table(short_name = "a", b = "c"))
  expect_equal(mock_args(yaml_mock)[[1]], list(system.file("extdata", "tessi_tables.yml", package = "tessilake")))
})

test_that("tessi_tables.yml is configured correctly for Tessitura tables without primary keys", {
  db$db <- DBI::dbConnect(RSQLite::SQLite(),":memory:")
  available_columns <- readRDS(test_path("available_columns.Rds"))
  pk_table <- readRDS(test_path("pk_table.Rds"))
  # stub list of tables
  stub(read_sql_table,"dbListTables",mock(filter(available_columns,TABLE_SCHEMA=="dbo")$TABLE_NAME %>% unique,
                                       filter(available_columns,TABLE_SCHEMA=="BI")$TABLE_NAME %>% unique,
                                       cycle=TRUE),depth=2)
  # stub list of columns
  read_sql_no_pk <- mock(available_columns, pk_table, data.frame(x=1:100),cycle=T)
  stub(read_sql_table,"read_sql",read_sql_no_pk)
  stub(read_tessi,"read_sql_table",read_sql_table)

  tables <- tessi_list_tables()[is.na(primary_keys)]

  for(short_name in tables$short_name) {
    expect_silent(read_tessi(!!short_name))
  }
  sql_disconnect()
})

test_that("tessi_tables.yml is configured correctly for Tessitura tables with primary keys specified", {
  db$db <- DBI::dbConnect(RSQLite::SQLite(),":memory:")
  available_columns <- readRDS(test_path("available_columns.Rds"))
  pk_table <- readRDS(test_path("pk_table.Rds"))
  # stub list of tables
  stub(read_sql_table,"dbListTables",mock(filter(available_columns,TABLE_SCHEMA=="dbo")$TABLE_NAME %>% unique,
                                          filter(available_columns,TABLE_SCHEMA=="BI")$TABLE_NAME %>% unique,
                                          cycle=TRUE),depth=2)
  # stub list of columns
  read_sql_w_pk <- mock(available_columns, data.frame(x=1:100),cycle=T)
  stub(read_sql_table,"read_sql",read_sql_w_pk)
  stub(read_tessi,"read_sql_table",read_sql_table)

  tables <- tessi_list_tables()[!is.na(primary_keys)]

  for(short_name in tables$short_name) {
    expect_silent(read_tessi(!!short_name))
  }

  sql_disconnect()
})

# read_tessi -----------------------------------------------------------

test_that("read_tessi complains if table_name is not given", {
  expect_error(read_tessi(), "table_name")
})

test_that("read_tessi complains if asked for a table it doesn't know about", {
  m = mock(data.table(x=1:1000))
  stub(read_tessi,"read_sql_table",m)
  expect_error(read_tessi("tableThatDoesntExist"), "table_name")
  expect_silent(read_tessi("seasons", freshness = 0))
})

test_that("read_tessi passes all arguments on to read_sql_table", {
  m = mock(data.table(x=1:1000))
  stub(read_tessi,"read_sql_table",m)
  read_tessi("seasons", freshness = 0)
  expect_equal(mock_args(m)[[1]],list(table_name = "VT_SEASON", schema = "BI",
                                      primary_keys = "season_no",
                                      freshness = 0))
})

test_that("read_tessi merges with read_tessi_customer_no_map when table contains customer_no", {
  tbl = mock(data.table(customer_no=1:1000))
  map = mock(data.table(customer_no=1:1000,kept_customer_no=2:1001))
  stub(read_tessi,"read_sql_table",tbl)
  stub(read_tessi,"read_tessi_customer_no_map",map)

  expect_equal(read_tessi("seasons", freshness = 0),data.table(customer_no=2:1001))
})

# read_tessi_customer_no_map ----------------------------------------------

affiliations <- readRDS(test_path("affiliations.Rds"))
merges <- readRDS(test_path("merges.Rds"))
customers <- readRDS(test_path("customers.Rds"))
stub(read_tessi_customer_no_map,"read_sql",mock(select(customers,customer_no),
                                                merges,affiliations,cycle = TRUE))
map <- read_tessi_customer_no_map() %>% collect()

test_that("read_tessi_customer_no_map correctly maps all merges", {
  self_maps = filter(map,customer_no==kept_customer_no)
  diff_maps = filter(map,customer_no!=kept_customer_no)
  true_keeps = filter(merges,!kept_id %in% merges$delete_id)
  temp_keeps = filter(merges,kept_id %in% merges$delete_id)

  # deleted + temp_keeps <--> customer_no
  expect_true(all(merges$delete_id %in% map$customer_no))
  expect_true(!any(merges$delete_id %in% map$kept_customer_no))
  expect_true(all(temp_keeps$kept_id %in% map$customer_no))
  expect_true(!any(temp_keeps$kept_id %in% map$kept_customer_no))
  expect_true(!any(map$kept_customer_no %in% merges$delete_id))
  expect_true(!any(map$kept_customer_no %in% temp_keeps$kept_id))
  # true_keep <--> kept_customer_no
  expect_true(all(true_keeps$kept_id %in% map$kept_customer_no))
  expect_true(!any(true_keeps$kept_id %in% diff_maps$customer_no))
  expect_true(all(diff_maps$kept_customer_no %in% true_keeps$kept_id))
  expect_true(!any(diff_maps$customer_no %in% true_keeps$kept_id))
  # true_keeps in customer_no are self_maps
  expect_true(all(true_keeps$kept_id %in% self_maps$customer_no))
  expect_true(!any(true_keeps$kept_id %in% diff_maps$customer_no))
  # temp_keeps and deletes are diff_maps
  expect_true(all(diff_maps$customer_no %in% c(merges$delete_id, temp_keeps$kept_id)))
  expect_true(!any(self_maps$customer_no %in% c(merges$delete_id, temp_keeps$kept_id)))
})

test_that("read_tessi_customer_no_map puts merged customers in customer_no", {
  merged_customers = filter(customers,inactive==5)
  expect_lte(sum(merged_customers$customer_no %in% map$kept_customer_no),1)
  expect_true(all(merged_customers$customer_no %in% map$customer_no))
})

test_that("read_tessi_customer_no_map includes all customers", {
  expect_true(all(customers$customer_no %in% map$customer_no))
})

test_that("read_tessi_customer_no_map includes all affiliations in group_customer_no", {
  expect_true(all(affiliations$group_customer_no %in% map$group_customer_no))
})

test_that("read_tessi_customer_no_map includes all non-merged households in group_customer_no", {
  households = filter(customers,cust_type==13 & inactive != 5)
  expect_true(all(households$customer_no %in% map$group_customer_no))
})

test_that("read_tessi_customer_no_map doesn't duplicate customer_no", {
  expect_true(anyDuplicated(map$customer_no)==0)
})

test_that("read_tessi_customer_no_map has no nulls or nas", {
  expect_integer(map$customer_no,any.missing = FALSE)
  expect_integer(map$kept_customer_no,any.missing = FALSE)
  expect_integer(map$group_customer_no,any.missing = FALSE)
  expect_names(colnames(map),permutation.of = c("group_customer_no","kept_customer_no","customer_no"))
})

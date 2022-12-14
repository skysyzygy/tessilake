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

test_that("tessi_tables.yml is configured correctly for Tessitura tables", {
  db$db <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  available_columns <- readRDS(test_path("available_columns.Rds"))
  # stub list of tables
  stub(read_sql_table, "dbListTables", mock(
    filter(available_columns, table_schema == "dbo")$table_name %>% unique(),
    filter(available_columns, table_schema == "BI")$table_name %>% unique(),
    cycle = TRUE
  ), depth = 2)
  # stub list of columns
  read_sql <- mock(available_columns, data.frame(x = 1:100), cycle = T)
  stub(read_sql_table, "read_sql", read_sql)
  stub(read_tessi, "read_sql_table", read_sql_table)
  stub(read_tessi, "read_sql", data.frame())

  for (short_name in tessi_list_tables()$short_name) {
    expect_silent(read_tessi(!!short_name))
  }
  sql_disconnect()
})


# read_tessi -----------------------------------------------------------

test_that("read_tessi complains if table_name is not given", {
  expect_error(read_tessi(), "table_name")
})

test_that("read_tessi complains if asked for a table it doesn't know about", {
  m <- mock(data.table(x = 1:1000))
  stub(read_tessi, "read_sql_table", m)
  expect_error(read_tessi("tableThatDoesntExist"), "table_name")
  expect_silent(read_tessi("seasons", freshness = 0))
})

test_that("read_tessi passes all arguments on to read_sql_table", {
  m <- mock(data.table(x = 1:1000), cycle = TRUE)
  stub(read_tessi, "read_sql_table", m)
  read_tessi("seasons", select = "season_no", freshness = 0, incremental = TRUE)
  expect_mapequal(mock_args(m)[[1]], list(
    table_name = "VT_SEASON", schema = "BI",
    select = "season_no",
    primary_keys = "season_no",
    freshness = 0,
    incremental = TRUE
  ))
  read_tessi("audit", freshness = 0, incremental = TRUE)
  expect_mapequal(mock_args(m)[[2]], list(
    table_name = "TA_AUDIT_TRAIL",
    freshness = 0,
    incremental = TRUE
  ))
})


test_that("read_tessi calls merge_customer_no_map when table contains customer_no or creditee_no", {
  tbl <- data.table(customer_no = 1:1000)
  stub(read_tessi, "read_sql_table", tbl)
  merge_customer_no_map <- mock(tbl,cycle=T)
  stub(read_tessi, "merge_customer_no_map", merge_customer_no_map)
  read_tessi("seasons")
  tbl[,creditee_no := 1:1000]
  read_tessi("seasons")
  expect_length(mock_args(merge_customer_no_map),3)
  expect_equal(mock_args(merge_customer_no_map)[[1]][[2]],"customer_no")
  expect_equal(mock_args(merge_customer_no_map)[[3]][[2]],"creditee_no")
})

# tessi_customer_no_map ----------------------------------------------

affiliations <- readRDS(test_path("affiliations.Rds"))
merges <- readRDS(test_path("merges.Rds"))
customers <- readRDS(test_path("customers.Rds"))
stub(tessi_customer_no_map, "read_sql", mock(select(customers, customer_no),
  merges, affiliations,
  cycle = TRUE
))
map <- tessi_customer_no_map() %>% collect()

test_that("tessi_customer_no_map correctly maps all merges", {
  self_maps <- filter(map, customer_no == merged_customer_no)
  diff_maps <- filter(map, customer_no != merged_customer_no)
  true_keeps <- filter(merges, !kept_id %in% merges$delete_id)
  temp_keeps <- filter(merges, kept_id %in% merges$delete_id)

  # deleted + temp_keeps <--> customer_no
  expect_true(all(merges$delete_id %in% map$customer_no))
  expect_true(!any(merges$delete_id %in% map$merged_customer_no))
  expect_true(all(temp_keeps$kept_id %in% map$customer_no))
  expect_true(!any(temp_keeps$kept_id %in% map$merged_customer_no))
  expect_true(!any(map$merged_customer_no %in% merges$delete_id))
  expect_true(!any(map$merged_customer_no %in% temp_keeps$kept_id))
  # true_keep <--> merged_customer_no
  expect_true(all(true_keeps$kept_id %in% map$merged_customer_no))
  expect_true(!any(true_keeps$kept_id %in% diff_maps$customer_no))
  expect_true(all(diff_maps$merged_customer_no %in% true_keeps$kept_id))
  expect_true(!any(diff_maps$customer_no %in% true_keeps$kept_id))
  # true_keeps in customer_no are self_maps
  expect_true(all(true_keeps$kept_id %in% self_maps$customer_no))
  expect_true(!any(true_keeps$kept_id %in% diff_maps$customer_no))
  # temp_keeps and deletes are diff_maps
  expect_true(all(diff_maps$customer_no %in% c(merges$delete_id, temp_keeps$kept_id)))
  expect_true(!any(self_maps$customer_no %in% c(merges$delete_id, temp_keeps$kept_id)))
})

test_that("tessi_customer_no_map puts merged customers in customer_no", {
  merged_customers <- filter(customers, inactive == 5)
  expect_lte(sum(merged_customers$customer_no %in% map$merged_customer_no), 1)
  expect_true(all(merged_customers$customer_no %in% map$customer_no))
})

test_that("tessi_customer_no_map includes all customers", {
  expect_true(all(customers$customer_no %in% map$customer_no))
})

test_that("tessi_customer_no_map includes all affiliations in group_customer_no", {
  expect_true(all(affiliations$group_customer_no %in% map$group_customer_no))
})

test_that("tessi_customer_no_map includes all non-merged households in group_customer_no", {
  households <- filter(customers, cust_type == 13 & inactive != 5)
  expect_true(all(households$customer_no %in% map$group_customer_no))
})

test_that("tessi_customer_no_map doesn't duplicate customer_no", {
  expect_true(anyDuplicated(map$customer_no) == 0)
})

test_that("tessi_customer_no_map has no nulls or nas", {
  expect_integer(map$customer_no, any.missing = FALSE)
  expect_integer(map$merged_customer_no, any.missing = FALSE)
  expect_integer(map$group_customer_no, any.missing = FALSE)
  expect_names(colnames(map), permutation.of = c("group_customer_no", "merged_customer_no", "customer_no"))
})

# merge_customer_no_map ---------------------------------------------------

stub(merge_customer_no_map,"tessi_customer_no_map",tessi_customer_no_map)

test_that("merge_customer_no_map merges on named column", {
  test_table <- map %>% select(customer_no)
  expect_table <- map %>% dplyr::transmute(customer_no=merged_customer_no,group_customer_no)
  expect_equal(merge_customer_no_map(test_table,"customer_no") %>% arrange(customer_no),
                   arrange(expect_table,customer_no))
})

test_that("merge_customer_no_map merges on named column and updates names of merge table", {
  test_table <- map %>% dplyr::transmute(person_no=customer_no)
  expect_table <- map %>% dplyr::transmute(person_no=merged_customer_no,group_person_no=group_customer_no)
  expect_equal(merge_customer_no_map(test_table,"person_no") %>% arrange(person_no),
               arrange(expect_table,person_no))
})

test_that("merge_customer_no_map doesn't change a primary key",{
  test_table <- map %>% select(customer_no) %>% setattr("primary_keys","customer_no")
  expect_table <- map
  expect_equal(merge_customer_no_map(test_table,"customer_no") %>% arrange(customer_no),
               arrange(expect_table,customer_no) %>% setattr("primary_keys","customer_no"))
})

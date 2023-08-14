withr::local_package("mockery")
withr::local_package("checkmate")
local_cache_dirs()

test_that("read_tessi can read the first 100 rows from all the defined tables", {

  # only load first 100 rows
  arrange <- function(...) {
    dbplyr:::arrange.tbl_lazy(...) %>% head(100)
  }

  stub(read_sql, "arrange.tbl_lazy", arrange)
  stub(read_sql, "tbl", tbl)
  stub(read_sql_table, "read_sql", read_sql)
  stub(read_tessi, "read_sql_table", read_sql_table)
  stub(read_tessi, "read_sql", read_sql)

  name <- "audit"
  for (name in unique(tessi_list_tables()$short_name)) {
    table <- tessi_list_tables()[short_name == name]
    long_name <- table$long_name[[1]]
    date_column <- table$date_column[[1]]
    primary_keys <- table$primary_keys
    long_name <- ifelse(!grepl("\\.", long_name), paste0("dbo.", long_name), long_name)

    cli::cli_h1(name)

    x <- read_tessi(name)
    # check initial load
    expect_gte(nrow(collect(x)), 1)
    # check update
    incomplete_cache <- read_cache(long_name, "tessi") %>% head(nrow(.)-1)
    write_cache(incomplete_cache,
      long_name, "tessi",
      partition = FALSE, overwrite = TRUE
    )
    expect_equal(collect(read_tessi(!!name, freshness = 0)), setDT(collect(x)))
  }
})

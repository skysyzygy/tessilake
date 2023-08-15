withr::local_package("mockery")
withr::local_package("checkmate")
local_cache_dirs()

test_that("read_tessi can read the first 100 rows from all the defined tables", {

  # only load first 100 rows
  arrange <- function(table, ...) {
    out <- dbplyr:::arrange.tbl_lazy(table, ...)
    if(!grepl("INFORMATION_SCHEMA",table$lazy_query$x))
      out <- out %>% head(100)
    out
  }

  stub(read_sql, "arrange.tbl_lazy", arrange)
  stub(read_sql, "tbl", tbl)
  stub(read_sql_table, "read_sql", read_sql)
  stub(read_tessi, "read_sql_table", read_sql_table)
  stub(read_tessi, "read_sql", read_sql)

  names <- unique(tessi_list_tables()$short_name)
  names <- c("logins","performances","seasons","audit")
  for (name in names) {
    table <- tessi_list_tables()[short_name == name]
    long_name <- table$long_name[[1]]
    date_column <- table$date_column[[1]]
    primary_keys <- table$primary_keys
    long_name <- ifelse(!grepl("\\.", long_name), paste0("dbo.", long_name), long_name)

    cli::cli_h1(name)

    x <- read_tessi(name)
    # check initial load
    expect_gte(nrow(collect(x)), 1)
    expect_lte(nrow(collect(x)), 100)
    # check update
    incomplete_cache <- read_cache(long_name, "tessi") %>% head(nrow(.)-1)
    write_cache(incomplete_cache,
      long_name, "tessi",
      partition = FALSE, overwrite = TRUE
    )
    expect_equal(collect(read_tessi(!!name, freshness = 0)),
                 collect(x))
  }
})

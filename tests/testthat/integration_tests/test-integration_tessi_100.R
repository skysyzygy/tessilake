withr::local_package("mockery")
withr::local_package("checkmate")
local_cache_dirs()

test_that("read_tessi can read the first 100 rows from all the defined tables", {

  # only load first 100 rows
  arrange <- function(...) {
    dbplyr:::arrange.tbl_lazy(...) %>% head(100)
  }

  # only load first 100 rows
  tbl <- function(src, from, ...) {
    if (!grepl("INFORMATION_SCHEMA|from T", from, perl = TRUE) && is.null(get("primary_keys", envir = parent.frame()))) {
      return(dplyr::tbl(src, from) %>% head(100))
    }
    dplyr::tbl(src, from)
  }

  stub(read_sql, "arrange.tbl_lazy", arrange)
  stub(read_sql, "tbl", tbl)
  stub(read_sql_table, "read_sql", read_sql)
  stub(read_tessi, "read_sql_table", read_sql_table)

  name <- "creditees"
  for (name in unique(tessi_list_tables()$short_name)) {
    long_name <- tessi_list_tables()[short_name == name]$long_name[[1]]
    long_name <- ifelse(!grepl("\\.", long_name), paste0("dbo.", long_name), long_name)

    x <- read_tessi(name)
    # check initial load
    expect_gte(nrow(collect(x)), 1)
    # check update
    cache_write(cache_read(long_name, "deep", "tessi")[-1, ],
      long_name, "deep", "tessi",
      partition = FALSE, overwrite = TRUE
    )
    expect_equal(collect(read_tessi(!!name, freshness = 0)), collect(x))
  }
})

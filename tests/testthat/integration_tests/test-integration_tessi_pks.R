withr::local_package("mockery")
withr::local_package("checkmate")
local_cache_dirs()

test_that("read_tessi can read the primary keys from all the defined tables", {
  name <- "memberships"
  for (name in unique(tessi_list_tables()[!is.na(primary_keys)]$short_name)) {
    primary_keys <- tessi_list_tables()[short_name == name]$primary_keys
    long_name <- tessi_list_tables()[short_name == name]$long_name[[1]]
    long_name <- ifelse(!grepl("\\.", long_name), paste0("dbo.", long_name), long_name)

    # only load primary keys
    tbl <- function(src, from, ...) {
      if (grepl("INFORMATION_SCHEMA|from T", from, perl = TRUE)) {
        return(dplyr::tbl(src, from))
      }

      dplyr::tbl(src, from) %>%
        select(!!na.omit(c(
          primary_keys,
          ifelse(grepl("last_update_dt", from),
            "last_update_dt",
            NA
          )
        )))
    }

    stub(read_tessi, "tbl", tbl, depth = 2)

    assign(name, read_tessi(name))
    # check that all primary keys are present
    expect_names(names(get(name)), must.include = primary_keys)
    # check that we could partition if we wanted to
    expect_silent(cache_make_partitioning(get(!!name)))
    # check that primary keys are unique
    expect_equal(nrow(unique(setkeyv(setDT(collect(select(get(!!name), !!primary_keys))), primary_keys))), nrow(get(!!name)))

    rm(read_tessi)
  }
})

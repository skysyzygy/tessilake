withr::local_package("mockery")
withr::local_package("checkmate")
local_cache_dirs()

test_that("read_tessi can read the first 100 rows from all the defined tables", {

  read_sql <- function(query=NULL,primary_keys=NULL,...) {
    if(!grepl("INFORMATION_SCHEMA",query))
      query = paste(gsub("select","select top 100",query,fixed=TRUE),
                    ifelse(!is.null(primary_keys),
                           paste("order by",paste(primary_keys,sep=",")),
                           ""))
    tessilake::read_sql(query=query,primary_keys=primary_keys,...)
  }
  stub(read_sql_table,"read_sql",read_sql)
  stub(read_tessi,"read_sql_table",read_sql_table)

  name = "creditees"
  for (name in unique(tessi_list_tables()$short_name)) {
    long_name <- tessi_list_tables()[short_name == name]$long_name[[1]]
    long_name <- ifelse(!grepl("\\.",long_name),paste0("dbo.",long_name),long_name)

    x = read_tessi(name)
    # check initial load
    expect_gte(nrow(collect(x)),1)
    # check update
    cache_write(cache_read(long_name,"deep","tessi")[-1,],
                long_name,"deep","tessi",partition=FALSE,overwrite=TRUE)
    expect_equal(collect(read_tessi(!!name,freshness = 0)),collect(x))

  }

})

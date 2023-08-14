withr::local_package("mockery")
withr::local_package("checkmate")
withr::local_package("dplyr")
local_cache_dirs()
sql_connect()

tables <- tessi_list_tables()[coalesce(incremental,"TRUE")=="TRUE"]

tables[,`:=`(schema=coalesce(stringr::str_extract(long_name,"^.+?(?=\\.)"),"dbo"),
             long_name=stringr::str_remove(long_name,"^.+?\\."))]

# we use read_sql_table to construct the query as it is in real life, with column order, etc.
stub(read_sql_table,"read_sql",function(query,...) tbl(db$db, sql(query)))

test_full_load <- function(long_name,schema) {
  cli::cli_h1(long_name)
  time <- system.time(
    result <- read_sql_table(long_name,schema) %>% collect %>% setDT
  )
  cache <- write_cache(result,long_name,"tessi",partition=FALSE,overwrite=TRUE)
  cli::cli_inform("time1.0: {scales::number(summary(time),.01)}")
  list(time,result)
}

test_update <- function(long_name,schema,full_table,primary_keys,date_column,frac) {
  n <- round(nrow(full_table)*frac)
  if(n<1) return(list(sql=system.time(0),arrow=system.time(0)))
  if(is.na(primary_keys))
    primary_keys = NULL
  if(is.na(date_column)) {
    if("last_update_dt" %in% colnames(full_table)) {
      date_column = "last_update_dt"
    } else {
      date_column = NULL
    }
  }
  time_sql <- system.time(
    update_table(read_sql_table(long_name,schema),
                                primary_keys=!!primary_keys,
                                date_column=!!date_column,
                 full_table[1:(.N-n),],
                 delete = T)
  )
  cli::cli_inform("time_sql{frac}: {scales::number(summary(time_sql),.01)}")

  time_arrow <- system.time(
    update_table(read_cache(long_name,"tessi"),
                 primary_keys=!!primary_keys,
                 date_column=!!date_column,
                 full_table[1:(.N-n),],
                 delete = T)
  )
  cli::cli_inform("time_arrow{frac}: {scales::number(summary(time_arrow),.01)}")
  list(sql=time_sql,arrow=time_arrow)
}

test_load_update <- function(long_name,schema,primary_keys,date_column) {
  t <- test_full_load(long_name,schema)
  unlist(c(time1.00 = t[[1]],
      time0.10 = test_update(long_name,schema,t[[2]],primary_keys,date_column,.10),
      time0.01 = test_update(long_name,schema,t[[2]],primary_keys,date_column,.01)))
}

test_that("update_table incremental loads through update_table faster (within 1 second) in real life",{

  for(long_name in unique(tables$long_name)) {
    table = tables[eval(expr(long_name == !!long_name)),.(long_name = min(long_name),
                                                   schema = min(schema),
                                                   primary_keys = list(primary_keys),
                                                   date_column = min(date_column))]
    test_results <- test_load_update(table$long_name,table$schema,table$primary_keys[[1]],table$date_column)

    expect_lte(test_results["time0.10.sql.elapsed"],test_results["time1.00.elapsed"]+1,label=paste(long_name,"time0.10.sql.elapsed"))
    expect_lte(test_results["time0.01.sql.elapsed"],test_results["time0.10.sql.elapsed"]+1,label=paste(long_name,"time0.01.sql.elapsed"))
    expect_lte(test_results["time0.10.arrow.elapsed"],test_results["time1.00.elapsed"]+1,label=paste(long_name,"time0.10.arrow.elapsed"))
    expect_lte(test_results["time0.01.arrow.elapsed"],test_results["time0.10.arrow.elapsed"]+1,label=paste(long_name,"time0.01.arrow.elapsed"))
  }

})


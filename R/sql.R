db <- NULL

sql_connect <- function() {
  if(is.null(db))
    db <<- DBI::dbConnect(odbc::odbc(), config::get("tessilake.tessitura"), encoding="latin1")
}

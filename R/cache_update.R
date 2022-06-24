#' cache_update
#'
#' Internal function to update portions of cached arrow files.
#'
#' @param x data.frame or part of a data.frame to be cached
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param primary_keys character vector of columns to be used for partitioning, only the first one is currently used
#' @param date_column character name of the column to be used for determining the date of last row update
#' @param delete whether to delete rows in cache missing from `x`, default is not to delete the rows
#' @param ... extra arguments passed on to [`arrow::open_dataset`] and [`arrow::write_dataset`]
#'
#' @return invisible
#' @importFrom arrow open_dataset
#' @importFrom dplyr select filter all_of anti_join distinct
#' @importFrom rlang sym
#' @examples
#' \dontrun{
#' x <- data.table(a = 1:1000, b = runif(1000))
#' y <- data.table(b = 100:199, b = runif(100))
#' cache_write(x, "test", "deep", "stream")
#' cache_update(y, "test", "deep", "stream")
#' }
cache_update <- function(x, table_name, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                         primary_keys = cache_get_attributes(x)$primary_keys,
                         date_column = NULL, delete = FALSE, ...) {
  . <- NULL

  if (!cache_exists(table_name, depth, type)) {
    return(cache_write(x, table_name, depth, type, primary_keys = primary_keys))
  }

  dataset <- cache_read(table_name, depth, type, include_partition = TRUE, ...)

  assert_dataframeish(x)

  dataset_attributes <- cache_get_attributes(dataset)
  partition <- !is.null(dataset_attributes$partitioning)

  if (partition == TRUE) {
    if (is.null(primary_keys) || dataset_attributes$primary_keys != primary_keys) {
      stop(sprintf(
        "Dataset has primary keys (%s) but x's primary keys are (%s). Cowardly refusing to continue.",
        dataset_attributes$primary_keys %||% "NULL",
        primary_keys %||% "NULL"
      ))
    }

    partition_name <- paste0("partition_", dataset_attributes$primary_keys[[1]])
    x_primary_keys <- select(x, all_of(primary_keys)) %>% collect

    partitions <- eval_tidy(rlang::parse_expr(dataset_attributes$partitioning), x_primary_keys) %>% unique
    dataset_partitions = select(dataset,!!partition_name) %>% unique %>% collect %>% .[[1]]

    # load only the dataset partitions that need to get updated
    dataset <- dataset %>%
      filter(!!sym(partition_name) %in% partitions) %>%
      select(-!!partition_name)

    dataset_attributes$names <- setdiff(dataset_attributes$names, partition_name)
  }

  dataset <- dataset %>% collect()
  cache_set_attributes(dataset, dataset_attributes)
  setDT(dataset)

  x <- update_table(x, dataset, primary_keys = !!primary_keys, date_column = !!date_column, delete = delete)

  cache_write(x, table_name, depth, type, primary_keys = primary_keys, partition = partition, overwrite = TRUE, ...)

  if(delete == TRUE && partition == TRUE) {
    cache_delete(table_name,depth,type,partitions = setdiff(dataset_partitions,partitions))
  }

}

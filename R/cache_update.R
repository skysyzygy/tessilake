#' cache_update
#'
#' Internal function to update portions of cached \link[arrow:arrow-package]{arrow} files.
#'
#' @param x data.frame or part of a data.frame to be cached
#' @param table_name string
#' @param depth string, e.g. "deep" or "shallow"
#' @param type string, e.g. "tessi" or "stream"
#' @param primary_keys character vector of columns to be used for identifying rows when updating the cache
#' @param date_column character name of the column to be used for determining the date of last row update
#' @param delete whether to delete rows in cache missing from `x`, default is not to delete the rows
#' @param incremental whether or not to update the cache incrementally or to simply overwrite the existing cache, default is `TRUE`.
#' @inheritParams update_table_date_only
#' @param ... extra arguments passed on to [arrow::write_dataset]
#'
#' @return invisible
#' @importFrom arrow open_dataset
#' @importFrom dplyr select filter all_of anti_join distinct transmute semi_join
#' @importFrom rlang sym
#' @importFrom utils modifyList
#' @examples
#' \dontrun{
#' x <- data.table(a = 1:1000, b = runif(1000))
#' y <- data.table(b = 100:199, b = runif(100))
#' cache_write(x, "test", "deep", "stream")
#' cache_update(y, "test", "deep", "stream")
#' }
cache_update <- function(x, table_name, depth, type,
                         primary_keys = cache_get_attributes(x)$primary_keys,
                         date_column = NULL, delete = FALSE, incremental = TRUE,
                         prefer = "to", ...) {
  . <- NULL

  if (!cache_exists(table_name, depth, type)) {
    return(cache_write(x, table_name, depth, type, primary_keys = primary_keys, ...))
  }

  dataset <- cache_read(table_name, depth, type, include_partition = TRUE)

  assert_dataframeish(x)

  dataset_attributes <- cache_get_attributes(dataset)
  partitioning <- dataset_attributes$partitioning

  # enforce primary key alignment with underlying dataset if primary keys aren't given
  primary_keys = primary_keys %||% cache_get_attributes(x)$primary_keys
  if (!setequal(dataset_attributes$primary_keys, primary_keys)) {
    stop(sprintf(
      "Dataset has primary keys (%s) but x's primary keys are (%s). Cowardly refusing to continue.",
      dataset_attributes$primary_keys %||% "NULL",
      primary_keys %||% "NULL"
    ))
  }

  if (!is.null(partitioning)) {
    partition_name <- paste0("partition_", dataset_attributes$partition_key)
    partition_key <- dataset_attributes$partition_key

    x_partitions <- select(x, all_of(partition_key)) %>%
      transmute(!!partition_name := !!rlang::parse_expr(partitioning)) %>%
      unique() %>%
      collect() %>%
      .[[1]]

    dataset_partitions <- select(dataset, !!partition_name) %>%
      unique() %>%
      collect() %>%
      .[[1]]

    # load only the dataset partitions that need to get updated
    dataset <- dataset %>%
      filter(!!rlang::sym(partition_name) %in% x_partitions) %>%
      select(-!!partition_name)

    dataset_attributes$names <- setdiff(dataset_attributes$names, partition_name)
  }

  dataset <- dataset %>% collect()
  cache_set_attributes(dataset, dataset_attributes)
  setDT(dataset)

  x <- update_table(x, dataset, primary_keys = !!primary_keys, date_column = !!date_column, delete = delete, incremental = incremental)

  partition = partitioning
  if(!is.null(partitioning) && !is.null(primary_keys)) {
    partition = TRUE
  }

  args <- modifyList(rlang::list2(...),
                     list(x = x, table_name = table_name, depth = depth, type = type,
                          primary_keys = primary_keys, partition = partition,
                          overwrite = TRUE))

  do.call(cache_write, args)

  if (delete == TRUE && !is.null(partitioning)) {
    cache_delete(table_name, depth, type,
                 partitions = setdiff(dataset_partitions, x_partitions))
  }
}

#' cache_path
#'
#' Internal function to build cache directory/path and check that we have read/write access
#' `tessilake.{depth}/{type}/{table_name}`
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#'
#' @return string for the configured cache path
#' @importFrom checkmate assert_character assert_choice test_character test_directory
#' @examples
#' \dontrun{
#' cache_path("test","deep","stream")
#' }
cache_path <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream")) {
  assert_character(table_name)
  assert_choice(depth, c("deep", "shallow"))
  assert_choice(type, c("tessi", "stream"))

  cache_root <- config::get(paste0("tessilake.", depth))

  file.create(file.path(cache_root,"test"),showWarnings = FALSE)
  if (!test_character(cache_root) || !file.exists(file.path(cache_root,"test"))) {
    stop(paste0("Please set the tessilake.", depth, " option in config.yml to point to a cache path where you have read/write access."))
  }
  file.remove(file.path(cache_root,"test"))

  # build the cache query with arrow
  cache_path <- file.path(cache_root, type, table_name)

  cache_path
}

#' cache_read
#'
#' Internal function to read cached arrow files. Simple wrapper on open_dataset.
#' Optionally returns the partition information as a dataset column.
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param include_partition boolean, whether or not to return the partition information as a column
#' @param ... extra arguments to pass on to arrow::open_dataset
#'
#' @return [`arrow::Dataset`]
#' @importFrom arrow open_dataset read_feather read_parquet
#' @examples
#' \dontrun{
#' cache_read("test","deep","stream")
#' }
cache_read <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                       include_partition = FALSE, ...) {

  cache_path <- cache_path(table_name, depth, type)

  if(dir.exists(cache_path)) {
    cache <- open_dataset(cache_path, format = ifelse(depth == "deep", "parquet", "arrow"), ...)

    attributes = cache_get_attributes(cache)
    if(!is.null(attributes$partitioning)) {
      partition_name = paste0("partition_",attributes$primary_keys)

      if (partition_name %in% names(cache) && include_partition == FALSE) {
        cache <- select(cache, -!!partition_name)
      }
    }

  } else if(file.exists(paste0(cache_path,".feather"))) {
    cache <- read_feather(paste0(cache_path,".feather"),as_data_frame=F)
  } else if(file.exists(paste0(cache_path,".parquet"))) {
    cache <- read_parquet(paste0(cache_path,".parquet"),as_data_frame=F)
  } else {
    message(paste("Cache file not found at",cache_path))
    cache <- FALSE
  }

  cache
}

#' cache_write
#'
#' Internal function to write cached arrow files. Simple wrapper on write_dataset that points to a directory defined by
#' `tessilake.{depth}/{type}` and uses partitioning specified by primary_keys attribute/argument.
#'
#' @param x data.frame to be written
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param primary_keys character vector of columns to be used for partitioning, only the first one is currently used
#' @param partition boolean, whether or not to partition the dataset using primary_keys information
#' @param ... extra arguments passed on to [`arrow::write_dataset`]
#'
#' @return invisible
#' @importFrom arrow write_dataset write_parquet write_feather
#' @importFrom dplyr select mutate
#' @importFrom data.table setattr
#' @importFrom rlang is_symbol as_name env_has
#' @examples
#' \dontrun{
#' x = data.table(a=c(1,2,3))
#' cache_write(x,"test","deep","stream",primary_keys=c("a"))
#' }
cache_write <- function(x, table_name, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                        primary_keys = attr(x, "primary_keys"),
                        partition = !is.null(primary_keys), ...) {

  cache_path <- cache_path(table_name, depth, type)
  assert_dataframeish(x)

  attributes <- cache_get_attributes(x)
  primary_keys <- primary_keys %||% attributes$primary_keys

  if(partition == TRUE) {
    if(is.null(primary_keys)) {
      stop("Cannot partition without primary key information.")
    }

    partitioning <- cache_make_partitioning(x, primary_keys = primary_keys)
    partition_name <- paste0("partition_",primary_keys[[1]])

    if (inherits(x, "data.table")) {
      x[, (partition_name) := eval(partitioning)]
    } else {
      x <- mutate(x, !!partition_name := eval(partitioning)) %>% collect
    }

    if(!dir.exists(cache_path)) dir.create(cache_path,recursive = T)

    attributes$partitioning = rlang::expr_deparse(partitioning)
    attributes$primary_keys = primary_keys
    cache_set_attributes(x,attributes)

    write_dataset(x, cache_path,
      format = ifelse(depth == "deep", "parquet", "feather"),
      partitioning = partition_name, ...
    )

    if (inherits(x, "data.table")) x[, (partition_name) := NULL]

  } else {
    if(!dir.exists(dirname(cache_path))) dir.create(dirname(cache_path))
    attributes$primary_keys = primary_keys
    cache_set_attributes(x,attributes)

    writer = ifelse(depth=="deep",write_parquet,write_feather)
    writer(x, paste0(cache_path,".",ifelse(depth == "deep", "parquet", "feather")), ...)
  }

  invisible()
}

#' cache_update
#'
#' Internal function to update portions of cached arrow files.
#'
#' @param x data.frame or part of a data.frame to be cached
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param primary_keys character vector of columns to be used for partitioning, only the first one is currently used
#' @param ... extra arguments passed on to [`arrow::open_dataset`] and [`arrow::write_dataset`]
#'
#' @return invisible
#' @importFrom arrow open_dataset
#' @importFrom dplyr select filter_at
#' @importFrom rlang sym
#' @examples
#' \dontrun{
#' x = data.table(a=1:1000,b=runif(1000))
#' y = data.table(b=100:199,b=runif(100))
#' cache_write(x,"test","deep","stream")
#' cache_update(y,"test","deep","stream")
#' }
cache_update <- function(x, table_name, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                         primary_keys = attr(x, "primary_keys"),
                         date_column = NULL, ...) {

  dataset <- cache_read(table_name, depth, type, include_partition = T, ...)
  if(is.logical(dataset))
    return(cache_write(x,table_name,depth,type,primary_keys=primary_keys))

  assert_dataframeish(x)

  x_attributes <- cache_get_attributes(x)
  dataset_attributes <- cache_get_attributes(dataset)
  primary_keys = primary_keys %||% x_attributes$primary_keys
  partition = !is.null(dataset_attributes$partitioning)

  if (partition == TRUE) {
    if (is.null(primary_keys) || dataset_attributes$primary_keys != primary_keys) {
      stop(sprintf(
        "Dataset has primary keys (%s) but x's primary keys are (%s). Cowardly refusing to continue.",
        dataset_attributes$primary_keys %||% "NULL",
        primary_keys %||% "NULL"
      ))
    }

    partition_name <- paste0("partition_",dataset_attributes$primary_keys[[1]])
    x_primary_keys <- select(x,primary_keys) %>% collect()
    partitions <- eval_tidy(rlang::parse_expr(dataset_attributes$partitioning), x_primary_keys)

    # load only the dataset partitions that need to get updated
    dataset <- dataset %>%
      filter(!!sym(partition_name) %in% partitions) %>%
      select(-!!partition_name)
  }

  dataset <- dataset %>% collect()
  cache_set_attributes(dataset,dataset_attributes)
  setDT(dataset)

  x <- update_table(x, dataset, primary_keys = !!primary_keys, date_column = !!date_column)

  cache_write(x, table_name, depth, type, primary_keys = primary_keys, partition = partition, ...)
}

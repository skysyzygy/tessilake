
#' cache_read
#'
#' Internal function to read cached arrow files. Simple wrapper on open_dataset.
#' Optionally returns the partition information as a dataset column.
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param include_partition boolean, whether or not to return the partition information as a column
#' @param select vector of strings indicating columns to select from database
#' @param ... extra arguments to pass on to arrow::open_dataset
#'
#' @return [`arrow::Dataset`]
#' @importFrom arrow open_dataset read_feather read_parquet
#' @examples
#' \dontrun{
#' cache_read("test", "deep", "stream")
#' }
cache_read <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                       include_partition = FALSE, select = NULL, ...) {
  cache_path <- cache_path(table_name, depth, type)

  if (dir.exists(cache_path)) {
    cache <- open_dataset(cache_path, format = ifelse(depth == "deep", "parquet", "arrow"), ...)

    if(!is.null(select)) {
      assert_names(select,subset.of=colnames(cache))
      cache = select(cache,!!select)
    }

    attributes <- cache_get_attributes(cache)

    if (!is.null(attributes$partitioning)) {
      partition_name <- paste0("partition_", attributes$primary_keys)

      if (partition_name %in% names(cache) && include_partition == FALSE) {
        cache <- select(cache, -!!partition_name)
      }
    }
  } else if (file.exists(paste0(cache_path, ".feather"))) {
    cache <- read_feather(paste0(cache_path, ".feather"), as_data_frame = F, col_select = !!select, ...)
  } else if (file.exists(paste0(cache_path, ".parquet"))) {
    cache <- read_parquet(paste0(cache_path, ".parquet"), as_data_frame = F, col_select = !!select, ...)
  } else {
    message(paste("Cache file not found at", cache_path))
    cache <- FALSE
  }

  # repair colnames in r attributes
  #cache_set_attributes(cache,list(names=NULL))
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
#' @param overwrite boolean, whether or not to overwrite an existing cache
#' @param ... extra arguments passed on to [`arrow::write_dataset`]
#'
#' @return invisible
#' @importFrom arrow write_dataset write_parquet write_feather
#' @importFrom dplyr select mutate
#' @importFrom data.table setattr
#' @importFrom rlang is_symbol as_name env_has
#' @examples
#' \dontrun{
#' x <- data.table(a = c(1, 2, 3))
#' cache_write(x, "test", "deep", "stream", primary_keys = c("a"))
#' }
cache_write <- function(x, table_name, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                        primary_keys = cache_get_attributes(x)$primary_keys,
                        partition = !is.null(primary_keys), overwrite = FALSE, ...) {
  if (cache_exists(table_name, depth, type) == TRUE && overwrite == FALSE) {
    stop("Cache already exists, and overwrite is not TRUE")
  }

  cache_path <- cache_path(table_name, depth, type)

  assert_dataframeish(x)

  attributes <- cache_get_attributes(x)
  attributes_old <- attributes

  if (partition == TRUE) {
    if (is.null(primary_keys)) {
      stop("Cannot partition without primary key information.")
    }

    partitioning <- cache_make_partitioning(x, primary_keys = primary_keys)
    partition_name <- paste0("partition_", primary_keys[[1]])

    if (inherits(x, "data.table")) {
      x[, (partition_name) := eval(partitioning)]
    } else {
      x <- mutate(x, !!partition_name := eval(partitioning)) %>% collect()
    }

    if (!dir.exists(cache_path)) dir.create(cache_path, recursive = T)

    attributes$partitioning <- rlang::expr_deparse(partitioning)
    attributes$primary_keys <- primary_keys
    cache_set_attributes(x, attributes)

    write_dataset(x, cache_path,
      format = ifelse(depth == "deep", "parquet", "feather"),
      partitioning = partition_name, ...
    )

    if (inherits(x, "data.table")) x[, (partition_name) := NULL]
  } else {
    if (!dir.exists(dirname(cache_path))) dir.create(dirname(cache_path))
    attributes$primary_keys <- primary_keys
    cache_set_attributes(x, attributes)

    writer <- ifelse(depth == "deep", write_parquet, write_feather)
    writer(x, paste0(cache_path, ".", ifelse(depth == "deep", "parquet", "feather")), ...)
  }

  # restore the old attributes so we don't have side-effects on x if x is a data.table
  if (is.data.frame(x)) {
    for (name in names(attributes)) {
      setattr(x, name, attributes_old[[name]])
    }
  }

  invisible()
}

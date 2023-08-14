
#' read_cache
#'
#' Function to read cached arrow files. Simple wrapper on open_dataset.
#' Optionally returns the partition information as a dataset column.
#'
#' @param table_name string
#' @param depth string, e.g. "deep" or "shallow"
#' @param type string, e.g. "tessi" or "stream"
#' @param include_partition boolean, whether or not to return the partition information as a column
#' @param select vector of strings indicating columns to select from database
#' @param ... extra arguments to pass on to arrow::open_dataset
#' @param num_tries integer number of times to try reading before failing
#'
#' @return [`arrow::Dataset`]
#' @importFrom arrow open_dataset read_feather read_parquet
#' @export
#' @examples
#' \dontrun{
#' read_cache("test", "deep", "stream")
#' }
read_cache <- cache_read <- function(table_name, depth, type,
                       include_partition = FALSE, select = NULL, num_tries = 60, ...) {
  cache_path <- cache_path(table_name, depth, type)

  if (dir.exists(cache_path)) {
    cache <- open_dataset(cache_path, format = coalesce(config::get("tessilake")$depths[[depth]]$format,"parquet"), ...)

    if (!is.null(select)) {
      assert_names(select, subset.of = colnames(cache))
      cache <- select(cache, !!select)
    }

    attributes <- cache_get_attributes(cache)

    if (!is.null(attributes$partitioning)) {
      partition_name <- paste0("partition_", attributes$primary_keys)

      if (partition_name %in% names(cache) && include_partition == FALSE) {
        cache <- select(cache, -!!partition_name)
      }
    }
  } else if (file.exists(paste0(cache_path, ".feather"))) {
    cache_reader <- read_feather
    cache_file <- paste0(cache_path, ".feather")
  } else if (file.exists(paste0(cache_path, ".parquet"))) {
    cache_reader <- read_parquet
    cache_file <- paste0(cache_path, ".parquet")
  } else {
    cache_reader <- \(...) stop("Cache does not exist")
  }

  while(!exists("cache") && num_tries > 0) {
    last_error <- tryCatch(cache <- cache_reader(cache_file, as_data_frame = F, col_select = !!select),
                           error = force)
    if(exists("cache"))
      break
    num_tries <- num_tries - 1
    Sys.sleep(1)
  }

  if(!exists("cache")) {
    rlang::warn(c(paste("Couldn't read cache at", cache_path),
                  "*" = last_error$message))
    return(FALSE)
  }

  cache
}

#' write_cache
#'
#' Function to write cached arrow files. Simple wrapper on write_dataset that points to a directory defined by
#' `tessilake.{depth}/{type}` and uses partitioning specified by primary_keys attribute/argument.
#'
#' @param x data.frame to be written
#' @param table_name string
#' @param depth string, e.g. "deep" or "shallow"
#' @param type string, e.g. "tessi" or "stream"
#' @param primary_keys character vector of columns to be used for partitioning, only the first one is currently used
#' @param num_tries integer number of times to try reading before failing
#' @param partition boolean, whether or not to partition the dataset using primary_keys information
#' @param overwrite boolean, whether or not to overwrite an existing cache
#' @param ... extra arguments passed on to [arrow::write_feather], [arrow::write_parquet] or [arrow::write_dataset]
#'
#' @return invisible
#' @importFrom arrow write_dataset write_parquet write_feather
#' @importFrom dplyr select mutate
#' @importFrom data.table setattr
#' @importFrom rlang is_symbol as_name env_has
#' @export
#' @examples
#' \dontrun{
#' x <- data.table(a = c(1, 2, 3))
#' write_cache(x, "test", "deep", "stream", primary_keys = c("a"))
#' }
write_cache <- cache_write <- function(x, table_name, depth, type,
                        primary_keys = cache_get_attributes(x)$primary_keys,
                        partition = !is.null(primary_keys), overwrite = FALSE,
                        num_tries = 60, ...) {
  if (cache_exists(table_name, depth, type) == TRUE && overwrite == FALSE) {
    stop("Cache already exists, and overwrite is not TRUE")
  }

  cache_path <- cache_path(table_name, depth, type)

  assert_dataframeish(x)

  attributes <- cache_get_attributes(x)
  attributes_old <- attributes

  format <- coalesce(config::get("tessilake")$depths[[depth]]$format,"parquet")

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

    cache_writer <- write_dataset
    args <- list(dataset = x,
                  path = cache_path,
                  format = format,
                  partitioning = partition_name)

  } else {
    if (!dir.exists(dirname(cache_path))) dir.create(dirname(cache_path))
    attributes$primary_keys <- primary_keys
    cache_set_attributes(x, attributes)

    cache_writer <- switch(format,
                     feather = write_feather,
                     write_parquet)

    args <- list(x = x,
                 sink = paste0(cache_path, ".", format))
  }

  args <- modifyList(rlang::list2(...),args)

  while(!exists("cache") && num_tries > 0) {
    last_error <- tryCatch(cache <- do.call(cache_writer, args),
                           error = force)
    if(exists("cache"))
      break
    num_tries <- num_tries - 1
    Sys.sleep(1)
  }

  if(!exists("cache")) {
    rlang::warn(c(paste("Couldn't write cache at", cache_path),
                  "*" = last_error$message))
  }

  # restore the old attributes so we don't have side-effects on x if x is a data.table
  if (is.data.frame(x)) {
    for (name in names(attributes)) {
      setattr(x, name, attributes_old[[name]])
    }
    if (inherits(x, "data.table") & exists("partition_name"))
      x[, (partition_name) := NULL]
  }

  invisible()
}



#' read_cache
#'
#' Function to read cached \link[arrow:arrow-package]{arrow} files. Reads from the most recently updated cache across all storage depths.
#' Optionally returns the partition information as a dataset column.
#'
#' @param table_name string
#' @param depth string, e.g. "deep" or "shallow", deprecated in [read_cache]
#' @param type string, e.g. "tessi" or "stream"
#' @param include_partition boolean, whether or not to return the partition information as a column
#' @param select vector of strings indicating columns to select from database
#' @param ... extra arguments to pass on to [cache_read] and then [arrow::open_dataset], [arrow::read_parquet], or [arrow::read_feather]
#' @param num_tries integer number of times to try reading before failing
#'
#' @return [`arrow::Dataset`]
#' @importFrom checkmate assert_character
#' @importFrom lifecycle deprecated deprecate_warn
#' @export
#' @examples
#' \dontrun{
#' write_cache(data.table(a=letters), "test", "stream")
#' read_cache("test", "stream")
#' }
read_cache <- function(table_name, type, depth = deprecated(), ...) {
  assert_character(table_name, len = 1)
  assert_character(type, len = 1)

  if(lifecycle::is_present(depth) | type %in% names(config::get("tessilake")$depths)) {
    deprecate_warn("0.4.0", "tessilake::read_cache(depth)",
                    details = "The depth argument is no longer needed, reads are made from the most-recently updated storage.",
                    always = TRUE)
  }

  depths <- names(config::get("tessilake")[["depths"]])
  mtimes <- purrr::map_vec(depths, \(depth) cache_get_mtime(table_name, depth, type))

  cache_read(
    table_name = table_name,
    depth = depths[order(mtimes, decreasing = TRUE)[1]],
    type = type,
    ...
  )

}

#' @describeIn read_cache Underlying cache reader that invokes [arrow::open_dataset], [arrow::read_parquet], or [arrow::read_feather]
#' and handles partitioning information
#' @importFrom arrow open_dataset read_feather read_parquet
cache_read <- function(table_name, depth, type,
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
#' Function to write cached \link[arrow:arrow-package]{arrow} files. Writes to the first defined storage in `config::get("tessilake")`,
#' and then syncs to the other locations by calling [sync_cache].
#'
#' @param x data.frame to be written
#' @param table_name string
#' @param incremental boolean, whether to call [cache_update] or [cache_write] to update the cached dataset.
#' @param depth string, e.g. "deep" or "shallow", deprecated in [write_cache]
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
#' @importFrom lifecycle deprecated deprecate_warn
#' @export
#' @examples
#' \dontrun{
#' x <- data.table(a = c(1, 2, 3))
#' write_cache(x, "test", "stream", primary_keys = c("a"))
#' }
#' @importFrom utils modifyList
write_cache <- function(x, table_name, type,
                        depth = deprecated(),
                        incremental = FALSE, ...) {
  assert_dataframeish(x)
  assert_character(table_name, len = 1)
  assert_character(type, len = 1)
  depths <- names(config::get("tessilake")[["depths"]])

  if(lifecycle::is_present(depth) | type %in% names(config::get("tessilake")$depths)) {
    lifecycle::deprecate_warn("0.4.0", "tessilake::write_cache(depth)",
                               details = "The depth argument is no longer needed, writes are to the primary storage and then synced across all storages.",
                               always = TRUE)
  }

  args <- modifyList(rlang::list2(...),
                     list(x = x, table_name = table_name, depth = depths[1], type = type))

  if(incremental) {
    do.call(cache_update, args)
  } else {
    do.call(cache_write, args)
  }

  sync_cache(table_name = table_name, type = type, incremental = incremental, ...)

  cache_path <- cache_path(table_name = table_name, depth = depths[1], type = type)
  cache_files <- c(cache_path, dir(dirname(cache_path),
                                           pattern = paste0(table_name, "\\..+"),
                                           full.names = TRUE, recursive = TRUE))

  if(length(cache_files) > 0)
    system2("touch",cache_files)

}


#' @describeIn write_cache Underlying cache writer that invokes [arrow::write_feather], [arrow::write_parquet] or [arrow::write_dataset] and handles partitioning
#' specified by `primary_keys` attribute/argument.
#' @importFrom utils modifyList
cache_write <- function(x, table_name, depth, type,
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

#' sync_cache
#'
#' Syncs cached files across storage depths by incrementally updating using [cache_update] or simply copying and converting
#' the most recently-modified cache to the other locations using [cache_read] and [cache_write].
#'
#' @param table_name string
#' @param type string, e.g. "tessi" or "stream"
#' @param incremental boolean, whether to incrementally update the caches by using [cache_update] or simply copying the whole file.
#' @param date_column character name of the column to be used for determining the date of last row update, useful for incremental updates.
#' @param ... additional parameters passed on to [cache_update]
#' @return invisible
#' @importFrom checkmate assert_character
#' @export
#' @examples
#' \dontrun{
#' x <- data.table(a = c(1, 2, 3))
#' write_cache(x, "test", "stream", primary_keys = c("a"))
#' sync_cache("test", "stream")
#' }
sync_cache <- function(table_name, type, incremental = FALSE, date_column = NULL, ...) {
  assert_character(table_name, len = 1)
  assert_character(type, len = 1)

  depths <- names(config::get("tessilake")[["depths"]])
  mtimes <- purrr::map_vec(depths, \(depth) cache_get_mtime(table_name, depth, type))
  depths <- depths[order(mtimes, decreasing = TRUE)]

  for(index in seq(2,length(depths))) {
    table <- cache_read(table_name = table_name, depth = depths[index-1], type = type)
    if(incremental) {
      cache_update(table,
                   table_name = table_name, depth = depths[index], type = type,
                   delete = TRUE, date_column = date_column, ...)
    } else {
      cache_write(table,
                  table_name = table_name, depth = depths[index], type = type,
                  overwrite = TRUE, ...)
    }
  }

  invisible()
}

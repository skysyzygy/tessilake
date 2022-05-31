
#' cache_create
#'
#' Internal function to create cache directory, simply defined by
#' `tessilake.{depth}/{type}`
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#'
#' @return string for the configured cache path
#' @importFrom checkmate assert_character assert_choice test_character test_directory
#' @examples
#' \dontrun{
#' cache_create("test","deep","stream")
#' }
cache_create <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream")) {
  assert_character(table_name)
  assert_choice(depth, c("deep", "shallow"))
  assert_choice(type, c("tessi", "stream"))

  cache_root <- config::get(paste0("tessilake.", depth))
  if (!(test_character(cache_root) && test_directory(cache_root, "rwx"))) {
    stop(paste0("Please set the tessilake.", depth, " option in config.yml to point to a cache path where you have read/write access."))
  }
  # build the cache query with arrow
  cache_path <- file.path(cache_root, type, table_name)

  if (!dir.exists(cache_path)) dir.create(cache_path, recursive = T)
  cache_path
}

#' cache_read
#'
#' Internal function to read cached arrow files. Simple wrapper on open_dataset that points to a directory defined by
#' `tessilake.{depth}/{type}`. Optionally returns the partition information.
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param include_partition boolean, whether or not to return the partition information as a column
#' @param ... extra arguments to pass on to arrow::open_dataset
#'
#' @return [`arrow::Dataset`]
#' @importFrom arrow open_dataset
#' @examples
#' \dontrun{
#' cache_read("test","deep","stream")
#' }
cache_read <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                       include_partition = FALSE, ...) {

  cache_path <- cache_create(table_name, depth, type)

  cache <- open_dataset(cache_path, format = ifelse(depth == "deep", "parquet", "arrow"), ...)

  partitioning = cache_get_partitioning(cache)
  partition_name = paste0("partition_",partitioning$primary_keys)

  if (!is.null(partitioning$primary_keys) && partition_name %in% names(cache) && include_partition == FALSE) {
    cache <- select(cache, -!!partition_name)
  }

  cache
}

#' cache_get_partitioning
#'
#' Internal function to read/generate partition information from an Arrow Dataset.
#'
#' @param x Arrow Dataset
#'
#' @return a named vector, the key is the primary_key, the value is an un-evaluated R expression to be used in creating the partitioning by
#' @importFrom checkmate assert check_class
#' @examples
#' \dontrun{
#' cache_get_partitioning(cache_read("test","deep","stream"))
#' }
cache_get_partitioning <- function(x) {
  assert(check_class(x, "Dataset"), check_class(x, "arrow_dplyr_query"))

  dataset <- (if (inherits(x, "arrow_dplyr_query")) x$.data else x)

  ret <- list(primary_keys=NULL,expression=NULL)

  if(!is.null(dataset$metadata$r)) {
    partitioning <- unserialize(charToRaw(dataset$metadata$r))$attributes$partitioning
    primary_keys <- unserialize(charToRaw(dataset$metadata$r))$attributes$primary_keys

    if(!is.null(partitioning)) {
      ret <- list(primary_keys = primary_keys,
                expression = rlang::parse_expr(partitioning)
      )
    }
  }

   ret
}

#' cache_make_partitioning
#'
#' Builds suggested partitioning from the primary keys of the table.
#'
#' @param x data.frame
#' @param primary_keys primary keys to set partitioning with
#'
#' @return a named vector, the key is the primary_key, the value is an un-evaluated R expression to be used in creating the partitioning by
#' @export
#' @importFrom checkmate assert_character check_character assert_names check_numeric assert test_numeric test_character
#' @importFrom rlang expr
#' @importFrom stats median
#' @examples
#' x <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"))
#'
#' cache_make_partitioning(x, primary_keys = "a")
#' # expression a/10000
#'
#' x %>% mutate(partition = eval(cache_make_partitioning(x, primary_keys = "a")))
#'
cache_make_partitioning <- function(x, primary_keys = attr(x, "primary_keys")) {
  assert_data_frame(x)
  assert_character(primary_keys, min.len = 1)
  assert_names(colnames(x), must.include = primary_keys)

  if (is.data.table(x)) {
    primary_key <- x[, primary_keys[[1]], with = F][[1]]
  } else {
    primary_key <- x[, primary_keys[[1]]]
  }

  assert(
    check_character(primary_key, any.missing = F, unique = T),
    check_numeric(primary_key, any.missing = F, unique = T)
  )

  primary_key <- sort(primary_key)

  if (test_numeric(primary_key, any.missing = F, unique = T)) {
    offset <- 10000 * median(primary_key[-1] - primary_key[-length(primary_key)])
    partitioning = expr(floor(!!sym(primary_keys[[1]]) / !!offset))
  } else if (test_character(primary_key, any.missing = F, unique = T)) {
    partitioning = expr(substr(tolower(!!sym(primary_keys[[1]])), 1, 2))
  }

  list(primary_keys = primary_keys[[1]], expression = partitioning)
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
#' @importFrom arrow write_dataset
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
                        partition = TRUE, ...) {

  cache_path <- cache_create(table_name, depth, type)

  if (!is.null(primary_keys) && partition == TRUE) {
    partitioning <- cache_make_partitioning(x, primary_keys = primary_keys)
    setattr(x, "partitioning", rlang::expr_deparse(partitioning$expression))
    setattr(x, "primary_keys", partitioning$primary_keys)
    partition_name = paste0("partition_",partitioning$primary_keys)

    if (inherits(x, "data.table")) {
      x[, (partition_name) := eval(partitioning$expression)]
    } else {
      x <- mutate(x, !!partition_name := eval(partitioning$expression))
    }
    write_dataset(x, cache_path,
      format = ifelse(depth == "deep", "parquet", "arrow"),
      partitioning = partition_name, ...
    )

    if (inherits(x, "data.table")) x[, (partition_name) := NULL]
  } else {
    write_dataset(x, cache_path, format = ifelse(depth == "deep", "parquet", "arrow"), ...)
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
                         primary_keys = attr(x, "primary_keys"), ...) {
  dataset <- cache_read(table_name, depth, type, include_partition = T, ...)

  assert_data_frame(x)

  partitioning <- cache_get_partitioning(dataset)
  partition_name = paste0("partition_",partitioning$primary_keys)

  if (!is.null(partitioning$primary_keys)) {
    if (!partitioning$primary_keys %in% primary_keys) {
      stop(sprintf(
        "Dataset has partitioning (%s) but dataset primary keys are (%s). Cowardly refusing to continue.",
        rlang::expr_deparse(partitioning$expression),
        primary_keys %||% "NULL"
      ))
    }

    partitions <- eval_tidy(partitioning$expression, x)

    # load only the dataset partitions that need to get updated
    dataset <- dataset %>%
      filter(!!sym(partition_name) %in% partitions) %>%
      select(-!!partition_name) %>%
      collect() %>%
      setDT()

    x <- update_table(x, dataset, primary_keys = c(!!primary_keys))
  }

  cache_write(x, table_name, depth, type, primary_keys = primary_keys, ...)
}

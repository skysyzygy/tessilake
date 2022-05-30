
#' cache_create
#'
#' Internal function to create cache directory, simply defined by
#' `tessilake.{depth}/{type}`
#'
#' @param tableName string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#'
#' @return string for the configured cache path
#' @importFrom checkmate assert_character assert_choice test_character test_directory
#' @examples
cache_create <- function(tableName, depth = c("deep", "shallow"), type = c("tessi", "stream")) {
  assert_character(tableName)
  assert_choice(depth,c("deep","shallow"))
  assert_choice(type,c("tessi","stream"))

  cacheRoot <- config::get(paste0("tessilake.", depth))
  if (!(test_character(cacheRoot) && test_directory(cacheRoot,"rwx"))) {
    stop(paste0("Please set the tessilake.", depth, " option in config.yml to point to a cache path where you have read/write access."))
  }
  # build the cache query with arrow
  cachePath <- file.path(cacheRoot, type, tableName)

  if (!dir.exists(cachePath)) dir.create(cachePath, recursive = T)
  cachePath
}

#' cache_read
#'
#' Internal function to read cached arrow files. Simple wrapper on open_dataset that points to a directory defined by
#' `tessilake.{depth}/{type}`. Optionally returns the partition information.
#'
#' @param tableName string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param include.partition boolean, whether or not to return the partition information as a column
#'
#' @return [`arrow::Dataset`]
#' @importFrom arrow open_dataset
#' @examples
cache_read <- function(tableName, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                       include.partition = F, ...) {
  cachePath <- cache_create(tableName, depth, type)

  cache <- open_dataset(cachePath, format = ifelse(depth == "deep", "parquet", "arrow"), ...)

  if ("partition" %in% names(cache) && include.partition == F) {
    cache <- select(cache, -partition)
  }

  cache
}

#' cache_get_partitioning
#'
#' Internal function to read/generate partition information from an Arrow Dataset.
#'
#' @param x Arrow Dataset
#'
#' @return string column name of partition
#' @importFrom checkmate assert check_class
#' @examples
cache_get_partitioning <- function(x) {
  assert(check_class(x,"Dataset"),check_class(x,"arrow_dplyr_query"))

  dataset <- (if (inherits(x, "arrow_dplyr_query")) x$.data else x)
  partitioning = unserialize(charToRaw(dataset$metadata$r))$attributes$partitioning

  if(!is.null(partitioning)) rlang::parse_expr(partitioning)

}

#' cache_make_partitioning
#'
#' Builds suggested partitioning from the primary keys of the table.
#'
#' @param x data.frame
#' @param primaryKeys primary keys to set partitioning with
#'
#' @return un-evaluated R expression to be used in creating the partitioning by
#' @export
#' @importFrom checkmate assert_character check_character assert_names check_numeric assert test_numeric test_character
#' @importFrom rlang expr
#' @examples
#' x = data.frame(a=c(1,2,3),b=c("a","b","c"))
#'
#' cache_make_partitioning(x,primaryKeys="a")
#' # expression a/10000
#'
#' x %>% mutate(partition = eval(cache_make_partitioning(x,primaryKeys="a")))
#'
cache_make_partitioning <- function(x, primaryKeys = attr(x, "primaryKeys")) {
  assert_data_frame(x)
  assert_character(primaryKeys,min.len=1)
  assert_names(colnames(x),must.include=primaryKeys)

  if(is.data.table(x)) {
    primaryKey = x[,primaryKeys[[1]],with=F][[1]]
  } else {
    primaryKey = x[,primaryKeys[[1]]]
  }

  assert(check_character(primaryKey,any.missing=F,unique=T),
         check_numeric(primaryKey,any.missing=F,unique=T))

  primaryKey = sort(primaryKey)

  if (test_numeric(primaryKey,any.missing = F,unique = T)) {
    offset = 10000 * median(primaryKey[-1]-primaryKey[-length(primaryKey)])
    return(expr(floor(!!sym(primaryKeys[[1]]) / !!offset)))
  } else if (test_character(primaryKey,any.missing = F,unique = T)) {
    return(expr(substr(tolower(!!sym(primaryKeys[[1]])),1,2)))
  }

}

#' cache_write
#'
#' Internal function to write cached arrow files. Simple wrapper on write_dataset that points to a directory defined by
#' `tessilake.{depth}/{type}` and uses partitioning specified by primaryKeys attribute/argument.
#'
#' @param x data.frame to be written
#' @param tableName string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param primaryKeys character vector of columns to be used for partitioning, only the first one is currently used
#'
#' @return invisible
#' @importFrom arrow write_dataset
#' @importFrom dplyr select mutate
#' @importFrom bit setattr
#' @examples
cache_write <- function(x, tableName, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                        primaryKeys = attr(x, "primaryKeys"), ...) {
  cachePath <- cache_create(tableName, depth, type)

  if (!is.null(primaryKeys)) {
    partitioning = cache_make_partitioning(x,primaryKeys=primaryKeys)
    setattr(x,"partitioning",rlang::expr_deparse(partitioning))

    if (inherits(x, "data.table")) {
      x[, partition := eval(partitioning)]
    } else {
      x <- mutate(x, partition = eval(partitioning))
    }
    write_dataset(x, cachePath,
      format = ifelse(depth == "deep", "parquet", "arrow"),
      partitioning = "partition", ...
    )

    if (inherits(x, "data.table")) x[, partition := NULL]

  } else {
    write_dataset(x, cachePath, format = ifelse(depth == "deep", "parquet", "arrow"), ...)
  }

  invisible()
}

#' cache_update
#'
#' Internal function to update portions of cached arrow files.
#'
#' @param x data.frame or part of a data.frame to be cached
#' @param tableName string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#'
#' @return invisible
#' @importFrom arrow open_dataset
#' @importFrom dplyr select filter_at
#' @importFrom rlang sym
#' @examples
cache_update <- function(x, tableName, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                         primaryKeys = attr(x, "primaryKeys"), ...) {
  dataset <- cache_read(tableName, depth, type, include.partition = T, ...)

  assert_data_frame(x)

  partitioning = cache_get_partitioning(dataset)
  primaryKey <- primaryKeys[[1]]

  if (!is.null(partitioning)) {
    if(is.null(primaryKeys)) {
      stop(sprintf(
        "Dataset has partitioning (%s) but dataset doesn't have a primary key to develop partition with. Cowardly refusing to continue.",
        as.character(partitioning)
      ))
    }

    partitions = eval_tidy(partitioning,x)

    # load only the dataset partitions that need to get updated
    dataset <- dataset %>%
      filter(partition %in% partitions) %>%
      select(-partition) %>%
      collect() %>%
      setDT()

    x <- update_table(x, dataset, primaryKeys = c(!!primaryKeys))
  }

  cache_write(x, tableName, depth, type, primaryKeys = primaryKeys, ...)
}

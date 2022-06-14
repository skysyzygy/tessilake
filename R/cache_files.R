#' cache_get_mtime
#'
#' Get the last modification time of the cache by querying the mtime of all of the files that make up the cache
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#'
#' @return POSIXct modification time
#' @examples
#' \dontrun{
#' cache_get_mtime("test","deep","stream")
#' }
cache_get_mtime <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream")) {
  cache_files <- c(
    dir(cache_path(table_name, depth, type), full.names = TRUE, recursive = TRUE),
    dir(dirname(cache_path(table_name, depth, type)), full.names = TRUE, recursive = TRUE)
  )

  cache_mtime <- max(file.mtime(cache_files), na.rm = TRUE)
}

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
#' @export
#' @examples
#' \dontrun{
#' cache_path("test", "deep", "stream")
#' }
cache_path <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream")) {
  assert_character(table_name)
  assert_choice(depth, c("deep", "shallow"))
  assert_choice(type, c("tessi", "stream"))

  cache_root <- config::get(paste0("tessilake.", depth))

  file.create(file.path(cache_root, "test"), showWarnings = FALSE)
  if (!test_character(cache_root) || !file.exists(file.path(cache_root, "test"))) {
    stop(paste0("Please set the tessilake.", depth, " option in config.yml to point to a cache path where you have read/write access."))
  }
  file.remove(file.path(cache_root, "test"))

  # build the cache query with arrow
  cache_path <- file.path(cache_root, type, table_name)

  cache_path
}

#' cache_exists
#'
#' Internal function to test if a cache already exists.
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#'
#' @return TRUE/FALSE
#' @export
#' @examples
#' \dontrun{
#' cache_exists("test", "deep", "stream")
#' }
cache_exists <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream")) {
  cache_path <- cache_path(table_name, depth, type)

  dir.exists(cache_path) || file.exists(paste0(cache_path, ".feather")) || file.exists(paste0(cache_path, ".parquet"))
}

#' cache_delete
#'
#' Delete some or all cache files for a given cache.
#'
#' @param table_name string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#' @param partitions optional vector of partitions to delete
#'
#' @return nothing, invisibly
#' @export
#'
#' @examples
#' \dontrun{
#' cache_delete("test","deep","stream",partitions=c(1,2,3))
#' }
cache_delete <- function(table_name, depth = c("deep", "shallow"), type = c("tessi", "stream"),
                         partitions = NULL) {

  if(!is.null(partitions)) assert(check_character(partitions),check_numeric(partitions))

  if(!cache_exists(table_name, depth, type))
    stop(paste("Cache",table_name, depth, type, "doesn't exist so it can't be deleted."))

  cache_path <- cache_path(table_name, depth, type)

  if(is.null(partitions)) {
    unlink(paste0(cache_path,c("",".feather",".parquet")), recursive = TRUE)
  } else {
    if(!dir.exists(cache_path))
      stop(paste("Cache",table_name, depth, type, "isn't partitioned, can't delete specified partitions."))
    gc() # have to force R to destroy open file connectors... not a perfect solution but the best I can come up with now.
    purrr::map(unique(partitions),~unlink(file.path(cache_path,paste0("partition*=",.x)),recursive = TRUE))
  }
  invisible()
}


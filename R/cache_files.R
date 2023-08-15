#' cache_get_mtime
#'
#' Get the last modification time of the cache by querying the `mtime` of all of the files that make up the cache
#'
#' @param table_name string
#' @param depth string, e.g. "deep" or "shallow"
#' @param type string, e.g. "tessi" or "stream"
#'
#' @return POSIXct modification time
#' @examples
#' \dontrun{
#' cache_get_mtime("test", "deep", "stream")
#' }
cache_get_mtime <- function(table_name, depth, type) {
  cache_path <- cache_path(table_name, depth, type)

  cache_files <- c(
    dir(cache_path,
        full.names = TRUE, recursive = TRUE),
    dir(dirname(cache_path),
        pattern = paste0(basename(cache_path),"\\..+"),
        full.names = TRUE)
  )

  max(c(file.mtime(cache_files), -Inf), na.rm = TRUE)
}

#' cache_path
#'
#' Convenience functions to return the configured cache path and check that we have read/write access to it
#' `tessilake.{depth}/{type}/{table_name}`
#'
#' @param table_name string
#' @param depth string, e.g. "deep" or "shallow"
#' @param type string, e.g. "tessi" or "stream"
#'
#' @return string for the configured cache path
#' @importFrom checkmate assert_character assert_choice test_character test_directory
#' @export
#' @examples
#' \dontrun{
#' cache_path("test", "deep", "stream")
#' }
#' @describeIn cache_path function to return the specified cache path and check that we have read/write access to
cache_path <- function(table_name, depth, type) {
  assert_character(table_name, len = 1)
  assert_choice(depth, names(config::get("tessilake")$depths))

  cache_root <- config::get("tessilake")[["depths"]][[depth]][["path"]]

  tmpfile <- tempfile("cache_write_test",cache_root)

  file.create(tmpfile, showWarnings = FALSE)
  if (!test_character(cache_root) || !file.exists(tmpfile)) {
    stop(paste0("Please set the tessilake.", depth, " option in config.yml to point to a cache path where you have read/write access."))
  }
  file.remove(tmpfile)

  # build the cache query with arrow
  cache_path <- file.path(cache_root, type, table_name)

  cache_path
}

#' @export
#' @describeIn cache_path wrapper around [cache_path] that returns the path for the primary (first) defined storage
cache_primary_path <- function(table_name, type) {

  cache_path(table_name = table_name, depth = names(config::get("tessilake")$depths)[1], type = type)

}

#' cache_exists
#'
#' Internal function to test if a cache already exists.
#'
#' @param table_name string
#' @param depth string, e.g. "deep" or "shallow"
#' @param type string, e.g. "tessi" or "stream"
#'
#' @return TRUE/FALSE
#' @export
#' @examples
#' \dontrun{
#' cache_exists("test", "deep", "stream")
#' }
cache_exists <- function(table_name, depth, type) {
  cache_path <- cache_path(table_name, depth, type)

  dir.exists(cache_path) || file.exists(paste0(cache_path, ".feather")) || file.exists(paste0(cache_path, ".parquet"))
}

#' @export
#' @describeIn cache_exists Convenience function that tests if a cache already exists at any depth
cache_exists_any <- function(table_name, type) {
  depths <- names(config::get("tessilake")[["depths"]])

  any(sapply(depths, \(depth) cache_exists(table_name = table_name, depth = depth, type = type)))
}

#' cache_delete
#'
#' Delete some or all cache files for a given cache.
#'
#' @param table_name string
#' @param depth string, e.g. "deep" or "shallow"
#' @param type string, e.g. "tessi" or "stream"
#' @param partitions optional vector of partitions to delete
#'
#' @return nothing, invisibly
#' @export
#'
#' @examples
#' \dontrun{
#' cache_delete("test", "deep", "stream", partitions = c(1, 2, 3))
#' }
cache_delete <- function(table_name, depth, type,
                         partitions = NULL) {
  if (!is.null(partitions)) assert(check_character(partitions), check_numeric(partitions))

  if (!cache_exists(table_name, depth, type)) {
    stop(paste("Cache", table_name, depth, type, "doesn't exist so it can't be deleted."))
  }

  cache_path <- cache_path(table_name, depth, type)

  if (is.null(partitions)) {
    unlink(paste0(cache_path, c("", ".feather", ".parquet")), recursive = TRUE)
  } else {
    if (!dir.exists(cache_path)) {
      stop(paste("Cache", table_name, depth, type, "isn't partitioned, can't delete specified partitions."))
    }
    gc() # have to force R to destroy open file connectors... not a perfect solution but the best I can come up with now.
    purrr::map(unique(partitions), ~ unlink(file.path(cache_path, paste0("partition*=", .x)), recursive = TRUE))
  }
  invisible()
}

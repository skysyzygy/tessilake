#' local_cache_dirs
#'
#' Test helper, create cache dirs for tests.
#'
#' @param ...
#' @param envir parent.frame()
local_cache_dirs <- function(..., envir=parent.frame()) {
  dir.create(file.path(tempdir(), "deep"))
  dir.create(file.path(tempdir(), "shallow"))

  withr::defer({
    unlink(file.path(tempdir(), "deep"), recursive = T)
    unlink(file.path(tempdir(), "shallow"), recursive = T)
  }, envir = envir)
}

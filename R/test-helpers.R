#' local_cache_dirs
#'
#' Test helper, create cache directories for tests.
#'
#' @param ... other arguments for compatibility with testthat?
#' @param envir parent.frame()
#' @export
local_cache_dirs <- function(..., envir = parent.frame()) {
  dir.create(file.path(tempdir(), "deep"))
  dir.create(file.path(tempdir(), "shallow"))

  withr::defer({
      gc()
      unlink(file.path(tempdir(), "deep"), recursive = T)
      unlink(file.path(tempdir(), "shallow"), recursive = T)
    },
    envir = envir
  )
}

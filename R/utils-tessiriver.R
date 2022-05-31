
#' read_stream
#'
#' @param streamName character name of the stream file, with or without the extension
#' @param subset logical expression indicating elements or rows to keep
#' @param select character vector indicating columns to select from stream file
#' @param ... further arguments to be passed to or from other methods
#'
#' @importFrom rlang enquo
#' @importFrom ffbase unpack.ffdf ffwhich
#' @importFrom ff delete
#' @importFrom data.table setDT
#'
#' @return a data.table of the subsetted stream, invisibly
#' @export
#'
#' @examples
#'
#' if(FALSE){read_stream("addressStream",
#'          timestamp>=as.Date("2021-07-01"),
#'          c("group_customer_no","timestamp")
#'          )
#'          }
#'
#'

read_stream = function(streamName,subset,select,...) {
  subset = enquo(subset)
  streamDir = .GlobalEnv$streamDir

  streamFile = file.path(streamDir,paste0(streamName,c("",".gz")))
  streamFile = if(file.exists(streamFile[1])) streamFile[1] else streamFile[2]
  stopifnot(file.exists(streamFile))

  ffdf = unpack.ffdf(streamFile)

  i = ffwhich(ffdf,subset)
  DT = setDT(ffdf[i,select])

  delete(ffdf)

  invisible(DT)
}

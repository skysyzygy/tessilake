# as.ram2 -----------------------------------------------------------------

#' as.ram2
#' wrapper for ff:as.ram that also deletes the underlying ff/ffdf to reduce temp space
#' @param obj ff/ffdf object to be loaded to ram and deleted
#'
#' @return a data.table
#' @export
#' @importFrom ff as.ram delete is.ffdf
#' @importFrom ffbase as.ram.ffdf
#' @importFrom data.table setDT setattr
#' @examples
#' library(ff)
#' library(tessilake)
#'
#' test <- ff(1:5)
#' file.exists(filename(test)) # TRUE
#' test.ram <- as.ram2(test)
#' file.exists(filename(test)) # FALSE
#'
as.ram2 <- function(obj) {
  if (is.ffdf(obj)) {
    r <- setDT(obj[, ])
  } else {
    r <- obj[]
  }
  delete(obj)
  # fix_vmode sets vmode in a way that persists after loading from ff
  setattr(r, "vmode", NULL)
  r
}

# fix_vmode ---------------------------------------------------------------

#' fix_vmode
#' adjust the vmode of a vector to the lowest necessary to save it as ff
#'
#' @param vec the vector to be adapted
#'
#' @return a vector with the vmode attribute set appropriately
#'
#' @export
#' @importFrom Rfast is_integer
#' @importFrom glue glue
#' @importFrom data.table setattr
#' @importFrom ff is.ff vmode vmode<- .rammode
#'
#' @examples
#'
#' library(ff)
#' test <- 1:5
#' test <- fix_vmode(test)
#' vmode(test) # "nibble"
#' test <- fix_vmode(test[1:3])
#' vmode(test) # "quad"
#'
fix_vmode <- function(vec) {
  if (is.ff(vec)) {
    stop("ff objects can't be coerced")
  }
  if (is.character(vec)) {
    message("Converting character to factor...")
    return(as.factor(vec))
  } else if (is.factor(vec)) {
    message("Already a factor")
    return(vec)
  }

  minVec <- min(vec, na.rm = TRUE)
  maxVec <- max(vec, na.rm = TRUE)
  isDate <- inherits(vec, "Date")
  hasDec <- !isDate && !Rfast::is_integer(vec[which(!is.na(vec))])
  hasNA <- any(is.na(vec))
  signed <- minVec < 0 || isDate
  bits <- ceiling(log2(abs(as.numeric(maxVec)) + .01))

  vmode <-
    if (hasDec) {
      "double" # 64 bit float
    } else if (!signed & !hasNA) {
      if (!isDate & bits == 1) {
        "boolean"
      } # 1 bit logical without NA
      else if (bits <= 2) {
        "quad"
      } # 2 bit unsigned integer without NA
      else if (bits <= 4) {
        "nibble"
      } # 4 bit unsigned integer without NA
      else if (bits <= 8) {
        "ubyte"
      } # 8 bit unsigned integer without NA
      else if (bits <= 16) {
        "ushort"
      } # 16 bit unsigned integer without NA
      else if (bits <= 32) {
        "integer"
      } # 32 bit unsigned integer without NA
      else {
        "double"
      }
    } else {
      if (!isDate & bits == 1 & !signed) {
        "logical"
      } # 2 bit logical with NA
      else if (bits <= 7) {
        "byte"
      } # 8 bit signed integer with NA
      else if (bits <= 15) {
        "short"
      } # 16 bit signed integer with NA
      else if (bits <= 31) {
        "integer"
      } # 32 bit signed integer with NA
      else {
        "double"
      }
    }


  setattr(vec, "vmode", NULL)
  if (.rammode[vmode] != storage.mode(vec)) {
    message(glue("Converting to {vmode}..."))
    vmode(vec) <- vmode
  } else {
    message(glue("Assigning vmode attribute to {vmode}..."))
    setattr(vec, "vmode", vmode)
  }
  vec
}

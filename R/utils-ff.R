# as.ram2 -----------------------------------------------------------------

#' as.ram2
#' wrapper for ff:as.ram that also deletes the underlying ff/ffdf to reduce temp space
#' @param obj ff/ffdf object to be loaded to ram and deleted
#'
#' @return a data.table
#' @export
#' @importFrom ff as.ram delete
#' @importFrom ffbase as.ram.ffdf
#' @importFrom data.table setDT setattr
#' @examples
#'
#' library(ff)
#'
#' test <- ff(1:5)
#' file.exists(filename(test)) # TRUE
#' test.ram <- as.ram2(test)
#' file.exists(filename(test)) # FALSE
#'

as.ram2 = function(obj) {
  if(is.ffdf(obj))
    r = setDT(obj[,])
  else
    r = obj[]
  delete(obj)
  # fix_vmode sets vmode in a way that persists after loading from ff
  setattr(r,"vmode",NULL)
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
#' @import ff
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

fix_vmode = function(vec) {
  if(is.ff(vec)) {
    stop("ff objects can't be coerced")
  }
  if(is.character(vec)) {
    message("Converting character to factor...")
    return(as.factor(vec))
  } else if(is.factor(vec)) {
    message("Already a factor")
    return(vec)
  }

  minVec = min(vec,na.rm=TRUE)
  maxVec = max(vec,na.rm=TRUE)
  isDate = inherits(vec,"Date")
  hasDec = !isDate && !Rfast::is_integer(vec[which(!is.na(vec))])
  hasNA = any(is.na(vec))
  signed = minVec<0
  bits = ceiling(log2(abs(as.numeric(maxVec))+.01))

  vmode =
    if(hasDec) {"double" 	#64 bit float
    } else if(bits ==1 & !signed & !hasNA & !isDate) {"boolean" 	#1 bit logical without NA
    } else if(bits ==1 & !signed & !isDate) {"logical" 	#2 bit logical with NA
    } else if(bits <=2 & !signed & !hasNA) {"quad" 	#2 bit unsigned integer without NA
    } else if(bits <=4 & !signed & !hasNA) {"nibble" #	4 bit unsigned integer without NA
    } else if(bits <=7) {"byte" #8 bit signed integer with NA
    } else if(bits <=8 & !signed & !hasNA) {"ubyte" 	#8 bit unsigned integer without NA
    } else if(bits <=15) {"short" 	#16 bit signed integer with NA
    } else if(bits <=16 & !signed & !hasNA) {"ushort" 	#16 bit unsigned integer without NA
      #} else if(bits <=23) {"single" 	#32 bit float
    } else if(bits <=31) {"integer" 	#32 bit signed integer with NA
    } else {"double"} 	#64 bit float


  if(vmode(vec)!=vmode) {
    if(.rammode[vmode]!=storage.mode(vec)) {
      message(glue("Converting {vmode(vec)} to {vmode}..."))
      vmode(vec) <- vmode
    } else {
      message(glue("Changing vmode attribute from {vmode(vec)} to {vmode}"))
      setattr(vec,"vmode",vmode)
    }
  } else {
    message(glue("Nothing to be done, already {vmode(vec)}"))
  }
  vec
}

as.ram2 = function(.) {r = ff::as.ram(.); ff::delete(.); r}

# Reduce ff storage needs
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

  if(is.POSIXct(vec)) {
    message("Converting datetime to date...")
    vec = as_date(vec)
  }

  minVec = min(vec,na.rm=TRUE)
  maxVec = max(vec,na.rm=TRUE)
  #numValues = n_distinct(vec)
  hasDec = !is.Date(vec) && !Rfast::is_integer(vec[which(!is.na(vec))])
  hasNA = any(is.na(vec))
  signed = minVec<0
  bits = ceiling(log2(abs(as.numeric(maxVec))+.01))
  #fBits = ceiling(log(abs(as.integer(numValues)))/log(2))

  vmode =
    if(bits <=52 & hasDec) {"double" 	#64 bit float
    } else if(bits ==1 & !hasNA) {"boolean" 	#1 bit logical without NA
    } else if(bits ==1) {"logical" 	#2 bit logical with NA
    } else if(bits <=2 & !signed & !hasNA) {"quad" 	#2 bit unsigned integer without NA
    } else if(bits <=4 & !signed & !hasNA) {"nibble" #	4 bit unsigned integer without NA
    } else if(bits <=7) {"byte" #8 bit signed integer with NA
    } else if(bits <=8 & !signed & !hasNA) {"ubyte" 	#8 bit unsigned integer without NA
    } else if(bits <=15) {"short" 	#16 bit signed integer with NA
    } else if(bits <=16 & !signed & !hasNA) {"ushort" 	#16 bit unsigned integer without NA
      #} else if(bits <=23) {"single" 	#32 bit float
    } else if(bits <=31) {"integer" 	#32 bit signed integer with NA
    } else {"double"} 	#64 bit float

  # fVmode =
  #   if(fBits == 1) {"boolean" 	#1 bit logical without NA
  #   } else if(fBits <=2) {"quad" 	#2 bit unsigned integer without NA
  #   } else if(fBits <=4) {"nibble" #	4 bit unsigned integer without NA
  #   } else if(fBits <=8) {"ubyte" 	#8 bit unsigned integer without NA
  #   } else if(fBits <=16) {"ushort" 	#16 bit unsigned integer without NA
  #   } else if(fBits <=31) {"integer" 	#32 bit signed integer with NA
  #   } else {"double"} 	#64 bit float
  #
  # # Would be better as a factor
  # if (.rambytes[vmode]>.rambytes[fVmode]) {
  #   vec = as.factor(vec)
  #   vmode = fVmode
  # }

  if(vmode(vec)!=vmode) {
    if(.rammode[vmode]!=storage.mode(vec)) {
      message(glue::glue("Converting {vmode(vec)} to {vmode}..."))
      vmode(vec) <- vmode
    } else {
      message(glue::glue("Changing vmode attribute from {vmode(vec)} to {vmode}"))
      setattr(vec,"vmode",vmode)
    }
  } else {
    message(glue::glue("Nothing to be done, already {vmode(vec)}"))
  }
  vec
}


#' cache_create
#'
#' Internal function to create cached for arrow files. Simple wrapper on write_datset that points to a directory defined by
#' `tessilake.{depth}/{type}`
#'
#' @param tableName string
#' @param depth string, either "deep" or "shallow"
#' @param type string, either "tessi" or "stream"
#'
#' @return string for the configured cache path
#' @importFrom arrow write_dataset
#' @examples
cache_create = function(tableName,depth=c("deep","shallow"),type=c("tessi","stream")) {

  stopifnot("tableName is required" = !missing(tableName),
            "depth must be either 'deep' or 'shallow'" = depth %in% c("deep","shallow"),
            "type must be either 'tessi' or 'stream'" = type %in% c("tessi","stream"))
  cacheRoot = getOption(paste0("tessilake.",depth))
  if(is.null(cacheRoot) || !dir.exists(cacheRoot))
    stop(paste0("Please set the tessilake.",depth," option to point to an existing cache path."))
  # build the cache query with arrow
  cachePath = file.path(cacheRoot,type,tableName)

  if(!dir.exists(cachePath)) dir.create(cachePath,recursive=T)
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
cache_read = function(tableName,depth=c("deep","shallow"),type=c("tessi","stream"),
                      include.partition=F,...) {

  cachePath = cache_create(tableName,depth,type)

  cache = open_dataset(cachePath,format=ifelse(depth=="deep","parquet","arrow"),...)

  if("partition" %in% names(cache) && include.partition==F)
    cache = select(cache,-partition)

  cache
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
#' @examples
cache_write = function(x,tableName,depth=c("deep","shallow"),type=c("tessi","stream"),
                       primaryKeys=attr(x,"primaryKeys"),...) {

  cachePath = cache_create(tableName,depth,type)

  if(!is.null(primaryKeys)) {
    # if(inherits(x,"data.table")) {
    #   x[,partition:=get(primaryKeys[[1]]) %>% substr(1,3) %>% as.integer]
    # } else {
      x = mutate(x,partition=substr(get(primaryKeys[[1]]),1,3) %>% as.integer)
#    }
    write_dataset(x,cachePath,format=ifelse(depth=="deep","parquet","arrow"),
                          partitioning="partition",...)

 #   x$partition=NULL

  } else {
    write_dataset(x,cachePath,format=ifelse(depth=="deep","parquet","arrow"),...)
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
cache_update = function(x,tableName,depth=c("deep","shallow"),type=c("tessi","stream"),
                       primaryKeys=attr(x,"primaryKeys"),...) {

  dataset = cache_read(tableName,depth,type,include.partition=T,...)

  files = (if(inherits(dataset,"arrow_dplyr_query")) dataset$.data else dataset)$files
  dataset.partitioning = stringr::str_match(files,"(?<var>\\w+)=\\w+")[1,2]
  primaryKey = primaryKeys[[1]]

  if(!is.na(dataset.partitioning) && is.null(primaryKeys))
    stop(sprintf("Dataset has partitioning (%s) but dataset doesn't have a primary key to develop partition with. Cowardly refusing to continue.",
                 dataset.partitioning))
  if(!is.na(dataset.partitioning) && !is.numeric(select(x,primaryKey)[[1]]))
    stop(sprintf("First primary key (%s) isn't a numeric column, so I don't know how to partition this.",
                 primaryKey))

  if(!is.na(dataset.partitioning)) {
    partitions = select(x, !!primaryKey)[[1]] %>% substr(1,3) %>% as.integer

    # load only the dataset partitions that need to get updated
    dataset = dataset %>% filter(partition %in% partitions) %>% select(-partition) %>% collect %>% setDT

    x = update_table(x,dataset,primaryKeys=c(!!primaryKeys))
  }

  cache_write(x,tableName,depth,type,primaryKeys=primaryKeys,...)
}



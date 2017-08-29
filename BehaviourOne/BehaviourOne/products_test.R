##
##
##
## @script:       Products segmentation test
## @created:      2017.03.23
## @lastmodified: 2017.04.06
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##


###
SQL_CONNECT = 3


# 1: use RODBC to remotely connect to sql server
# 2: use RevoScaleR to remotely connect to sql server (Microsoft R ONLY!)
# 3: use RevoScaleR to load from cache local file
# 4: csv - just load simple csv file 

###
### @INPUT parameter preparation
###

svr <- "server=VSQL08\\HYPERLOOP;"
db  <- "database=temp_ML;"
uid <- "uid=andreidi;"
pwd <- "pwd=hyp2017!!"
sqls <- "SELECT * FROM _TextItems"

text_column  <- "AllText"
label_column <- "ItemName"
id_column <-"ItemId"
NORMALIZE <- FALSE
SAVE_NAME <- FALSE

###
### END INPUT PARAMS
###

######################
######################
######################
######################
###################### BEGIN UPDATE COPY-PASTE SECTION
######################
######################
######################
######################

MODULE.VERSION <- "0.2.7"
MODULE.NAME <- "PRODUCTS_PREP"
NR_ROWS = 1e8
FULL_DEBUG <- FALSE
REMOVE_INVALiD_INACTIV <- FALSE


SPARSITY <- 0.999

##
## HELPER FUNCTIONS
##

get_scriptpath <- function() {
  # location of script can depend on how it was invoked:
  # source() and knit() put it in sys.calls()
  path <- NULL
  
  if(!is.null(sys.calls())) {
    # get name of script - hope this is consisitent!
    path <- as.character(sys.call(1))[2] 
    # make sure we got a file that ends in .R, .Rmd or .Rnw
    if (grepl("..+\\.[R|Rmd|Rnw]", path, perl=TRUE, ignore.case = TRUE) )  {
      return(path)
    } else { 
      message("Obtained value for path does not end with .R, .Rmd or .Rnw: ", path)
    }
  } else{
    # Rscript and R -f put it in commandArgs
    args <- commandArgs(trailingOnly = FALSE)
  }
  return(path)
}


all_log <- " "
log_fnpath <- dirname(get_scriptpath())
log_ctime <- format(Sys.time(), "%Y%m%d%H%M%S")
log_FN <- paste0("_log_",MODULE.NAME,"_",log_ctime,".txt")
LogFileName <- file.path(log_fnpath, log_FN)
logger <- function(text) {
  all_log <<- paste0(all_log, text)
  cat(text)
  write(x = all_log,
        file = LogFileName,
        sep = '\n',
        append = FALSE)
}

timeit = function(strmsg, expr) {
  tm <- system.time(expr)
  stm <- sprintf("%.2f min", tm[3] / 60)
  logger(paste0(strmsg, " executed in ", stm, "\n"))
}

debug_object_size <- function(obj) {
  obj_name <- deparse(substitute(obj))
  strs1 <- format(round(as.numeric(object.size(obj) / (1024 * 1024)), 1), nsmall = 1, big.mark = ",")
  strs2 <- format(
    round(as.numeric(nrow(df)), 1),
    nsmall = 0, big.mark = ",",
    scientific = FALSE)
  logger(sprintf("Object %s [%s] size: %sMB (%s rows by %d cols)\n",
                 obj_name,
                 class(obj)[1],
                 strs1,
                 strs2,
                 length(names(obj))))
}

get_scriptpath <- function() {
  # location of script can depend on how it was invoked:
  # source() and knit() put it in sys.calls()
  path <- NULL
  
  if(!is.null(sys.calls())) {
    # get name of script - hope this is consisitent!
    path <- as.character(sys.call(1))[2] 
    # make sure we got a file that ends in .R, .Rmd or .Rnw
    if (grepl("..+\\.[R|Rmd|Rnw]", path, perl=TRUE, ignore.case = TRUE) )  {
      return(path)
    } else { 
      message("Obtained value for path does not end with .R, .Rmd or .Rnw: ", path)
    }
  } else{
    # Rscript and R -f put it in commandArgs
    args <- commandArgs(trailingOnly = FALSE)
  }
  return(path)
}

save_df <- function(df)
{
  logger("\nSaving data...\n")
  file_db_path <- dirname(get_scriptpath()) 
  if(FALSE)
    logger(sprintf("Current working directory: %s\n",file_db_path))
  FN <- paste0(MODULE.NAME,"_v",MODULE.VERSION,"_data.csv")
  FileName <- file.path(file_db_path, FN)
  timeit(sprintf("Saving File: %s",FileName),
         write.csv(x = df, file = FileName, row.names = TRUE))
}

##
## END HELPER FUNCTIONS
##

ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
logger(sprintf("[%s] BEGIN Products Exploration script v%s SQL_CONNECT=%d\n", ctime, MODULE.VERSION,SQL_CONNECT))
library(reshape)
library(tm)

USE_REVOSCALER = FALSE # FALSE if using standard kmeans/etc



conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)

timeit = function(strmsg, expr) {
    tm <- system.time(expr)
    stm <- sprintf("%.2f min", tm[3] / 60)
    logger(paste0(strmsg, " executed in ", stm, "\n"))
}


if ("RevoScaleR" %in% rownames(installed.packages())) {
    library(RevoScaleR)
    # Create an xdf file name
    file_db_path <- dirname(get_scriptpath()) #getwd()
    logger(sprintf("Current workind directory: %s\n",file_db_path))
    dfXdfFileName <- file.path(file_db_path, "hyperloop_saved_products.xdf")
    logger(sprintf("XDF File: %s\n",dfXdfFileName))
}

CustomKMeans = function(x, centers, nstart) {
    if (USE_REVOSCALER) {
        factors <- colnames(x)
        kformula = as.formula(paste("~", paste(factors, collapse = "+")))
        sformula = toString(kformula)
        logger(paste0("Using RevoScaleR rxKmeans: [", sformula, "] "))
        model <- rxKmeans(formula = kformula,
                          data = x,
                          numClusters = centers,
                          numStarts = nstart,
                          reportProgress = 0)
    } else {
        logger("Using standard kMeans: ")
        model <- kmeans(x = x,
                        nstart = nstart,
                        centers = centers
                        )
    }
    return(model)
}

GetMaxes = function(dfd, newfield, categ1, categ2) {
    t0_maxes <- proc.time()
    for (i in 1:nrow(dfd)) {
        best_categ1_index <- which.max(dfd[i, categ1])
        best_categ1 <- colnames(dfd[i, categ1])[best_categ1_index]
        best_categ2_index <- which.max(dfd[i, categ2])
        best_categ2 <- colnames(dfd[i, categ2])[best_categ2_index]
        dfd[i, newfield] <- paste0(best_categ1, "/", best_categ2)
    }
    t1_maxes <- proc.time()
    t_maxes <- t1_maxes[3] - t0_maxes[3]
    return(dfd)
}

if (SQL_CONNECT == 1) {
    logger("Connecting to SQL Server...\n")
    library(RODBC)
    channel <- odbcDriverConnect(conns)
    logger("Downloading data...\n")

    timeit("sqlQuery", dfi <- sqlQuery(channel, paste(sqls)))

    odbcClose(channel)
    logger("Done data downloading.\n")
} else if (SQL_CONNECT == 4) {

    mypath <- dirname(sys.frame(1)$ofile)
    dfi <- read.csv(paste0(mypath, "/test.csv"))
} else if ((SQL_CONNECT == 2) || (SQL_CONNECT == 3)) {
    if ((SQL_CONNECT == 2) || (!file.exists(dfXdfFileName))) {
        logger("Microsoft RevoScaleR connect to SQL Server...\n")
        timeit(sprintf("RxOdbcData [%s]",sqls),
            dsOdbcSource <- RxOdbcData(sqlQuery = sqls,
                                       connectionString = conns))

        # Import the data into the xdf file
        timeit("rxImport ",
            rxImport(dsOdbcSource, dfXdfFileName, overwrite = TRUE))
    }
    # Read xdf file into a data frame
    logger("RevoScaleR loading dataframe...\n")
    timeit("rxDataStep: ",
            dfi <- rxDataStep(inData = dfXdfFileName,
                              numRows = NR_ROWS,
                              maxRowsByCols = 2000000 * 200
                              ))
    dfi[is.na(dfi)] <- 0
    df <- data.frame(dfi)
    logger("Done data downloading.\n")
    if(TRUE)
    {
      logger("Deleting xdf file...\n")
      unlink(dfXdfFileName)
    }
}

##
## END DATA INPUT
##
debug_object_size(df)

###
### end input parameter preparation
###

## preprocessing
PRE_VER <- "0.0.2"

logger(paste0("Begin text preprocessing v", PRE_VER, " ...\n"))
t0_prep <- proc.time()

timeit("Removing unknown chars Text: ", df$AllText <- iconv(df$AllText, 'Latin-9'))
timeit("Removing unknown chars Name: ", df$ItemName <- iconv(df$ItemName, 'Latin-9'))
debug_object_size(df)

if(REMOVE_INVALiD_INACTIV){
  toMatch <- c("invalid", "inactiv")
  excluded_filter <- paste(toMatch, collapse = "|")
  logger(paste0("Cleaning ",excluded_filter,"...\n"))
  excluded_recs <- grep(excluded_filter, df$ItemName)
  df <- df[-excluded_recs,]
  debug_object_size(df) 
}

if(FALSE)
    print(head(df[nchar(df$AllText)>300,]))



timeit("Prepare VectorSource: ", vector_src <- VectorSource(df[, text_column]))
debug_object_size(vector_src)

timeit("Prepare Corpus: ", corpus <- Corpus(vector_src))
debug_object_size(corpus)
logger("\n")

timeit("Prepare DocumentTermMatrix: ", dtm <- DocumentTermMatrix(corpus))
debug_object_size(dtm)
print(dtm)
logger("\n")

df_has_columns <- function(dfin, colname) {
    test_filter <- colname

    if (length(colname) > 1) {
        test_filter <- paste(colname, collapse = "|")
    }
    logger(sprintf("Looking for columns such as [%s]\n",test_filter))
    icols <- grep(test_filter, colnames(dfin))
    has_columns <- FALSE
    if (length(icols) > 0) {
        has_columns <- TRUE
        print(colnames(dfin)[icols])
    } else {
        logger("Column not found\n")
    }
    return(has_columns)
}

if(SPARSITY<1)
{
  sparse_prc <- SPARSITY
  timeit(sprintf("RemoveSparseTerms %.3f: ", sparse_prc),
         dtm <- removeSparseTerms(dtm, sparse_prc))
  debug_object_size(dtm)
  print(dtm)
  logger("\n")  
}



timeit("Prepare final BoW FULL matrix: ",
            mtxt <- as.matrix(dtm))
debug_object_size(mtxt)
if(SAVE_NAME)
{
  dftxt <- data.frame(ID = df$ItemId, Name = df$ItemName, mtxt)
} else
{
  dftxt <- data.frame(ID = df$ItemId, mtxt)
}
debug_object_size(dftxt)

logger("\n")
test_brand <- c("dupaplaja","hart","basici","plaja")
df_has_columns(dftxt, test_brand)
logger("\n")

newcolumns <- setdiff(colnames(dftxt),c("ID","Name"))

if(NORMALIZE){
  timeit("Normalising: ",
         dftxt <- melt(dftxt, id = c("ID","Name"))
  )
  debug_object_size(dftxt)
}

t1_prep <- proc.time()
prep_time <- (t1_prep[3] - t0_prep[3]) / 60

logger(sprintf("Done products preprocessing. Total time %.2f min\n", prep_time))

## end preprocessing


######################
######################
######################
######################
###################### BEGIN UPDATE COPY-PASTE SECTION
######################
######################
######################
######################

save_df(dftxt)

##
##
##
## @script:       Churn Prediction Alpha test
## @created:      2017.03.23
## @lastmodified: 2017.03.24
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##

MODULE.VERSION <- "0.0.2"


###
### 
### BEGIN SQL Server R code
### 
### OBS: set SQL_CONNECT=0 for MS SQL Server Server side R scripts
###
SQL_CONNECT = 3
# 0: Run from SLQ Server; 
# 1: use RODBC to remotely connect to sql server
# 2: use RevoScaleR to remotely connect to sql server (Microsoft R ONLY!)
# 3: use RevoScaleR to load from cache local file
# 4: csv - just load simple csv file 

all_log <- " "
logger <- function(text) {
    all_log <<- paste0(all_log, text)
    cat(text)
}

###
### @INPUT parameter preparation
###


###
### END INPUT PARAMS
###


ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
logger(sprintf("[%s] BEGIN Products Exploration script SQL_CONNECT=%d\n", ctime, SQL_CONNECT))


USE_REVOSCALER = FALSE # FALSE if using standard kmeans/etc


svr <- "server=VSQL08\\HYPERLOOP;"
db  <- "database=MachineLearning3;"
uid <- "uid=andreidi;"
pwd <- "pwd=HypML2017"
conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)
sqls <- "SELECT * FROM _Attrib_20170324"

timeit = function(strmsg, expr) {
    tm <- system.time(expr)
    stm <- sprintf("%.2f min", tm[3] / 60)
    logger(paste0(strmsg, " executed in ", stm, "\n"))
}


if ("RevoScaleR" %in% rownames(installed.packages())) {
    library(RevoScaleR)
    # Create an xdf file name
    dfXdfFileName <- file.path(getwd(), "hyperloop_saved_churn.xdf")
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
        timeit("RxOdbcData ",
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
}

##
## END DATA INPUT
##

strs <- format(round(as.numeric(object.size(df) / (1024 * 1024)), 1), nsmall = 1, big.mark = ",")
logger(sprintf("Done data downloading. Dataset size: %sMB\n", strs))
strs <- format(
            round(as.numeric(nrow(df)), 1),
            nsmall = 0, big.mark = ",",
            scientific = FALSE)
logger(sprintf("Loaded %s rows in memory dataframe\n", strs))


###
### end input parameter preparation
###

## preprocessing
PRE_VER <- "0.0.1"

logger(paste0("Begin text preprocessing v", PRE_VER, " ...\n"))
t0_prep <- proc.time()

###

t1_prep <- proc.time()
prep_time <- (t1_prep[3] - t0_prep[3]) / 60

logger(sprintf("Done preprocessing. Total time %.2f min\n", prep_time))
## end preprocessing

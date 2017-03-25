##
##
##
## @script:       Products segmentation test
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
MODULE.VERSION <- "0.2.1"

ALL_CATEGS <- c("BATISTE", "STREPSILS", "PHARMA", "COSMETICE",
                "DERMOCOSMETICE", "BABY", "NEASOCIATE")
ALL_BRANDS <- c("BrandGeneral", "DR_HART", "NUROFEN", "APIVITA", "AVENE", "L_OCCITANE", "VICHY",
                "BIODERMA", "LA_ROCHE_POSAY", "L_ERBOLARIO", "PARASINUS", "TRUSSA", "SORTIS", "NESTLE",
                "OXYANCE", "TERTENSIF", "ASPENTER", "ALPHA")
ALL_COLUMNS <- c(ALL_BRANDS, ALL_CATEGS)
###
### 
### BEGIN SQL Server R code
### 
### OBS: set SQL_CONNECT=0 for MS SQL Server Server side R scripts
###
SQL_CONNECT = 3

NR_ROWS = 1e8
FULL_DEBUG <- FALSE
# 0: Run from SLQ Server; 
# 1: use RODBC to remotely connect to sql server
# 2: use RevoScaleR to remotely connect to sql server (Microsoft R ONLY!)
# 3: use RevoScaleR to load from cache local file
# 4: csv - just load simple csv file 

##
## HELPER FUNCTIONS
##

all_log <- " "
logger <- function(text) {
    all_log <<- paste0(all_log, text)
    cat(text)
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
    logger(sprintf("Object %s [%s] size: %sMB (%s rows)\n",
                                            obj_name,
                                            class(obj)[1],
                                            strs1,
                                            strs2))
}


##
## END HELPER FUNCTIONS
##

###
### @INPUT parameter preparation
###

text_column  <- "AllText"
label_column <- "ItemName"

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
sqls <- "SELECT * FROM _TextItems"

timeit = function(strmsg, expr) {
    tm <- system.time(expr)
    stm <- sprintf("%.2f min", tm[3] / 60)
    logger(paste0(strmsg, " executed in ", stm, "\n"))
}


if ("RevoScaleR" %in% rownames(installed.packages())) {
    library(RevoScaleR)
    # Create an xdf file name
    dfXdfFileName <- file.path(getwd(), "hyperloop_saved_products.xdf")
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

timeit("Removing unknown chars Text: ", df$AllText <- iconv(df$AllText, 'Latin-9'))
timeit("Removing unknown chars Name: ", df$ItemName <- iconv(df$ItemName, 'Latin-9'))
debug_object_size(df)
toMatch <- c("invalid", "inactiv")
excluded_filter <- paste(toMatch, collapse = "|")
logger(paste0("Cleaning ",excluded_filter,"..."))
excluded_recs <- grep(excluded_filter, df$ItemName)
df <- df[-excluded_recs,]
debug_object_size(df)
if(FULL_DEBUG)
    print(head(df[nchar(df$AllText)>300,]))

library(tm)

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

    icols <- grep(test_filter, colnames(dfin))
    has_columns <- FALSE
    if (length(icols) > 0) {
        has_columns <- TRUE
        print(colnames(dfin)[icols])
    } else {
        logger("Column not found :(")
    }
    return(has_columns)
}

sparse_prc <- 0.999
timeit(sprintf("RemoveSparseTerms %.3f: ", sparse_prc),
        dtm_clean_large <- removeSparseTerms(dtm, sparse_prc))
debug_object_size(dtm_clean_large)
print(dtm_clean_large)
logger("\n")

sparse_prc <- 0.9983
timeit(sprintf("RemoveSparseTerms %.3f: ", sparse_prc),
        dtm_clean_small <- removeSparseTerms(dtm, sparse_prc))
debug_object_size(dtm_clean_small)
print(dtm_clean_small)
logger("\n")

test_brand <- c("dr","hart")

if (FULL_DEBUG) {
timeit("Prepare final BoW FULL matrix: ",
            mtxt <- as.matrix(dtm))
debug_object_size(mtxt)
dftxt <- data.frame(Name = df$ItemName, mtxt)
debug_object_size(dftxt)
df_has_columns(dftxt, test_brand)
logger("\n")
}


timeit("Prepare final BoW large dataframe: ",
            dftxt_large <- as.data.frame(as.matrix(dtm_clean_large)))
dftxt_large <- data.frame(Name = df$ItemName, dftxt_large)
debug_object_size(dftxt_large)
df_has_columns(dftxt_large, test_brand)
logger("\n")

timeit("Prepare final BoW small dataframe: ",
            dftxt_small <- as.data.frame(as.matrix(dtm_clean_small)))
dftxt_small <- data.frame(Name = df$ItemName, dftxt_small)
debug_object_size(dftxt_small)
df_has_columns(dftxt_small, test_brand)
logger("\n")

i <- 26535
print(dftxt[i, col(dftxt[i,])[which(!dftxt[i,] == 0)]])


t1_prep <- proc.time()
prep_time <- (t1_prep[3] - t0_prep[3]) / 60

logger(sprintf("Done preprocessing. Total time %.2f min\n", prep_time))
## end preprocessing

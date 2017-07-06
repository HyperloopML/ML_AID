##
##
##
## @script:       Churn Prediction System
## @v1_created:   2017.03.23
## @v2_created:   2017.06.19
## @lastmodified: 2017.06.26
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##
##

LIBS <- c("e1071","reshape2", "reshape2", "xgboost", "caret", "MASS","plyr")



User.Field <- "CUST_ID"



# use predefined repository folder or just "" for automatic current folder
USE_REPOSITORY <- "d:/GoogleDrive/_hyperloop_data/churn_v2"
USED_PROCESSING_POWER <- 0.65

DEBUG = FALSE
DEBUG_HTSS = TRUE # if true then load even if cached==true

DEMO = FALSE


SHOW_PLOTS <- FALSE



###
### PLEASE REVIEW CONNECTION PARAMS
###
svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=AIDB;"
uid <- "uid=andreidi;"
pwd <- "pwd=AIDB2017!!"
Debug.Machine.Name <- "DAMIAN"
Debug.Machine2.Name <-"ANDREEA"
Debug.Machine3.Name <- "HPC"
###
### END CONNECTION PARAMS SECTION
###



if (Sys.info()["nodename"] == Debug.Machine.Name) {
    DEBUG <- TRUE
}

if (Sys.info()["nodename"] == Debug.Machine2.Name) {
  DEBUG <- TRUE
  USE_REPOSITORY <- "C:/Users/slash/Google Drive/_hyperloop_data/churn_v2"
}

if (Sys.info()["nodename"] == Debug.Machine3.Name) {
	DEBUG <- TRUE
	USE_REPOSITORY <- "C:/GoogleDrive/_hyperloop_data/churn_v2"
}


SQL.GETCHURN <- "EXEC [SP_GET_CHURN] @TRAN_PER_ID = %d, @CHURN_PER_ID = %d"
SQL.GETSTATS <- "EXEC [SP_CUST_LIST] @TRAN_PER_ID = %d"


SQL.DATASELECTION <- "SELECT * FROM "

Target.Field <- "CHURN"



NonPredictors.Fields <- c(
                        User.Field
                        , Target.Field

                        )



conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)



MODULE.VERSION <- "1.2.1.10"
MODULE.NAME <- "CHURN_V2"
MODULE.NAME.SHORT <- "CHURN2"


DO_PARALLEL <- FALSE


Dataset.Size <- 1.0 # % out of dataset (100% usually)
Training.Size <- 0.6 # % out of Dataset.Size
Validation.Size <- 0.5 # % out of (Dataset.Size - Training.Size)

NR_ROWS <- 2e7



##
## HELPER FUNCTIONS
##
##

SQL_CONNECT = 3

HELPER_LIB_VER <- "1.5.3"


FULL_DEBUG <- FALSE

options(scipen = 999)
options(digits = 3)

get_formula <- function(lbl, vars) {
    slabel <- paste0(lbl, "~")
    sfactors <- paste(vars, collapse = "+")
    clf_formula = as.formula(paste(slabel, sfactors))
    return(clf_formula)
}

get_formula_nobias <- function(lbl, vars) {
    slabel <- paste0(lbl, "~")
    sfactors <- paste(vars, collapse = "+")
    sfactors <- paste0("0+", sfactors)
    clf_formula = as.formula(paste(slabel, sfactors))
    return(clf_formula)
}

nonzero <- function(v) {
    non_zero_ind <- which(!(v == 0))
    non_zero_list <- v[non_zero_ind]
}

get_scriptpath <- function() {
    # location of script can depend on how it was invoked:
    # source() and knit() put it in sys.calls()
    path <- NULL

    if (!is.null(sys.calls())) {
        # get name of script - hope this is consisitent!
        path <- as.character(sys.call(1))[2]
        # make sure we got a file that ends in .R, .Rmd or .Rnw
        if (grepl("..+\\.[R|Rmd|Rnw]", path, perl = TRUE, ignore.case = TRUE)) {
            return(path)
        } else {
            message("Obtained value for path does not end with .R, .Rmd or .Rnw: ", path)
        }
    } else {
        # Rscript and R -f put it in commandArgs
        args <- commandArgs(trailingOnly = FALSE)
    }
    return(path)
}

get_data_dir <- function() {
    s_data_dir <- ""
    if (USE_REPOSITORY != "") {
        s_data_dir <- USE_REPOSITORY
        dir.create(s_data_dir, recursive = TRUE, showWarnings = FALSE)
    } else {
        s_data_dir <- dirname(dirname(get_scriptpath()))
    }
    return(s_data_dir)
}

all_log <- ""
log_fnpath <- get_data_dir()
log_ctime <- format(Sys.time(), "%Y%m%d%H%M%S")
log_FN <- paste0("_log_", MODULE.NAME, "_", log_ctime, ".txt")
LogFileName <- file.path(log_fnpath, log_FN)

logger <- function(stext) {
    prefix <- ""
    if (substr(stext, 1, 1) == '\n') {
        prefix <- "\n"
        stext <- substr(stext, 2, 10000)
    }
    slog_ctime <- format(Sys.time(), "[%Y-%m-%d %H%:%M:%S]")
    slog_ctime <- sprintf("[%s]%s", MODULE.NAME.SHORT, slog_ctime)
    if (is.vector(stext)) {
        stext <- paste(stext, collapse = ",", sep = "")
    }
    stext <- paste0(prefix, slog_ctime, " ", stext)

    if (substr(stext, nchar(stext), nchar(stext) + 1) != "\n") {
        stext <- paste0(stext, "\n")
    }

    cat(stext)
    flush.console()
    all_log <<- paste(all_log, stext, sep = "")

    # Evalute the desired series of expressions inside of tryCatch
    result <- tryCatch({
        fileConn <- file(LogFileName, open = "wt")
        writeLines(all_log, fileConn)
        close(fileConn)
    },
    warning = function(war) {
        # warning handler picks up where error was generated
        cat(paste(">>LOG_WARNING:  ", war, "<<\n"))
        f <- 0
        return(f)
    },
    error = function(err) {
        # error handler picks up where error was generated
        cat(paste(">>LOG_ERROR:  ", err, "<<\n"))
        f <- -1
        return(f)
    },
    finally = {
        # NOTE:  Finally is evaluated in the context of of the inital
        # NOTE:  tryCatch block and 'e' will not exist if a warning
        # NOTE:  or error occurred.
    }) # END tryCatch


    #write(x = all_log,file = LogFileName,sep = '\n',append = FALSE)
}

logger(sprintf("Preparing helper lib v%s...\n", HELPER_LIB_VER))

timeit = function(strmsg, expr, NODATE = TRUE) {
    prf <- ""
    if (substr(strmsg, 1, 1) == "\n") {
        prf <- "\n"
        strmsg <- gsub("\n", "", strmsg)
    }
    if (!NODATE) {
        optime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
        prf <- paste0(prf, sprintf("[%s] ", optime))
    }
    strmsg <- paste0(prf, strmsg)
    logger(strmsg)
    tm <- system.time(expr)
    ftime <- tm[3]
    stm <- sprintf("%.2fs", ftime)
    logger(paste0("  executed in ", stm, "\n"))
    ftime <- round(ftime, digits = 2)
    return(ftime)
}

ZeroVar <- function(df) {
    colVars <- apply(df, 2, var)
    isConstant <- (colVars <= 0)
    colNames <- names(isConstant[isConstant == T])
    return(colNames)

}

ZeroVarCaret <- function(df) {
    res <- nearZeroVar(df, saveMetrics = T)
    colNames <- rownames(res[res[, "zeroVar"] == T,])
    return(colNames)
}


debug_object_size <- function(obj) {
    obj_name <- deparse(substitute(obj))
    strs1 <- format(round(as.numeric(object.size(obj) / (1024 * 1024)), 1), nsmall = 1, big.mark = ",")
    strs2 <- format(
    round(as.numeric(nrow(obj)), 1),
    nsmall = 0, big.mark = ",",
    scientific = FALSE)

    strs3 <- format(
    round(as.numeric(length(names(obj))), 1),
    nsmall = 0, big.mark = ",",
    scientific = FALSE)

    logger(sprintf("  [Object %s [%s] size: %sMB (%s rows by %s cols)]\n",
                 obj_name,
                 class(obj)[1],
                 strs1,
                 strs2,
                 strs3))
}

## deparse best tuning parameters
get_tune_params <- function(caret_model) {
    best <- caret_model$bestTune
    sparams <- ""
    for (i in 1:length(names(best))) {
        sparams <- paste0(sparams, sprintf("%s=%.2f ", names(best[i]), best[i]))
    }
    return(sparams)
}
## end deparse tuning params


install_and_load <- function(libraries) {
    for (i in libraries) {
        if (!is.element(i, .packages(all.available = TRUE))) {
            logger(sprintf("Installing package: %s", i))
            install.packages(i)
        }
        logger(sprintf("Loading package: %s", i))
        if (!(paste0("package:", i) %in% search()))
            library(i, character.only = TRUE, verbose = TRUE)
        }
}

# setup environment
install_and_load(LIBS)
#

p_cluster <- 0
setup_paralel_env <- function() {

    logger(sprintf("Starting parallel processing backend at %.2f%%\n", USED_PROCESSING_POWER * 100))

    install_and_load(c("doParallel", "foreach"))

    ### Register parallel backend
    avail_cores <- detectCores() # available cores
    p_cluster <<- makeCluster(as.integer(avail_cores * USED_PROCESSING_POWER))
    registerDoParallel(p_cluster)
    logger(sprintf("[Parallel backend] Cores registered: %d\n", getDoParWorkers()))
    logger(sprintf("[Parallel backend] Environment: %s\n", getDoParName()))
    logger(sprintf("[Parallel backend] Backend version: %s\n", getDoParVersion()))
}

save_df <- function(df, sfn = "", simple_name = FALSE) {
    if (FULL_DEBUG)
        logger("\nSaving data...\n")
    file_db_path <- get_data_dir()
    if (FULL_DEBUG)
        logger(sprintf(" Used data directory: %s\n", file_db_path))
    FN <- paste0(MODULE.NAME, "_v", gsub("\\.", "_", MODULE.VERSION), "_", log_ctime, "_data.csv")
    if (sfn != "")
        if (simple_name)
            FN <- sfn else
                FN <- paste0(sfn, "_", log_ctime, "_data.csv")
            FileName <- file.path(file_db_path, FN)
    timeit(sprintf(" Saving File:[%s] ...", FileName),
           write.csv(x = df, file = FileName, row.names = TRUE))
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

load_file <- function(sfile) {
    file_db_path <- get_data_dir()
    fn <- file.path(file_db_path, sfile)
    if (file.exists(fn)) {
        logger(sprintf("  Loading %s",fn))
        df <- read.csv(fn, row.names = 1)
    } else {
      logger(sprintf("  Can't find file %s",fn))
      df <- data.frame()
    }
    return(df)
}


load_df <- function(str_table, caching = TRUE, use_sql = FALSE) {

    Data.FileName <- gsub("\\[", "_", gsub("\\]", "_", str_table))
    Data.FileName <- gsub(" ", "_", gsub("\\*", "_", Data.FileName))
    Data.FileName <- gsub(",", "_", gsub("\\.", "_", Data.FileName))
    Data.FileName <- paste0(Data.FileName, ".xdf")
    str_sql <- paste(SQL.DATASELECTION, str_table)

    if (use_sql)
        str_sql <- str_table


    if ("RevoScaleR" %in% rownames(installed.packages())) {
        library(RevoScaleR)
        # Create an xdf file name
        file_db_path <- get_data_dir()
        file_db_path <- file.path(file_db_path, "_dbcache")
        if (DEBUG)
            logger(sprintf("  Current dbcache directory: %s", file_db_path))
        if (!dir.exists(file_db_path)) {
            logger(sprintf("   Creating path: %s", file_db_path))
            dir.create(file_db_path, recursive = TRUE)
        }
        dfXdfFileName <- file.path(file_db_path, Data.FileName)
        logger(sprintf("  XDF File: %s\n", dfXdfFileName))
    }

    if (SQL_CONNECT == 1) {
        logger(sprintf("  Connecting MSSQLServer [%s]...\n", conns))
        library(RODBC)
        channel <- odbcDriverConnect(conns)

        timeit(sprintf("  Downloading [%s] table... ", str_sql),
           df_in <- sqlQuery(channel, paste(str_sql)))

        odbcClose(channel)
        logger("  Done data downloading.\n")
    } else if (SQL_CONNECT > 1) {
        if ((SQL_CONNECT == 2) || !file.exists(dfXdfFileName) || !caching) {
            logger(sprintf("  R evoScaleRConn MSSQLServer [%s]...\n", conns))

            dsOdbcSource <- RxOdbcData(sqlQuery = str_sql,
                                     connectionString = conns)


            # Import the data into the xdf file
            timeit(sprintf("  Downloading [rxImport: %s]...\n", str_sql),
                 rxImport(dsOdbcSource, dfXdfFileName, overwrite = TRUE))
        } else
            logger("  Bypassing SQL Server. Loading data directly from XDF file...\n")
        # Read xdf file into a data frame
        timeit("  RevoScaleR loading dataframe [rxDataStep]... ",
               df_in <- rxDataStep(inData = dfXdfFileName,
                                   numRows = NR_ROWS,
                                   maxRowsByCols = 20000000 * 200
               ))
        if (FALSE)
            unlink(dfXdfFileName)
        }
    df_in[is.na(df_in)] <- 0
    debug_object_size(df_in)
    return(data.frame(df_in))
}


rx_load_sql <- function(str_sql) {

    library(RevoScaleR)
    if (FULL_DEBUG)
        logger(sprintf("  RevoScaleRConn MSSQLServer [%s]...\n", conns))

    dsOdbcSource <- RxOdbcData(sqlQuery = str_sql,
                                connectionString = conns)


    # Import the data into the xdf file
    timeit(sprintf("  Loading dataframe [rxImport: %s]...", str_sql),
           df_in <- rxImport(inData = dsOdbcSource, reportProgress = 0))


    df_in[is.na(df_in)] <- 0
    debug_object_size(df_in)
    return(data.frame(df_in))
}

normalize <- function(x) {
    mx <- max(x)
    mn <- min(x)
    if (mx == 0 && mn == 0) mx <- 1
    return((x - mn) / (mx - mn))
}

save_plot <- function(sfn) {
    if (SHOW_PLOTS) {
        file_db_path <- get_data_dir()
        stime <- format(Sys.time(), "%Y%m%d%H%M%S")
        FN <- file.path(file_db_path, paste0(stime, "_", sfn, "_PLOT.png"))
        logger(sprintf(" Saving plot: %s\n", FN))
        dev.print(device = png, file = FN, width = 1024, height = 768)
    }
}

ExecSP <- function(sSQL) {

    logger(sprintf(" Executing [%s]\n", sSQL))
    res <- RxSqlServerData(sqlQuery = sSQL, connectionString = conns) #rx_load_sql(sSQL) 
    return(res)
}

LoadBatch <- function(ssql, cached = T) {
    df_batch <- NULL
    if (cached && DEBUG) {
        logger(" LoadBatch: Trying to load from cache...")
        df_batch <- load_df(ssql, use_sql = TRUE, caching = TRUE)
    } else {
        logger(" LoadBatch: DIRECT SQL LOAD...")
        df_batch <- rx_load_sql(ssql)
    }
    return(df_batch)
}

LoadBatchMaybe <- function(ssql, cached = T) {
    df_batch <- NULL
    if (cached && (!DEBUG_HTSS) && (DEBUG)) {
        logger(" LoadBatchMaybe: Trying to load from cache...")
        df_batch <- load_df(ssql, use_sql = TRUE, caching = TRUE)
    } else {
        logger(" LoadBatchMaybe: DIRECT SQL LOAD...")
        df_batch <- rx_load_sql(ssql)
    }
    return(df_batch)
}

LoadCSV <- function(sFN)
{
  logger(sprintf(" Loading CSV [%s]",sFN))
  return(load_file(sFN))
}

RSquared <- function(y, yhat) {
    ymean <- mean(y)
    rsq <- 1 - sum((yhat - y) ** 2) / sum((y - ymean) ** 2)
    return(rsq)
}


#########################
##
##
## END HELPER FUNCTIONS
##
#########################



LoadChurnData <- function(iTransPer, iChurnPer) {
    logger(sprintf("Loading period %d churn data (trans=%d, churn=%d)...",iTransPer,iTransPer,iChurnPer))
    sSQL <- sprintf(SQL.GETCHURN, iTransPer, iChurnPer)
    df <- LoadBatch(sSQL)
    logger(sprintf("Done loading period %d churn data (trans=%d, churn=%d).", iTransPer, iTransPer, iChurnPer))
    return(df)
}

LoadCustomerStatInfo <- function(iPeriod){
    logger(sprintf("Loading stat info for period %d...", iPeriod))
    sSQL <- sprintf(SQL.GETSTATS, iPeriod)
    df <- LoadBatch(sSQL)
    logger(sprintf("Done loading stat info for period %d.", iPeriod))
    return(df)
}

GetPredictors <- function(df) {
    all_fields <- colnames(df)
    pred_fields <- setdiff(all_fields, NonPredictors.Fields)
    return(pred_fields)
}



df_stats_2015 <- LoadCustomerStatInfo(2)
df_stats_2016 <- LoadCustomerStatInfo(3)

df_churn_2016 <- LoadChurnData(2, 3)

Predictor.Fields <- GetPredictors(df_churn_2016)

X_full <- df_churn_2016[, Predictor.Fields]
y_full <- as.numeric(df_churn_2016[, Target.Field])

nrounds = 5

timeit(sprintf(" Directly training SIMPLE XGB for %d rounds", nrounds),
                            xgb2 <- xgboost(data = as.matrix(X_full),
                                                label = y_full,
                                                missing = 0,
                                                verbose = 10,
                                                nround = nrounds,
                                                objective = "binary:logistic"))

p0 <- as.numeric(predict(xgb2, as.matrix(X_full)) > 0.5)
cf <- confusionMatrix(p0, y_full)
logger("Vanilla result:")
print(cf)

xgb.grid <- expand.grid(nrounds = c(50),
                        max_depth = c(2, 4, 6),
                        eta = c(0.15, 0.2, 0.3, 0.5),
                        gamma = c(0.001, 0.005, 0.01, 0.05, 0.1),
                        colsample_bytree = c(0.5, 1),
                        min_child_weight = c(1, 2, 5, 10))

trCtrl <- trainControl('adaptive_cv', number = 5, repeats = 5, verboseIter = TRUE)
timeit(" Tuning ...",
                    best_fit <- train(x = X_full,
                                      y = y_full,
                                      method = "xgbTree",
                                      tuneGrid = xgb.grid,
                                      trControl = trCtrl,
                                      metric= "Kappa")
                    )

xgb1 <- best_fit$finalModel
cv_max_depth = best_fit$bestTune[1, 2]
cv_eta = best_fit$bestTune[1, 3]
cv_gamma = best_fit$bestTune[1, 4]
cv_colsample_bytree = best_fit$bestTune[1, 5]
cv_min_child_weight = best_fit$bestTune[1, 6]
nrounds = 500

timeit(sprintf(" Directly training TUNED XGB for %d rounds", nrounds),
                            xgb2 <- xgboost(data = as.matrix(X_full),
                                                label = y_full,
                                                missing = 0,
                                                verbose = 0,
                                                nround = nrounds,
                                                eta = cv_eta,
                                                gamma = cv_gamma,
                                                colsample_bytree = cv_colsample_bytree,
                                                min_child_weight = cv_min_child_weight))

p1 <- as.numeric(predict(xgb1, as.matrix(X_full))>0.5)
p2 <- as.numeric(predict(xgb2, as.matrix(X_full))>0.5)

cf <- confusionMatrix(p1,y_full)
logger("Caret result:")
print(cf)

cf <- confusionMatrix(p2, y_full)
logger("500 rounds result:")
print(cf)

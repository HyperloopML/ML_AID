##
##
##
## @script:       Microsegmentation Content Based Recommender V2 System
## @v1_created:   2017.03.23
## @v2_created:   2017.05.23
## @lastmodified: 2017.06.15
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##
##

LIBS <- c("reshape2", "reshape2", "xgboost", "caret", "MASS")



TIME_FRAME_ID <- "3"
SEGMENT_MODEL_ID <- "39"
User.Field <- "MICRO_SGM_ID"
MicroSegment.Field <- "MICRO_SGM_ID"


# use predefined repository folder or just "" for automatic current folder
USE_REPOSITORY <- "d:/GoogleDrive/_hyperloop_data/microbehavior"
USED_PROCESSING_POWER <- 0.65

DEBUG = FALSE
DEBUG_HTSS = TRUE # if true then load even if cached==true

DEMO = FALSE


SGD.ALPHAS <- c(0.0001) #, 0.0005, 0.0001)
SGD.EPOCHS <- c(100) #, 90)
SGD.LAMBDAS <- c(0.001) #, 0.1)
SGD.SCALE_MIN_MAX <- c(0)
SGD.MOMENTUMS <- c(1)
NORMALEQ.LAMBDAS <- c(0.5) #,1,1.5,2,2.5,3,3.5)
LAMBDA_NORMAL_EQ <- 3

test_df <- NULL

ENABLE_SGD <- TRUE
ENABLE_STD <- FALSE
ENABLE_CNF <- FALSE
ENABLE_LMB <- FALSE
ENABLE_LMN <- FALSE

REDUCE_FEATS <- FALSE

SHOW_PLOTS <- FALSE



###
### PLEASE REVIEW CONNECTION PARAMS
###
svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=AIDB;"
uid <- "uid=andreidi;"
pwd <- "pwd=AIDB2017!!"
Debug.Machine.Name <- "DAMIAN"
###
### END CONNECTION PARAMS SECTION
###



if (Sys.info()["nodename"] == Debug.Machine.Name) {
    DEBUG <- TRUE
}

# get segmentation tranzactions based on segmentation id
SQL.TRANSCOUNT.SP <- "EXEC [SP_TRAN_COUNT] @SGM_ID=%d, @FULL=%d"

# get microsegments list based on a segmentation id
SQL.MICROLIST.SP <- "EXEC [SP_MICRO_SGM_LIST] @SGM_ID=%d"

# get microsegment data 
SQL.GETMICRO.SP <- "EXEC [SP_SGM_MICRO_ITEM_PROP] @SGM_ID = %d, @MICRO_SGM_ID = %d, @ITEM_ID = NULL, @CUST_ID = NULL, @ENABLED = NULL, @NON_RX = 1"

# initialize new model and return model ID
SQL.INITMODEL.SP <- " EXEC [SP_ADD_BEV_MODEL] @SGM_ID=%d, @MODEL_NAME='%s'"

# update model data by ID %1=model %2=error %3=runtimetime
SQL.UPDATEMODEL.SP <- "EXEC [SP_UPD_BEV_MODEL] @MODEL_ID=%d, @ERR1_NAME='%s', @ERR1_VAL=%f, @ERR2_NAME='%s', @ERR2_VAL=%f, @RUN_TIME=%f"

# add model info by model ID (params %1=model %2=micusrid, %3=bestattr1, %4=bestattr2, %5=bestattr3, %6=bestattr4, %7=bestattr5, %8=error
SQL.ADDMODELINFO.SP <- "EXEC [SP_ADD_BEV_MODEL_DETAIL] @MODEL_ID=%d,@MICRO_SGM_ID=%d,@BEST_P_1='%s',@BEST_P_2='%s',@BEST_P_3='%s',@BEST_P_4='%s',@BEST_P_5='%s',@BEST_P_6='%s',@BEST_P_7='%s',@BEST_P_8='%s',@BEST_P_9='%s',@BEST_P_10='%s',@ERR1_NAME='%s',@ERR1_VAL=%.3f,@ERR2_NAME='%s',@ERR2_VAL=%.3f, @BEST_P_IMP_1=%f,@BEST_P_IMP_2=%f,@BEST_P_IMP_3=%f,@BEST_P_IMP_4=%f,@BEST_P_IMP_5=%f,@BEST_P_IMP_6=%f,@BEST_P_IMP_7=%f,@BEST_P_IMP_8=%f,@BEST_P_IMP_9=%f,@BEST_P_IMP_10=%f"

# get all articles dataframe
SQL.GETART.SP <- "EXEC [SP_GET_ITEMS_PROPS]"

Target.Field <- "AMOUNT"
Target.Field.2 <- "QTY"
Target.Field.3 <- "COUNT"
Product.Field <- "ITEM_ID"
Product.Name <- "ITEM_NAME"
Enabled.Field <- "ENABLED"
Period.Field <- "PER_ID"
Segmentation.Field <- "SGM_ID"
Model.Field <- "MODEL_ID"

OutputID.Field <- "UID"

Preds.Table <- "BEV_PRED_ITEM"
Coefs.Table <- "BEV_MODEL_PROP_COEFS"


NonPredictors.Fields <- c(
                        User.Field
                        , Target.Field
                        , Product.Field
                        , Product.Name
                        , Target.Field.2
                        , Target.Field.3
                        , Period.Field
                        , Model.Field
                        , Segmentation.Field
                        , Enabled.Field
                        )



conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)


SQL_CONNECT = 3
# 1: use RODBC to remotely connect to sql server
# 2: use RevoScaleR to remotely connect to sql server (Microsoft R ONLY!)
# 3: XDF




MODULE.VERSION <- "2.0.2.3"
MODULE.NAME <- "BEHAVIOR_RECOMMENDATIONS_V2"
MODULE.NAME.SHORT <- "BEHAV"


TABLE.OUTPUTVECTORS <- "BEV_SGM_MICRO"

FILE.MATRIX = "BEHAVIOR_MATRIX"
FILE.SCORES = "SCORES_MATRIX"




SQL.DATASELECTION <- "SELECT * FROM"



DO_PARALLEL <- FALSE


Dataset.Size <- 1.0 # % out of dataset (100% usually)
Training.Size <- 0.6 # % out of Dataset.Size
Validation.Size <- 0.5 # % out of (Dataset.Size - Training.Size)

NR_ROWS <- 2e7



##
## HELPER FUNCTIONS
##
##


HELPER_LIB_VER <- "1.5.1"


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
        df <- read.csv(fn, row.names = 1)
    } else {
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
        if (FULL_DEBUG)
            logger(sprintf("  Current dbcache directory: %s\n", file_db_path))
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

#########################
##
##
## END HELPER FUNCTIONS
##
#########################

LoadItems <- function() {
    logger("Loading items dataframe ...")
    ssql <- SQL.GETART.SP
    df <- LoadBatch(ssql)
    return(df)
}

LoadMicrosegment <- function(sgm_id, micro_sgm_id, cached = T) {
    ssql <- sprintf(SQL.GETMICRO.SP, sgm_id, micro_sgm_id)
    return(LoadBatch(ssql, cached = cached))
}

SaveModelMetadata <- function(sgm_id, ModelName) {
    ssql <- sprintf(SQL.INITMODEL.SP, sgm_id, ModelName)
    df_model <- LoadBatchMaybe(ssql)
    return(df_model[1, 1])
}

UpdateModelMetadata <- function(model_id, err1_name,err1_val,err2_name,err2_val, model_time) {
    ssql <- sprintf(SQL.UPDATEMODEL.SP, model_id, err1_name, err1_val, err2_name, err2_val, model_time)
    LoadBatchMaybe(ssql) #ExecSP(ssql)
}

SaveSubmodelMeta <- function(model_id, MicroSegment,
                             A1, A2, A3, A4, A5, A6, A7, A8, A9, A10,
                             E1_name, E1_val, E2_name, E2_val,
                             fA1, fA2, fA3, fA4, fA5, fA6, fA7, fA8, fA9, fA10) {
    logger(sprintf("Saving submodel meta for MODEL:%d MICRO:%d %s:%.2f %s:%.2f A1:%s A1f:%.3f A2:%s A2f:%.3f",
                                                model_id,
                                                MicroSegment,
                                                E1_name,
                                                E1_val,
                                                E2_name,
                                                E2_val,
                                                A1, fA1, A2,fA2))
    ssql <- sprintf(SQL.ADDMODELINFO.SP,
                    model_id, MicroSegment,
                    A1, A2, A3, A4, A5, A6, A7, A8, A9, A10,
                    E1_name, E1_val, E2_name, E2_val,
                    fA1, fA2, fA3, fA4, fA5, fA6, fA7, fA8, fA9, fA10)

    LoadBatchMaybe(ssql) # ExecSP(ssql)
}

GetTransactionCount <- function(model_id) {
    FULL = FALSE

    ssql <- sprintf(SQL.TRANSCOUNT.SP, model_id, FULL)
    df_count <- LoadBatch(ssql)
    return(df_count)
}

GetMicrosegmentList <- function(model_id) {
    ssql <- sprintf(SQL.MICROLIST.SP, model_id)
    df_micros <- LoadBatch(ssql)
    return(df_micros)
}

UploadToSQL <- function(dest_table, source_df) {
    svrTable <- RxSqlServerData(table = dest_table, connectionString = conns)
    logger(sprintf(" rxDataStep Uploading dataframe with size (%s) to table [%s] ...", toString(dim(source_df)), dest_table))
    rxDataStep(inData = source_df, outFile = svrTable, append = "rows")
    logger(" Done uploading.")
}



scale_minmax <- function(df) {
    df_norm <- as.data.frame(lapply(df, normalize))
    return(df_norm)
}

STOHASTIC <- TRUE

SCALE_MIN_MAX <- FALSE

cost_list_SGD <- c()
cost_scores_SGD <- c()
cost_index_SGD <- 0
CostFunctionSGD <- function(H, Y) {

    cost_index_SGD <<- cost_index_SGD + 1

    cdiff <- (H - Y)
    cdiff <- cdiff ** 2
    cost <- sum(cdiff) / length(cdiff)
    avg_cost <- -1
    if (!is.finite(cost)) {
        logger("SGD Cost generated not finite value!\n")
        avg_cost <- cost_scores_SGD[cost_index_SGD - 1]
    } else {
        cost_list_SGD[cost_index_SGD] <<- cost
        m <- length(cost_list_SGD)
        avg_cost <- sum(cost_list_SGD) / m
        cost_scores_SGD[cost_index_SGD] <<- avg_cost
    }
    return(avg_cost)
}

cost_list_STD <- c()
cost_scores_STD <- c()
cost_index_STD <- 0
CostFunctionSTD <- function(H, Y) {

    cost_index_STD <<- cost_index_STD + 1

    cdiff <- (H - Y)
    cdiff <- cdiff ** 2
    cost <- sum(cdiff) / length(cdiff)
    avg_cost <- -1

    if (!is.finite(cost)) {
        logger("STD Cost generated not finite value!\n")
        avg_cost <- cost_scores_STD[cost_index_STD - 1]
    } else {
        cost_list_STD[cost_index_STD] <<- cost
        m <- length(cost_list_STD)
        avg_cost <- sum(cost_list_STD) / m
        cost_scores_STD[cost_index_STD] <<- avg_cost
    }

    return(avg_cost)
}


cnf_cost_list <- c()
cnf_cost_scores <- c()
cnf_cost_index <- 0
CostFunctionCONF <- function(H, Y) {

    cnf_cost_index <<- cnf_cost_index + 1
    cost <- (H - Y) ** 2
    if (!is.finite(cost)) {
        logger("CNF Cost generated not finite value!\n")
        avg_cost <- cnf_cost_scores[cnf_cost_index - 1]
    } else {
        cnf_cost_list[cnf_cost_index] <<- cost
        m <- length(cnf_cost_list)
        avg_cost <- sum(cnf_cost_list) / m
        cnf_cost_scores[cnf_cost_index] <<- avg_cost
    }
    return(avg_cost)
}

train_costs <- c()
train_costs_index <- 0
cost_func_launch <- 0
train_cost_steps <- 0

RSquared <- function(y, yhat) {
    ymean <- mean(y)
    rsq <- 1 - sum((yhat - y) ** 2) / sum((y - ymean) ** 2)
    return(rsq)
}

SimpleCost <- function(coefs, data, y) {
    H <- data %*% coefs
    h2 <- (H - y) ** 2
    nr_obs <- length(h2)
    cost <- sum(h2) / nr_obs
    #cost <- cost + 
    if (!is.finite(cost)) {
        logger("CNF Cost generated not finite value!\n")
        cost <- 5000
    }
    return(cost)
}

GetRMSE <- function(y, yhat) {
    return(sqrt(mean((yhat - y) ** 2)))
}

GetRPRC <- function(y, yhat) {
    res <- abs(yhat - y)
    y[y == 0] <- 1
    res_prc <- res / y
    return(mean(res_prc))
}

TrainCost <- function(coefs, data, y) {
    cost <- -1

    if (cost_func_launch == 0) {
        train_costs <<- c()
        train_costs_index <<- 0
    }

    cost_func_launch <<- cost_func_launch + 1

    if ((cost_func_launch %% train_costs_steps) == 0) {
        cost <- SimpleCost(coefs, data, y)
        train_costs_index <<- train_costs_index + 1
        train_costs[train_costs_index] <<- cost
    }

    return(cost)
}

CheckPreds <- function(UserID, coefs, data, lm, col_names, UserInfo) {
    logger("\nSUMMARY OF THE TRAINING PROCESS")
    y <- data[, Target.Field]
    nr_obs <- length(y)
    prods <- data[, Product.Field]

    if (SCALE_MIN_MAX) {
        items_coefs <- scale_minmax(data[, Predictor.Fields])
        data_mat <- as.matrix(items_coefs)
    } else {
        data_mat <- as.matrix(data[, Predictor.Fields])
    }

    c1 <- length(coefs[[1]])
    c2 <- ncol(data_mat)
    if (c1 != c2) {
        cnames <- c("BIAS", colnames(data_mat))
        data_mat <- cbind(rep(1, nrow(data_mat)), data_mat) # add bias intercept term
        colnames(data_mat) <- cnames
    }
    df <- data.frame(Prods = prods, Count = y)
    colnames(df) <- c("ProdID", Target.Field)
    df_rmse <- data.frame()
    df_rmse[1, "INFO"] <- UserInfo
    c <- 1
    smicr <- paste0("M", UserID)
    for (i in 1:length(coefs)) {
        coef_vect <- coefs[[i]]
        if (!is.null(coef_vect)) {
            # assume 1 is SGD
            yhat <- data_mat %*% coef_vect
            col <- col_names[c]
            df[, col] <- yhat

            rsq <- RSquared(y, yhat)
            err <- GetRMSE(y, yhat)
            err_prc <- GetRPRC(y, yhat)
            col1 <- paste0(col, "_RE")
            col2 <- paste0(col, "_R2")
            col3 <- paste0(col, "_PR")
            df_rmse[1, col1] <- round(err, digits = 2)
            df_rmse[1, col2] <- round(rsq, digits = 2)
            df_rmse[1, col3] <- round(err_prc, digits = 2)

            if (i == 1) {
                if (err < SGD_BEST_COST) {
                    SGD_BEST_COST <<- err
                    save_plot(smicr)
                }
            }
        }
        c <- c + 1
    }
    for (i in 1:length(lm)) {
        clf <- lm[[i]]
        if (!is.null(clf)) {
            col <- col_names[c]
            if (col == "XGB") {
                yhat <- predict(clf, as.matrix(data[, Predictor.Fields]))
            } else {
                yhat <- predict(clf, data)
            }
            df[, col] <- yhat

            rsq <- RSquared(y, yhat)
            err <- GetRMSE(y, yhat)
            err_prc <- GetRPRC(y, yhat)
            col1 <- paste0(col, "_RE")
            col2 <- paste0(col, "_R2")
            col3 <- paste0(col, "_PR")
            df_rmse[1, col1] <- round(err, digits = 2)
            df_rmse[1, col2] <- round(rsq, digits = 2)
            df_rmse[1, col3] <- round(err_prc, digits = 2)
        }
        c <- c + 1
    }

    #df_rmse[1, "MOM_SGD"] <- SGD_USE_MOMENTUM
    #df_rmse[1, "ALPHA"] <- SGD_ALPHA
    #df_rmse[1, "LAMBDA"] <- SGD_LAMBDA
    #df_rmse[1, "SCALE"] <- SCALE_MIN_MAX
    df_rmse[1, "TIME"] <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    df_rmse[1, "RED_F"] <- REDUCE_FEATS
    df_rmse[1, "EPOCHS"] <- SGD_EPOCHS


    sFN <- paste0("RMSE_", smicr, ".csv")

    df_hist <- load_file(sFN)
    df_rmse <- rbind(df_rmse, df_hist)
    cat("\nHEAD:\n")
    print(head(df, 6))
    cat("\nTAIL:\n")
    print(tail(df, 2))
    cat("\nRMSE:\n")
    print(df_rmse)
    save_df(df_rmse, sfn = sFN, simple_name = TRUE)
}


TrainNormalEquation <- function(data, target_col, var_cols) {
    items_coefs <- as.data.frame(data[, var_cols])
    y_train <- data[, target_col]


    if (SCALE_MIN_MAX) {
        items_coefs <- scale_minmax(items_coefs)
    }


    coefs_names <- c("BIAS", var_cols)
    items_coefs <- as.matrix(items_coefs)
    X <- cbind(rep(1, nrow(items_coefs)), items_coefs)
    colnames(X) <- coefs_names
    if (DEBUG && FALSE) {
        cat("\nNormal Eq X:\n")
        print(X[1:5, 1:9])
    }

    lambda = LAMBDA_NORMAL_EQ
    I <- diag(ncol(X))

    Theta <- base::t(ginv(base::t(X) %*% X + lambda * I) %*% base::t(X) %*% y_train)
    micro_coefs_neq <- as.vector(Theta)
    names(micro_coefs_neq) <- coefs_names
    if (DEBUG) {
        test_df <<- X %*% micro_coefs_neq
        y_train <- data[, target_col]
        J <- SimpleCost(micro_coefs_neq, X, y_train)
        logger(sprintf("NormalEq on (%s) with lambda:%.2f MSE:%.1f ", toString(dim(X)), lambda, J))
    }
    return(micro_coefs_neq)
}

SGD_ALPHA <- 0.0005
SGD_EPOCHS <- 100
SGD_LAMBDA <- 0.1
SGM_MOMENTUM <- 0.9
SGD_BATCH <- 128
SGD_USE_MOMENTUM <- 0
SGD_AVERAGE <- 1

SGD_BEST_COST <- 1e20

SGD_CURRENT_ID <- -1


TrainMicromodelSGD <- function(data, target_col, var_cols) {

    if (DEBUG) {
        cost_func_launch <<- 0 # reset cost function
    }

    y_train <- data[, target_col]
    items_coefs <- as.data.frame(data[, var_cols])
    #items_coefs <- sapply(items_coefs,as.numeric)

    if (SCALE_MIN_MAX) {
        items_coefs <- scale_minmax(items_coefs)
    }

    micro_coefs_sgd <- as.vector(rep(0, length(var_cols) + 1))
    coefs_names <- c("BIAS", var_cols)
    items_coefs <- as.matrix(items_coefs)
    items_coefs <- cbind(rep(1, nrow(items_coefs)), items_coefs)

    alpha_sgd <- SGD_ALPHA
    lambda_sgd <- SGD_LAMBDA
    epochs <- SGD_EPOCHS
    batch_size <- SGD_BATCH
    momentum_speed <- SGM_MOMENTUM
    momentum <- 0

    ###
    ### stohastic gradient descent for content based filtering
    ###
    total_size <- nrow(data)
    iters <- nrow(data) %/% batch_size
    if (iters <= 10) {
        batch_size <- 16
        iters <- nrow(data) %/% batch_size
    } else {
        iters <- iters + 1
    }

    steps <- iters %/% 1
    train_costs_steps <<- (epochs * iters) %/% 100



    for (epoch in 1:epochs) {
        for (i in 1:iters) {
            soffs <- ((i - 1) * batch_size + 1) %% (total_size - batch_size)
            eoffs <- soffs + batch_size
            batch_items_coefs <- items_coefs[soffs:eoffs,]
            batch_y <- y_train[soffs:eoffs]
            if (FULL_DEBUG && (i %% steps) == 0) {
                names(micro_coefs_sgd) <- coefs_names
                non_zero_coefs_std <- nonzero(micro_coefs_sgd)
                cat("Non zero coefs SGD:\n")
                print(non_zero_coefs_std[1:5])
            }
            item_coefs_matrix <- as.matrix(batch_items_coefs)
            H_sgd <- (item_coefs_matrix %*% micro_coefs_sgd)
            Y_sgd <- batch_y
            if (DEBUG) {
                #J <- CostFunctionSGD(H_sgd, Y_sgd)
                J <- TrainCost(micro_coefs_sgd, items_coefs, y_train)
            }

            ydiff <- H_sgd - Y_sgd
            tmp_micro_coefs = micro_coefs_sgd
            tmp_micro_coefs[1] = 0
            grad_sgd <- base::t(item_coefs_matrix) %*% ydiff + lambda_sgd * tmp_micro_coefs

            if (SGD_AVERAGE == 1)
                grad_sgd <- grad_sgd / batch_size

            if (SGD_USE_MOMENTUM == 1) {
                momentum <- momentum_speed * momentum + grad_sgd
            } else {
                momentum <- grad_sgd
            }

            micro_coefs_sgd <- micro_coefs_sgd - alpha_sgd * momentum
        }
    }

    if (DEBUG) {
        #plot_scores <- cost_scores_SGD
        J <- SimpleCost(micro_coefs_sgd, items_coefs, y_train)
        plot_scores <- train_costs
        cost_scores_SGD <<- plot_scores
        nrsco <- length(plot_scores)
        cidx <- c(1, nrsco %/% 5, nrsco %/% 4, nrsco %/% 3, nrsco %/% 2, nrsco %/% 1.4, nrsco %/% 1.2, nrsco)
        sinf <- paste0(" ID:", SGD_CURRENT_ID,
                       " A:", alpha_sgd,
                       " E:", epochs,
                       " L:", lambda_sgd,
                       " M:", SGD_USE_MOMENTUM)

        logger(sprintf(paste0(sinf, " C: %s"), toString(plot_scores[cidx])))

        if (SHOW_PLOTS) {

            # plot device setup
            # for first plot in series !
            if (length(dev.list()) != 0)
                dev.off()
            par(oma = c(0, 0, 2, 0))
            par(mfrow = c(2, 2))

            plot(plot_scores,
             type = "l",
            #asp = 1,
             main = paste0("SGD J:", round(J, digits = 2),
                           " LJ:", round(tail(plot_scores, 1), digits = 2)
                           )
             )

            title(main = sinf, outer = TRUE)
        }

    }
    names(micro_coefs_sgd) <- coefs_names
    return(micro_coefs_sgd)
}


TrainMicromodelSTD <- function(data, target_col, var_cols) {

    if (DEBUG) {
        cost_func_launch <<- 0 # reset cost function

        if (FALSE)
            data <- data[order(data[, target_col], decreasing = TRUE),]
        }



    y_train <- data[, target_col]
    items_coefs <- as.data.frame(data[, var_cols])
    #items_coefs <- sapply(items_coefs,as.numeric)

    if (SCALE_MIN_MAX) {
        items_coefs <- scale_minmax(items_coefs)
    }

    micro_coefs_std <- as.vector(rep(0, length(var_cols) + 1))
    coefs_names <- c("BIAS", var_cols)
    names(micro_coefs_std) <- coefs_names
    items_coefs <- as.matrix(items_coefs)
    items_coefs <- cbind(rep(1, nrow(items_coefs)), items_coefs)

    alpha_std <- SGD_ALPHA
    lambda_std <- SGD_LAMBDA
    epochs <- SGD_EPOCHS %/% 2


    train_costs_steps <<- (epochs * nrow(data)) %/% 100

    for (epoch in 1:epochs) {
        if (STOHASTIC) {
            ###
            ### stohastic gradient descent for content based filtering
            ###

            steps <- nrow(data) %/% 1

            for (i in 1:nrow(data)) {
                if (FULL_DEBUG && (i %% steps) == 0) {
                    non_zero_coefs_std <- nonzero(micro_coefs_std)
                    cat("Non zero coefs STD:\n")
                    print(non_zero_coefs_std[1:5])
                }
                item_coefs_vect <- items_coefs[i,] #as.vector(sapply(items_coefs[i,], as.numeric))
                H_std <- (micro_coefs_std %*% item_coefs_vect)[1]
                Y_std <- y_train[i]
                if (DEBUG) {
                    #J <- CostFunctionSTD(H_std, Y_std)
                    J <- TrainCost(micro_coefs_std,
                                   items_coefs,
                                   y_train)
                }
                tmp_micro_coefs = micro_coefs_std
                tmp_micro_coefs[1] = 0

                grad_std <- (H_std - Y_std) * item_coefs_vect + lambda_std * tmp_micro_coefs

                micro_coefs_std <- micro_coefs_std - alpha_std * grad_std
            }
        } else {
            ###
            ### full batch gradient descent for content based filtering
            ###

            Theta <- (X_T %*% X + lambda * I) ^ -1 * X_T * Y

        }
    }


    if (DEBUG) {
        #plot_scores <- cost_scores_STD
        J <- SimpleCost(micro_coefs_std,
                                   items_coefs,
                                   y_train)
        plot_scores <- train_costs
        cost_scores_STD <<- plot_scores
        nrsco <- length(plot_scores)
        cidx <- c(1, nrsco %/% 5, nrsco %/% 4, nrsco %/% 3, nrsco %/% 2, nrsco %/% 1.4, nrsco %/% 1.2, nrsco)
        sinf <- paste0(" STD A:", alpha_std,
                   " E:", epochs,
                   " L:", lambda_std,
                   " C: %s")

        logger(sprintf(sinf, plot_scores[cidx]))
        if (SHOW_PLOTS) {
            plot(plot_scores,
             type = "l",
            #asp = 1,
             main = paste0("STD J:", round(J, digits = 2),
                           " LJ:", round(tail(plot_scores, 1), digits = 2)
                           )
             )
        }

    }

    return(micro_coefs_std)

}



TrainMicromodelCONF <- function(data, target_col, var_cols) {
    ##
    ##  implements stohastic/batch implicit feedback filtering with confidence
    ##

    if (DEBUG) {
        if (FALSE)
            data <- data[order(data[, target_col], decreasing = TRUE),]
        cost_func_launch <<- 0 # reset cost function
    }

    micro_coefs_cnf <- as.vector(rep(0, length(var_cols)))
    names(micro_coefs_cnf) <- var_cols


    y_train <- data[, target_col]
    items_coefs <- as.data.frame(data[, var_cols])
    #items_coefs <- sapply(items_coefs,as.numeric)

    if (SCALE_MIN_MAX) {
        items_coefs <- scale_minmax(items_coefs)
    }

    micro_coefs_cnf <- as.vector(rep(0, length(var_cols) + 1))
    coefs_names <- c("BIAS", var_cols)
    names(micro_coefs_cnf) <- coefs_names
    items_coefs <- as.matrix(items_coefs)
    items_coefs <- cbind(rep(1, nrow(items_coefs)), items_coefs)

    alpha_cnf <- SGD_ALPHA * 0.1
    lambda_cnf <- SGD_LAMBDA * 2
    epochs <- SGD_EPOCHS %/% 2

    confidence_scaler <- 1


    ###
    ### stohastic gradient descent for content based filtering with confidence intervals
    ###
    steps <- nrow(data) %/% 1

    train_costs_steps <<- (epochs * nrow(data)) %/% 100

    for (epoch in 1:epochs) {

        for (i in 1:nrow(data)) {

            if (FULL_DEBUG && (i %% steps) == 0) {
                non_zero_coefs_cnf <- nonzero(micro_coefs_cnf)
                cat("Non zero coefs cnf:\n")
                print(non_zero_coefs_cnf[1:5])
            }
            item_coefs_vect <- items_coefs[i,] #as.vector(sapply(items_coefs[i,], as.numeric))

            H_cnf <- (micro_coefs_cnf %*% item_coefs_vect)[1]
            conf <- 1 + confidence_scaler * y_train[i]
            Y_cnf <- 1
            if (DEBUG) {
                #J <- CostFunctionCONF(H_cnf, Y_cnf)
                J <- TrainCost(micro_coefs_cnf, items_coefs, y_train)
            }

            tmp_micro_coefs = micro_coefs_cnf
            tmp_micro_coefs[1] = 0

            grad_cnf <- conf * (H_cnf - Y_cnf) * item_coefs_vect + lambda_cnf * tmp_micro_coefs

            micro_coefs_cnf <- micro_coefs_cnf - alpha_cnf * grad_cnf
        }

    }



    if (DEBUG) {
        #plot_scores <- cnf_cost_scores
        J <- SimpleCost(micro_coefs_cnf, items_coefs, y_train)
        plot_scores <- train_costs
        cnf_cost_scores <<- plot_scores
        nrsco <- length(plot_scores)
        cidx <- c(1, nrsco %/% 5, nrsco %/% 4, nrsco %/% 3, nrsco %/% 2, nrsco %/% 1.4, nrsco %/% 1.2, nrsco)
        sinf <- paste0(" CNF A:", alpha_cnf,
                   " E:", epochs,
                   " L:", lambda_cnf,
                   " C: %s")
        logger(sprintf(sinf, plot_scores[cidx]))
        if (SHOW_PLOTS) {
            plot(plot_scores,
             type = "l",
            #asp = 1,
             main = paste0("CNF J:", round(J, digits = 2),
                           " LJ:", round(tail(plot_scores, 1), digits = 2)
                           )
             )
        }

    }

    return(micro_coefs_cnf)

}

GetScores <- function(df_items, coefs) {
    X <- cbind(rep(1, nrow(df_items)), df_items)

    if (DEBUG) {
        logger(sprintf(" GetScores(Items: %s , Coefs: %d)", toString(dim(X)), length(coefs)))
    }

    scores <- as.matrix(X) %*% as.vector(coefs)
    return(scores)
}


GetProductsScoring <- function(df_micro_behavior, df_products, vars) {

    user_list <- paste0("Micro_", df_micro_behavior[, User.Field])
    prod_matrix <- data.matrix(df_products[, vars])
    cust_matrix <- data.matrix(df_micro_behavior[, vars])
    cust_matrix_T <- base::t(cust_matrix)

    # compute:
    # MxN * N*K = MxK
    timeit("Score matrix generation...",
        score_matrix <- prod_matrix %*% cust_matrix_T
        )
    df_res <- as.data.frame(score_matrix)
    colnames(df_res) <- user_list
    if ("ItemName" %in% colnames(df_products)) {
        df_res$ProductName <- df_products$ItemName
    }
    df_res$ProductID <- df_products$ItemId
    nr_cols <- length(colnames(df_res))
    new_order <- c(nr_cols, nr_cols - 1, 1:(nr_cols - 2))
    df_res <- df_res[, new_order]
    return(df_res)
}


generate_recommendations <- function(df_mc = NULL) {

    logger("Generating recommendations ...\n")

    df_prods <- load_df(TABLE.ALLPRODUCTS) #rx_load_sql(SQL.ALLPRODUCTS)

    if (is.null(df_mc)) {
        df_testmicro <- rx_load_sql(SQL.DEMOMICROSEG)
    } else {
        df_testmicro <- df_mc
    }

    Predictor.Fields <- setdiff(colnames(df_prods), NonPredictors.Fields)
    df_scores <- GetProductsScoring(df_testmicro, df_prods, Predictor.Fields)
    logger(sprintf("Generating %d microsegments:\n", nrow(df_testmicro)))
    print(head(df_testmicro[, 1:15]))

    start_col = 3
    end_col = 5
    for (i in start_col:min(length(colnames(df_scores)), end_col)) {
        logger(sprintf("\nTop for microcluster %s:\n", colnames(df_scores)[i]))
        print(head(df_scores[order(df_scores[, i], decreasing = TRUE),]))
    }
    decp <- 4
    logger(sprintf("Rounding to %d decimals...", decp))
    cell.is.num <- sapply(df_scores, is.numeric)
    df_scores[cell.is.num] <- lapply(df_scores[cell.is.num], round, decp)

    save_df(df_scores, FILE.SCORES)
    logger("Done generating recommendations.\n")
    return(df_scores)
}

##
## start demo and behavior vectors prep !
##

if (DEMO) {
    ##
    ## DEMO only section
    ##
    generate_recommendations()

} else {
    ##
    ## behaviour generation section
    ##
    t0_prep <- proc.time()

    ##
    ##

    logger(
        sprintf(
            "\nBEGIN Behavior/Recommendations stand-alonescript v.%s DEBUG=%d\n",
                MODULE.VERSION, DEBUG))



    if (DO_PARALLEL) {
        # setup parallel processing environment
        setup_paralel_env()
    }


    df_microlist <- GetMicrosegmentList(as.integer(SEGMENT_MODEL_ID))
    df_nr <- GetTransactionCount(as.integer(SEGMENT_MODEL_ID))


    nr_all_obs <- df_nr[1, 1]


    if (DEBUG) {
        #All.Microsegments <- unique(df_microlist[, User.Field])[1:5]

        Microsegments.Bad <- c(315, 247, 200, 2, 3)
        Microsegments.Good <- c(13, 35, 162, 353)
        #All.Microsegments <- c(Microsegments.Bad, Microsegments.Good)
        All.Microsegments <- c(150) #,1, 100,150,200,250,300,350,399)

    } else {
        All.Microsegments <- unique(df_microlist[, User.Field])
    }
    Nr.Microsegments <- length(All.Microsegments)
    logger(sprintf("Loading %d microsegments [%s ...]\n",
                        Nr.Microsegments,
                        paste(All.Microsegments[1:3], collapse = ", ")))


    SCALE_FACTOR <- 1
    USE_CUSTOM_MODEL <- TRUE
    INSPECT_RESULTS <- FALSE

    status_prc <- 0
    nr_step <- 0
    total_loaded <- 0

    all_debug_steps <- Nr.Microsegments * length(SGD.ALPHAS) * length(SGD.LAMBDAS) * length(SGD.EPOCHS)
    debug_step <- 0


    #save model meta
    Model.Name <- sprintf("Model for %d [%s] behavior vectors [%s]-based for segmentation [%d]",
                        Nr.Microsegments, User.Field, Target.Field, as.integer(SEGMENT_MODEL_ID))
    logger(sprintf("\nSaving metadata for model: [%s]", Model.Name))
    Model.ID <- SaveModelMetadata(as.integer(SEGMENT_MODEL_ID), Model.Name)
    logger(sprintf("\nTRAINING %d UIDs FOR MODEL_ID:%d [%s]", Nr.Microsegments, Model.ID, Model.Name))
    model_total_time <- as.numeric(Sys.time())
    model_err1 <- 0
    model_err2 <- 0

    df_items <- LoadItems()

    for (MicroSegment in All.Microsegments) {
        logger(sprintf("\nTraining microsegment/user [%d]", MicroSegment))
        SGD_BEST_COST <<- 1e10
        SGD_CURRENT_ID <<- MicroSegment


        nr_step <- nr_step + 1

        if (DEBUG) {

            df_micro <- LoadMicrosegment(as.integer(SEGMENT_MODEL_ID),
                as.integer(MicroSegment))
        } else {
            df_micro <- LoadMicrosegment(as.integer(SEGMENT_MODEL_ID),
                as.integer(MicroSegment),
                cached = FALSE)
        }

        Predictor.Fields <- setdiff(colnames(df_micro), NonPredictors.Fields)


        timeit(" Finding zero variance columns... ", bad_cols1 <- ZeroVar(df_micro[, Predictor.Fields]))
        #timeit(" CaNZV ", bad_cols2 <- ZeroVarCaret(df_micro[, Predictor.Fields]))

        All.Predictor.Fields <- Predictor.Fields
        if (REDUCE_FEATS)
            Predictor.Fields <- setdiff(All.Predictor.Fields, bad_cols1)

        logger(sprintf("Total columns: %d, ZeroVar: %d, Final: %d",
                            length(All.Predictor.Fields),
                            length(bad_cols1),
                            length(Predictor.Fields)))

        logger("Converting to numeric...")
        df_micro[, Predictor.Fields] <- sapply(df_micro[, Predictor.Fields], as.numeric)




        obj_size <- object.size(df_micro) / (1024 * 1024)
        total_loaded <- total_loaded + obj_size
        #df_micro[, Target.Field] <- df_micro[, Target.Field] * SCALE_FACTOR
        nr_observations <- nrow(df_micro)
        current_batch <- nr_observations / nr_all_obs
        status_prc <- status_prc + current_batch

        logger(sprintf("\n[Processing: %2.2f%% Step: %d/%d Batch/SoFar: %.1fMB/%.1fMB Microsegment:%d w. %d obs]",
                status_prc * 100, nr_step, Nr.Microsegments, obj_size, total_loaded,
                MicroSegment, nr_observations))

        #####
        #####
        #####
        #####
        #####
        #####
        #####
        ##### XGB TRAIN CARENT CROSS VALID NEEDED!!!!
        #####
        #####
        #####
        #####
        #####
        #####
        logger("Training XGB...")
        xgb <- xgboost(data = as.matrix(df_micro[, Predictor.Fields]),
                                label = df_micro[, Target.Field],
                                missing = 0,
                                verbose = 0,
                                nround = 200)
        xgb_yhat <- predict(xgb, as.matrix(df_micro[, Predictor.Fields]))
        y <- df_micro[, Target.Field]
        xgb_r2 <- R2(xgb_yhat, y)

        feat_imp <- xgb.importance(feature_names = Predictor.Fields, model = xgb)

        A1 <- feat_imp[1][[1]]
        A2 <- feat_imp[2][[1]]
        A3 <- feat_imp[3][[1]]
        A4 <- feat_imp[4][[1]]
        A5 <- feat_imp[5][[1]]
        A6 <- feat_imp[6][[1]]
        A7 <- feat_imp[7][[1]]
        A8 <- feat_imp[8][[1]]
        A9 <- feat_imp[9][[1]]
        A10 <- feat_imp[10][[1]]

        fA1 <- feat_imp[1][[2]]
        fA2 <- feat_imp[2][[2]]
        fA3 <- feat_imp[3][[2]]
        fA4 <- feat_imp[4][[2]]
        fA5 <- feat_imp[5][[2]]
        fA6 <- feat_imp[6][[2]]
        fA7 <- feat_imp[7][[2]]
        fA8 <- feat_imp[8][[2]]
        fA9 <- feat_imp[9][[2]]
        fA10 <- feat_imp[10][[2]]

        print(head(feat_imp))

        logger(sprintf("XGB R2: %.2f", xgb_r2))

        if (DEBUG) {
            Micro3 <- NULL
            Micro4 <- NULL

            regr_fomula <- get_formula_nobias(Target.Field, Predictor.Fields)
            if (ENABLE_LMN) {
                t3 <- timeit("\nTraining LM micromodel nobias...",
                    Micro3 <- lm(formula = regr_fomula, data = df_micro),
                    NODATE = TRUE)
                Model3 <- Micro3$coefficients
            } else {
                Model3 <- c(0, 0, 0)
            }

            if (ENABLE_LMB) {
                regr_fomula <- get_formula(Target.Field, Predictor.Fields)
                t4 <- timeit("\nTraining LM micromodel with bias...",
                    Micro4 <- lm(formula = regr_fomula, data = df_micro),
                    NODATE = TRUE)
                Model4 <- Micro4$coefficients
            } else {
                Model4 <- c(0, 0, 0)
            }



            #
            # grid-search cross validation
            #
            for (A_ in SGD.ALPHAS) {
                for (L_ in SGD.LAMBDAS) {
                    for (E_ in SGD.EPOCHS) {
                        M_ <- SGD.MOMENTUMS[1]
                        for (S_ in SGD.SCALE_MIN_MAX) {

                            SGD_ALPHA <<- A_
                            SGD_LAMBDA <<- L_
                            SGD_EPOCHS <<- E_
                            SGD_USE_MOMENTUM <<- M_
                            SCALE_MIN_MAX <<- S_
                            debug_step <- debug_step + 1


                            inf <- paste0(sprintf("Starting cross-valid step %d/%d for ",
                                                  debug_step,
                                                  all_debug_steps)
                                          , "MICRO=", MicroSegment
                                          , " ALPHA=", SGD_ALPHA
                                          , " LAMBDA=", SGD_LAMBDA
                                          , " EPOCHS=", SGD_EPOCHS
                                          , " MOMENTUM=", SGD_USE_MOMENTUM
                                          , " SCALE_MINMAX=", SCALE_MIN_MAX
                                         )

                            logger(inf)

                            for (neq in NORMALEQ.LAMBDAS) {
                                LAMBDA_NORMAL_EQ <- neq
                                tn <- timeit("\nTraining NormalEquation micromodel...",
                                         ModelN <- TrainNormalEquation(df_micro,
                                                                       Target.Field,
                                                                       Predictor.Fields),
                                         NODATE = TRUE)
                            }
                            Model0 <- NULL
                            Model1 <- NULL
                            Model2 <- NULL
                            if (ENABLE_SGD) {

                                t0 <- timeit("\nTraining SGD micromodel...",
                                         Model0 <- TrainMicromodelSGD(df_micro,
                                                                      Target.Field,
                                                                      Predictor.Fields),
                                         NODATE = TRUE)
                            }

                            if (ENABLE_STD) {
                                t1 <- timeit("\nTraining STD micromodel...",
                                         Model1 <- TrainMicromodelSTD(df_micro,
                                                                      Target.Field,
                                                                      Predictor.Fields),
                                         NODATE = TRUE)
                            }

                            if (ENABLE_CNF) {

                                t2 <- timeit("\nTraining CNF micromodel...",
                                         Model2 <- TrainMicromodelCONF(df_micro,
                                                                       Target.Field,
                                                                       Predictor.Fields),
                                         NODATE = TRUE)
                            }
                            if (FULL_DEBUG) {
                                prin_cols <- c(1, 2, 50, 100, 200, 500)
                                cat(" NEQ:\n")
                                print(round(ModelN[prin_cols], digits = 3))
                                cat(" LMwbias:\n")
                                print(Model4[prin_cols])
                                cat(" SGD:\n")
                                print(Model0[prin_cols])
                                cat(" STD:\n")
                                print(Model1[prin_cols])
                                cat(" CNF:\n")
                                print(Model2[prin_cols])
                                cat(" LMnobias:\n")
                                print(c(0, Model3[1:(prin_cols - 1)]))
                            }

                            if (FULL_DEBUG) {
                                cat(" DFcounts: ")
                                print(colSums(df_micro[, Predictor.Fields])[1:prin_cols])
                            }

                            info <- paste0("m", MicroSegment
                            #, "_a", SGD_ALPHA
                            #, "_M", SGD_USE_MOMENTUM
                            #, "_L", SGD_LAMBDA
                            #, "_e", SGD_EPOCHS
                                            )
                            CheckPreds(MicroSegment,
                                        coefs = list(ModelN, Model0, Model1, Model2)
                                        , data = df_micro
                                        , lm = list(Micro3, Micro4, xgb)
                                        , col_names = c("NEQ",
                                                        "SGD",
                                                        "STD",
                                                        "CNF",
                                                        "LMn",
                                                        "LMb",
                                                        "XGB")
                                        , UserInfo = info
                            #, times = c(t0,t1,t2,t3,t4)
                                        )

                            ModelCoefficients <- ModelN

                        }
                    }
                }
            }


        } else {
            timeit(" Training micromodel ..",
                ModelCoefficients <- TrainNormalEquation(df_micro, Target.Field, Predictor.Fields),
                NODATE = TRUE)


        }

        yhat <- GetScores(df_micro[, Predictor.Fields], ModelCoefficients)

        rsq <- RSquared(y = df_micro[, Target.Field], yhat = yhat)
        rmse <- GetRMSE(y = df_micro[, Target.Field], yhat = yhat)

        SaveSubmodelMeta(Model.ID, MicroSegment,
                         A1, A2, A3, A4, A5, A6, A7, A8, A9, A10,
                         "N_R2", rsq, "X_R2", xgb_r2,
                         fA1, fA2, fA3, fA4, fA5, fA6, fA7, fA8, fA9, fA10)

        ### 
        ### NOW GENERATE PREDICTIONS FOR NEQ & XGB MODELS AND SAVE BOTH !!!
        ### 
        nr_o <- nrow(df_items)
        logger(sprintf("Generating predictions for %d products...", nr_o))
        preds_n <- GetScores(df_items[, Predictor.Fields], ModelCoefficients)
        logger(sprintf("Generating adv predictions..."))
        preds_x <- predict(xgb, as.matrix(df_items[, Predictor.Fields]))
        df_preds <- data.frame(ITEM_ID = df_items[, Product.Field],
                                PRED_LOW = preds_n, PRED_HIGH = preds_x)

        df_gt <- df_micro[, c(Product.Field, Target.Field)]
        colnames(df_gt) <- c(Product.Field, "GT")

        df_preds <- merge(df_preds, df_gt, by = Product.Field, all.x = TRUE)

        df_preds[, Model.Field] <- rep(Model.ID, nr_o)
        df_preds[, "UID"] <- as.integer(rep(MicroSegment, nr_o))

        df_preds <- df_preds[c(Model.Field, "UID", Product.Field, "PRED_LOW", "PRED_HIGH", "GT")]

        #save_df(df_preds, sfn=sprintf("PRED_MICRO_%d",MicroSegment))


        logger("Saving predictions to SQL Server ...")
        UploadToSQL(Preds.Table, df_preds)
        logger("Done saving predictions to SQL Server.")



        if (INSPECT_RESULTS) {
            NonZeroCoef <- show_non_zero(ModelCoefficients)
            NonZeroCols <- show_non_zero(colSums(df_micro[, Predictor.Fields]))

            df_res <- data.frame(Prods = names(NonZeroCols),
                        DifProds = as.vector(NonZeroCols),
                        Coeff = as.vector(NonZeroCoef))
        }

        KeyFields <- c(Model.Field, OutputID.Field)
        df_output <- data.frame()
        df_output <- rbind(df_output, c(as.integer(Model.ID), as.integer(MicroSegment), ModelCoefficients))
        colnames(df_output) <- c(KeyFields, names(ModelCoefficients))
        #save_df(df_output, sfn="COEFS")

        df_output_melted <- melt(df_output,
            id = KeyFields)

        colnames(df_output_melted) <- c(KeyFields, "PROP_CODE", "PROP_VAL")

        df_output_melted[, KeyFields] <-
            sapply(df_output_melted[, KeyFields],
                    as.integer)

        df_output_melted[, "PROP_VAL"] <- as.double(df_output_melted[, "PROP_VAL"])

        logger("Saving behavior vector to SQL Server ...")
        UploadToSQL(Coefs.Table, df_output_melted)
        logger("Done saving behavior vector to SQL Server.")

        model_err1 = model_err1 + rsq
        model_err2 = model_err2 + xgb_r2

    }

    ###
    ###
    ### now update overall model with average N_R2 and X_R2
    ###
    ###
    model_err1_name <- "N_R2"
    model_err2_name <- "X_R2"
    model_err1 <- model_err1 / Nr.Microsegments
    model_err2 <- model_err2 / Nr.Microsegments
    model_total_time <- as.numeric(Sys.time()) - model_total_time

    logger(sprintf("Model done in %.2fs. Saving overall model metadata %s=%.2f %s=%.2f...",
                                model_total_time,
                                model_err1_name, model_err1,
                                model_err2_name, model_err2
                                ))

    UpdateModelMetadata(model_id = Model.ID,
                        err1_name = model_err1_name, err1_val = model_err1,
                        err2_name = model_err2_name, err2_val = model_err2,
                        model_time = model_total_time)

    ### CLOSING SECTION

    t1_prep <- proc.time()
    prep_time <- (t1_prep[3] - t0_prep[3]) / 60

    logger(sprintf("\nDone model training and cross-validation. Total time %.2f min\n",
            prep_time))
    ### END  model selection, training, validation and testing

    if (DO_PARALLEL) {
        logger("Shutting down parallel backend...")
        stopCluster(p_cluster)
    }

    logger("\nScript done.\n")
}
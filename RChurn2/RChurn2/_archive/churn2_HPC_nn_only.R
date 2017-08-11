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

LIBS <- c("e1071","reshape2", "reshape2", "xgboost", "caret", "MASS","plyr", "pROC")




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
HPC.Machine.Name <- "HPC"
###
### END CONNECTION PARAMS SECTION
###

Current.Machine.Name = Sys.info()["nodename"]

if (Current.Machine.Name == Debug.Machine.Name) {
    DEBUG <- TRUE
}

if (Current.Machine.Name == Debug.Machine2.Name) {
  DEBUG <- TRUE
  USE_REPOSITORY <- "C:/Users/slash/Google Drive/_hyperloop_data/churn_v2"
}

if (Current.Machine.Name == Debug.Machine3.Name) {
	DEBUG <- TRUE
	USE_REPOSITORY <- "C:/GoogleDrive/_hyperloop_data/churn_v2"
}


SQL.GETCHURN <- "EXEC [SP_GET_CHURN] @TRAN_PER_ID = %d, @CHURN_PER_ID = %d"
SQL.GETSTATS <- "EXEC [SP_CUST_LIST] @TRAN_PER_ID = %d"


SQL.DATASELECTION <- "SELECT * FROM "

Target.Field <- "CHURN"


Overall.Fields <- c("R", "F", "M")

NonPredictors.Fields <- c(
                        User.Field
                        , Target.Field
                        )

Short.Predictor.Fields <- c("R_1","M_1","F_1",
                            "R_2","M_2","F_2",
                            "R_3","M_3","F_3",
                            "R_4","M_4","F_4"
                            )


Churn.Threshold <- 0.4



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


##
## ChurnV2 lib
##

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

InstallKeras <- function()
{
  logger("Installing keras...")
  
  if (!is.element("devtools", .packages(all.available = TRUE))){
    logger("Installing devtools")
    install.packages("devtools")
  }
  library(devtools)
  if (!is.element("keras", .packages(all.available = TRUE))){
    devtools::install_github("r-lib/processx")
    logger("Downloading keras from github")
    devtools::install_github("rstudio/keras")
    library(keras)
    logger("Installing TensorFlow...")
    install_tensorflow()
    logger("Done installing TensorFlow")
  }else{
    library(keras)
  }
}

ChurnClassifier <- function(hidden_layers = c(128), nr_inputs)
{
  logger(sprintf("Preparing NN (input:%s layers:[%s])",toString(nr_inputs), toString(hidden_layers)))
  nr_hiddens = length(hidden_layers)
  model <-  keras_model_sequential()
  h1_units = hidden_layers[1]
  layer_dense(model, 
              units = h1_units, 
              activation = "elu", 
              input_shape = c(nr_inputs))
  
  if(nr_hiddens > 1)
  {
    for (nr_hidden in 2:nr_hiddens)
    {
      layer_dropout(model, rate = 0.5)
      layer_dense(model, 
                  units = hidden_layers[nr_hidden], 
                  activation = "elu")
    }
  }
  model %>% 
    layer_dense(units = 1, activation = "sigmoid") %>%
    compile(
      optimizer = 'rmsprop',
      loss = 'binary_crossentropy',
      metrics = c('accuracy')
    )
    if (FULL_DEBUG) {
        logger(sprintf("Neural Network layout>>"))
        summary(model)
    }
  return(model)
}

IS_CONDA = T
##
#if (IS_CONDA) {
#    library(reticulate)
#    use_condaenv("r-tensorflow")
#    ttt <- keras_model_sequential()
#}
##


conf_mats <-list()
i_conf <- 1

GetPredStats <- function (yhat,y, yhat_train=NULL, y_train=NULL, hiddens = NULL)
{
  logger(sprintf(" Computing confusion matrix for %d observations",length(y)))
  cf_test <- confusionMatrix(yhat,y, positive = "1")
  
  v_kappa_test <- cf_test$overall["Kappa"]
  v_accuracy_test <- cf_test$overall["Accuracy"]
  v_recall_test <- cf_test$byClass['Recall']
  
  v_kappa_train <- -1
  v_accuracy_train <- -1
  v_recall_train <- -1
  
  if(!is.null(yhat_train))
  {
    logger(sprintf(" Computing confusion matrix for %d observations",length(y_train)))
    cf_train <- confusionMatrix(yhat_train,y_train, positive = "1")
    
    v_kappa_train <- cf_train$overall["Kappa"]
    v_accuracy_train <- cf_train$overall["Accuracy"]
    v_recall_train <- cf_train$byClass['Recall']
    
  }
  
  results <- data.frame(v_accuracy_test, v_kappa_test, v_recall_test, 
               v_accuracy_train, v_kappa_train, v_recall_train)

  colnames(results) <- c("TestAcc", "TestKappa", "TestRecall",
                      "TrainAcc", "TrainKappa", "TrainRecall")

  if (!is.null(hiddens)) {
    results["Layout"] <- toString(hiddens)
  }
  
  conf_mats[[i_conf]] <<- cf_test
  i_conf <<- i_conf + 1
  
  return(results)
}



##
## end ChurnV2 lib
##


#df_stats_2015 <- LoadCustomerStatInfo(2)
#df_stats_2016 <- LoadCustomerStatInfo(3)

df_churn_2016 <- load_file("CHURN_2016.csv") #LoadChurnData(2, 3)

Predictor.Fields <- GetPredictors(df_churn_2016)
Nr.Predictors <- length(Predictor.Fields)

PredictorSelector <- list(Predictor.Fields,
                          Short.Predictor.Fields,
                          setdiff(Predictor.Fields, Overall.Fields)
                          )

X_full <- df_churn_2016[, Predictor.Fields]
y_full <- as.numeric(df_churn_2016[, Target.Field])

logger(sprintf("Preparing train/test data on %d obs...", nrow(X_full)))
train_part <- createDataPartition(y_full, p = 0.85, list=FALSE)
X_train <- X_full[train_part,]
X_test <- X_full[-train_part,]
y_train <- y_full[train_part]
y_test <- y_full[-train_part]
logger(sprintf(" Train: %d, test %d", nrow(X_train), nrow(X_test)))

v_max_depth = 3
v_eta = 0.2
v_gamma = 0.5
v_colsample_bytree = 1
v_min_child_weight = 2
v_nrounds = 10
v_subsample = 1

if(DEBUG){
  max_tests = length(PredictorSelector)
} else
{
  max_tests = 1
}

##
 Churn.Threshold <- 0.4
##

#Check Columns Variability !!!! 
 
 
 
 
Selection.Fields <- Predictor.Fields
for(i in 1:max_tests)
{
  Selection.Fields <- PredictorSelector[[i]]
  nr_preds = length(Selection.Fields)
  X_train <- X_full[train_part, Selection.Fields]
  X_test <- X_full[-train_part, Selection.Fields]
  
}
  


##
##
##
logger("\nPreparing and training Neural Networks")
nr_hid <- Nr.Predictors
nn_tests <- list()

nn_tests[[length(nn_tests) + 1]] <- c(nr_hid, nr_hid %/% 2)
nn_tests[[length(nn_tests) + 1]] <- c(nr_hid * 2, nr_hid)
nn_tests[[length(nn_tests) + 1]] <- c(nr_hid * 2, nr_hid, nr_hid %/% 2)

if (nr_hid >= 8){
    nn_tests[[length(nn_tests) + 1]] <- c(nr_hid %/% 2, nr_hid %/% 4)
    nn_tests[[length(nn_tests) + 1]] <- c(nr_hid * 2, nr_hid, nr_hid %/% 2, nr_hid %/% 4)
}

if(nr_hid >=16){
    nn_tests[[length(nn_tests) + 1]] <- c(nr_hid %/% 2, nr_hid %/% 4, nr_hid %/% 8)
    nn_tests[[length(nn_tests) + 1]] <- c(nr_hid * 2, nr_hid, nr_hid %/% 2, nr_hid %/% 4, nr_hid %/% 8)
}


s_results <- c()

best_train_recall <- 0
best_train_conf <- NULL
best_test_recall <-0
best_test_conf <- NULL

nn_start_time <- Sys.time()
df_res <- data.frame()
for(nn_test_layers in nn_tests)
{
  nr_preds <- ncol(X_train)
    nn_clf <- ChurnClassifier(nn_test_layers, nr_preds)

  timeit("Running fit/training ...",
  hist <- nn_clf %>% fit(x = as.matrix(X_train), y = y_train, 
                             batch_size = 512, epochs = 2, verbose = 1,
    validation_data = list(as.matrix(X_test), y_test))
    )

  timeit("Predicting on training data...",
        yhat_nn_train <- round(predict(nn_clf, as.matrix(X_train)), digits = 3)
    )

  timeit("Predicting on testing data...",
    yhat_nn_test <- round(predict(nn_clf, as.matrix(X_test)), digits = 3)
    )
  
  if(FULL_DEBUG)
  {
    logger("Preparing ROC for training ...")
    nn_train_roc <- roc(predictor = as.vector(yhat_nn_train), 
                        response = as.vector(y_train))
    logger("Preparing ROC for testing...")
    nn_test_roc  <- roc(predictor = as.vector(yhat_nn_test),  
                        response = as.vector(y_test))
    plot(nn_test_roc, main = sprintf("ROC NN layout: [%s]",toString(nn_test_layers)))
    plot(hist)
  }
  
  
  p_nn_test <- as.numeric(yhat_nn_test > Churn.Threshold)
  p_nn_train <- as.numeric(yhat_nn_train > Churn.Threshold)
  
  nn_res <- GetPredStats(yhat = p_nn_test, y = y_test, 
                         yhat_train = p_nn_train, y_train = y_train, hiddens = nn_test_layers)

  df_res <- rbind(df_res,nn_res)
  
  s <- sprintf("TestRec: %.3f TestKap: %.3f TrainRec: %.3f TrainKap: %.3f on NN ([%s], %.1f thr) ", 
               nn_res["TestRecall"], nn_res["TestKappa"],
               nn_res["TrainRecall"], nn_res["TrainKappa"],
               toString(nn_test_layers), Churn.Threshold)
  
  if(best_train_recall < nn_res["TrainRecall"])
  {
    best_train_recall <- nn_res["TrainRecall"]
    best_train_kappa <- nn_res["TrainKappa"]
    best_train_layers <- nn_test_layers
  }
  
  if(best_test_recall < nn_res["TestRecall"])
  {
    best_test_recall <-  nn_res["TestRecall"]
    best_test_kappa <- nn_res["TestKappa"]
    best_test_layers <- nn_test_layers
    best_test_conf <- i_conf
  }
    
    
  logger(s)
  s_results = c(s_results,s)
}

nn_end_time <- Sys.time()
nn_time <- difftime(nn_end_time,nn_start_time, units="sec")

logger(sprintf("DNN processing time: %.1f min",nn_time/60))

logger("Summing results")
for (s in s_results) {
    logger(s)
}

#display best train and test - show best test confusion matrix

logger(sprintf("Best train Recall: %.3f Kappa: %.3f for %s",
    best_train_recall, best_train_kappa, toString(best_train_layers)))
logger(sprintf("Best test  Recall: %.3f Kappa: %.3f for %s",
    best_test_recall, best_test_kappa, toString(best_test_layers)))

print(df_res[order(df_res$TestRecall),])
print(conf_mats[[best_test_conf]])
##
##
##



logger("Script done.")

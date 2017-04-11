
##
##
## @script:       Churn Prediction Stand Alone CrossValidation
## @created:      2017.03.23
## @lastmodified: 2017.04.07
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##
DEBUG = FALSE


###
### PLEASE REVIEW CONNECTION PARAMS
###
svr <- "server=VSQL08\\HYPERLOOP;"
db  <- "database=StatisticalStore;"
uid <- "uid=andreidi;"
pwd <- "pwd=HypML2017"
###
### END CONNECTION PARAMS SECTION
###


USED_PROCESSING_POWER = 0.65


conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)
ML.Models.Table = "[dbo].[ML_Models]"
Data.Models.Table = "[dbo].[ML_DataModels]"


SQL_CONNECT = 2
# 1: use RODBC to remotely connect to sql server
# 2: use RevoScaleR to remotely connect to sql server (Microsoft R ONLY!)
# 3: XDF




MODULE.VERSION <- "1.2.8"
MODULE.NAME <- "CHURN"

PROCESS_ONLY_ENABLED <- TRUE


if (!DEBUG) {
    # production settings
    SQL.DATASELECTION <- "SELECT * FROM"
} else {
    # debug/cache settings
    SQL.DATASELECTION <- "SELECT TOP 100 * FROM"
    SQL_CONNECT = 3
}





Output.Columns <- c("DataTable", "ScriptVer", "Model", "TrainAcc",
                    "CrossAcc", "CrossRecall", "CrossKappa", "TrainTime", 
                    "Params", "TrainRecs", "CrossRecs")

Field.TableName <- "TableName"
Field.Variables <- "InputField"
Field.Label     <- "LabelField"

Inspect.Fields <- c("MARGIN", "M")




DO_PARALLEL <-TRUE


Cross.Metric <- "Accuracy"

Coded.Label <- "classe"
Dataset.Size <- 1.0     # % out of dataset (100% usually)
Training.Size <- 0.6    # % out of Dataset.Size
Validation.Size <- 0.5  # % out of (Dataset.Size - Training.Size)

USE_BEST_CROSS <- TRUE

NR_ROWS <- 2e7


options(width = 300)

##
## HELPER FUNCTIONS
##
##
HELPER_LIB_VER <- "1.3.2"

# use predefined repository folder or just "" for automatic current folder
USE_REPOSITORY <- "c:/_hyperloop_data/churn"

FULL_DEBUG <- FALSE

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
    if (is.vector(stext)) {
        stext <- paste(stext,collapse=",", sep="")
    }
    cat(stext)
    flush.console()
    all_log <<- paste(all_log, stext, sep = "")

    # Evalute the desired series of expressions inside of tryCatch
    result <- tryCatch(
                   {
                        fileConn <- file(LogFileName, open = "wt")
                        writeLines(all_log, fileConn)
                        close(fileConn)
                   }, 
                   warning = function(war) {
                        # warning handler picks up where error was generated
                        cat(paste(">>LOG_WARNING:  ", war,"<<\n"))
                        f <- 0
                        return(f)
                   },
                   error = function(err) {
                        # error handler picks up where error was generated
                        cat(paste(">>LOG_ERROR:  ", err,"<<\n"))
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

logger(sprintf("Preparing helper lib v%s...",HELPER_LIB_VER))

timeit = function(strmsg, expr) {
    prf <- ""
    if(substr(strmsg,1,1)=="\n")
    {
      prf <- "\n"
      strmsg <- gsub("\n","",strmsg)
    }
    optime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    prf <- paste0(prf, sprintf("[%s] ",optime))
    strmsg <- paste0(prf,strmsg)
    logger(strmsg)
    tm <- system.time(expr)
    ftime <- tm[3]
    stm <- sprintf("%.2fmin", ftime / 60)
    logger(paste0("  executed in ", stm, "\n"))
    return(ftime)
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
            logger(sprintf("Installing package: %s\n",i))
            install.packages(i)
        }
        logger(sprintf("Loading package: %s\n", i))
        if(!(paste0("package:",i) %in% search()))
            library(i, character.only = TRUE, verbose = TRUE)
    }
}

p_cluster <- 0
setup_paralel_env <- function() {
  
    logger(sprintf("Starting parallel processing backend at %.2f%%\n", USED_PROCESSING_POWER*100))

    install_and_load(c("doParallel","foreach"))

    ### Register parallel backend
    avail_cores <- detectCores() # available cores
    p_cluster <<- makeCluster(as.integer(avail_cores*USED_PROCESSING_POWER))
    registerDoParallel(p_cluster)
    logger(sprintf("[Parallel backend] Cores registered: %d\n", getDoParWorkers()))
    logger(sprintf("[Parallel backend] Environment: %s\n", getDoParName()))
    logger(sprintf("[Parallel backend] Backend version: %s\n", getDoParVersion()))
}

save_df <- function(df) {
    logger("\nSaving data...\n")
    file_db_path <- get_data_dir()
    if(FULL_DEBUG)
        logger(sprintf("Used data directory: %s\n", file_db_path))
    FN <- paste0(MODULE.NAME, "_v", MODULE.VERSION, "_", log_ctime, "_data.csv")
    FileName <- file.path(file_db_path, FN)
    logger(sprintf("Saving File: %s\n", FileName))
    timeit("Save: ", write.csv(x = df, file = FileName, row.names = TRUE))
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


load_df <- function(str_table) {

    Data.FileName <- gsub("\\[", "", gsub("\\]", "", str_table))
    Data.FileName <- paste0(Data.FileName, ".xdf")

    str_sql <- paste(SQL.DATASELECTION, str_table)

    if ("RevoScaleR" %in% rownames(installed.packages())) {
        library(RevoScaleR)
        # Create an xdf file name
        file_db_path <- get_data_dir()
        if (FULL_DEBUG)
            logger(sprintf("Current workind directory: %s\n", file_db_path))
        dfXdfFileName <- file.path(file_db_path, Data.FileName)
        logger(sprintf("XDF File: %s\n", dfXdfFileName))
    }

    if (SQL_CONNECT == 1) {
        logger(sprintf("Connecting MSSQLServer [%s]...\n", conns))
        library(RODBC)
        channel <- odbcDriverConnect(conns)

        timeit(sprintf("Downloading [%s] table... ", str_sql),
               df_in <- sqlQuery(channel, paste(str_sql)))

        odbcClose(channel)
        logger("Done data downloading.\n")
    } else if (SQL_CONNECT > 1) {
        if ((SQL_CONNECT == 2) || !file.exists(dfXdfFileName)) {
            logger(sprintf("RevoScaleRConn MSSQLServer [%s]...\n", conns))

            dsOdbcSource <- RxOdbcData(sqlQuery = str_sql,
                                        connectionString = conns)

            
            # Import the data into the xdf file
            timeit(sprintf("Downloading [rxImport: %s]...\n",str_sql),
                   rxImport(dsOdbcSource, dfXdfFileName, overwrite = TRUE))
        }
        # Read xdf file into a data frame
        timeit("RevoScaleR loading dataframe [rxDataStep]... ",
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


##
## END HELPER FUNCTIONS
##

t0_prep <- proc.time()

ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
logger(
    sprintf(
        "\n[%s] BEGIN Churn multi-model cross-validation stand-alonescript v.%s DEBUG=%d\n",
        ctime, MODULE.VERSION,DEBUG))



if (DO_PARALLEL) {
    # setup parallel processing environment
    setup_paralel_env()
}


df_datamodels <- load_df(Data.Models.Table)
df_mlmodels   <- load_df(ML.Models.Table)

Proposed.Models <- c()
Proposed.Libraries <- c()

df_mlmodels <- df_mlmodels[order(df_mlmodels$ID),]

for (i in 1:nrow(df_mlmodels)) {
    if (!PROCESS_ONLY_ENABLED || df_mlmodels[i, "Enabled"]) {
        cmodel <- gsub("\"", "", df_mlmodels[i, "Model"])
        libs <- strsplit(gsub("\"", "", df_mlmodels[i, "Library"]), ",")[[1]]

        Proposed.Models <- c(Proposed.Models, cmodel)
        Proposed.Libraries <- union(Proposed.Libraries, libs)
    }
}

Nr.Proposed.Models <- length(Proposed.Models)
logger(sprintf("Proposed models: %s\n",paste(Proposed.Models,collapse = ",")))
logger(sprintf("Proposed libs: %s\n", paste(Proposed.Libraries,collapse = ",")))

install_and_load(Proposed.Libraries)

#Train.Accuracy <- c()
#Cross.Accuracy <- c()
#Cross.Recall <- c()
#Cross.Kappa <- c()
#Training.Time <- c()
#Used.Data <- c()
#Script.Ver <- c()
#Tuning.Params <- c()
#Current.Model <- c()

summary_info <- data.frame(
                           #Used.Data, Script.Ver,
                           #Current.Model, Train.Accuracy,
                           #Cross.Accuracy, Cross.Recall, Cross.Kappa,
                           #Training.Time, Tuning.Params
                           )


for (table_index in 1:nrow(df_datamodels)) {

    Table.Name <- df_datamodels[table_index, Field.TableName]
    Table.Fields <- df_datamodels[table_index, Field.Variables]
    Table.Label <- df_datamodels[table_index, Field.Label]
    logger(sprintf("\nLoading table:[%s] label:[%s] fields:[%s]\n",
                    Table.Name, Table.Label, Table.Fields))
    df <- load_df(Table.Name)
    debug_object_size(df)

    Target.Label <- gsub("\"", "", Table.Label)
    Predictor.Fields <- gsub("\"", "", Table.Fields)
    Predictor.Fields <- strsplit(Predictor.Fields, ",")[[1]]


    ## preprocessing
    PRE_VER <- "0.0.5"

    logger(paste0("Begin data preprocessing v", PRE_VER, " ...\n"))


    ###
    ###

    df$classe <- df[, Target.Label]
    df[, Target.Label] <- NULL
    df$classe <- as.factor(df$classe)


    ###
    ###

    t1_prep <- proc.time()
    prep_time <- (t1_prep[3] - t0_prep[3]) / 60

    logger(sprintf("Done preprocessing. Total time %.2f min\n", prep_time))
    ## end preprocessing


    ### BEGIN cross-validation data preparation

    finalData <- df
    inTraining <- createDataPartition(finalData$classe, p = Training.Size, list = FALSE)
    df_train <- finalData[inTraining,]
    testdataStd <- finalData[-inTraining,]
    inVal <- createDataPartition(testdataStd$classe, p = Validation.Size, list = FALSE)
    df_valid <- testdataStd[inVal,]
    df_test <- testdataStd[-inVal,]
    ### END cross-validation data preparation

    ### BEGIN model selection, training, validation and testing
    TRAIN_VER <- "0.3.3"

    logger(paste0("Begin model training and cross-validation v", TRAIN_VER, " ...\n"))

    t0_prep <- proc.time()

    ###
    ###

    Confusion.Matrices <- c()



    Best.Accuracy <- 0

    for (c_model in 1:Nr.Proposed.Models) {
        Current.Model.Method <- Proposed.Models[c_model]
        slabel <- paste0(Coded.Label, "~")
        sfactors <- paste(Predictor.Fields, collapse = "+")
        clf_formula = as.formula(paste(slabel, sfactors))
        sformula = paste(slabel, sfactors)
        smsg <- sprintf("\nTraining [%s: %s] model ...",
                        Current.Model.Method, sformula)
        c_time <- timeit(smsg,
                     Current.Model <- train(clf_formula
                                            , data = df_train
                                            , method = Current.Model.Method
                                            , metric = Cross.Metric
                                            )
                    )

        train_time <- c_time[[1]]

        preds_train <- predict(Current.Model, df_train)
        preds_valid <- predict(Current.Model, df_valid)

        cfm_train <- confusionMatrix(preds_train, df_train$classe, positive = "1")
        cfm_valid <- confusionMatrix(preds_valid, df_valid$classe, positive = "1")

        train_Accuracy <- cfm_train$overall["Accuracy"][[1]]
        cross_Accuracy <- cfm_valid$overall["Accuracy"][[1]]
        cross_Kappa <- cfm_valid$overall["Kappa"][[1]]
        cross_Recall <- cfm_valid$byClass["Sensitivity"][[1]]
        script_Ver <- paste0("ChurnModel v", MODULE.VERSION)
        tuning_Params <- get_tune_params(Current.Model)
        train_Recs <- nrow(df_train)
        cross_Recs <- nrow(df_valid)
        used_Data <- Table.Name

        new_rec <- data.frame(used_Data, script_Ver,
                     Current.Model.Method, train_Accuracy,
                     cross_Accuracy, cross_Recall, cross_Kappa,
                     train_time, tuning_Params,
                     train_Recs, cross_Recs)

        colnames(new_rec) <- Output.Columns

        summary_info <- rbind(summary_info, new_rec)
        colnames(summary_info) <- Output.Columns

        Confusion.Matrices <- c(Confusion.Matrices, cfm_valid)
        tacc <- train_Accuracy
        vacc <- cross_Accuracy
        vkap <- cross_Kappa
        vrec <- cross_Recall

        logger(sprintf(" Train acc: %.3f Valid acc: %.3f Valid kap: %.3f Valid rec: %.3f\n",
                    tacc, vacc, vkap, vrec))
        logger(sprintf(" Tune params: %s\n",
                    tuning_Params))
        logger(sprintf(" Done training [%s] model ...\n", Current.Model.Method))

        save_df(summary_info)


        if (USE_BEST_CROSS) {
            if (Best.Accuracy < cross_Recall) {
                Best.Model <- Current.Model
                Best.Accuracy <- cross_Recall
            }
        } else {
            if (Best.Accuracy < cross_Accuracy) {
                Best.Model <- Current.Model
                Best.Accuracy <- cross_Accuracy
            }
        }
    }


    summary_info <- summary_info[order(summary_info$CrossRecall),]
    print("Tuning results:")
    print(summary_info)

    timeit(paste0("\nPredict Testing data with best model: ", Best.Model$method),
       testpred <- predict(Best.Model, df_test)
       )
    test_conf <- confusionMatrix(testpred, df_test$classe, positive = "1")

    df_test$Prediction <- testpred

    logger(sprintf("Total positives: %d\n", nrow(df_test[df_test$classe == 1,])))

    if (FALSE) {
        df_right <- df_test[df_test$classe == 1 & df_test$Prediction == 1,]
        df_right <- df_right[order(df_right[, Inspect.Fields[1]],
                           decreasing = TRUE),]
        print(head(df_right), 3)
    }

    ###
    ###


    # now cleanup R messy memory
    rm(df)
    rm(df_valid)
    rm(df_test)
    gc()
}


### 
### cleanup
###
t1_prep <- proc.time()
prep_time <- (t1_prep[3] - t0_prep[3]) / 60

logger(sprintf("\nDone model training and cross-validation. Total time %.2f min\n", prep_time))
### END  model selection, training, validation and testing

if (DO_PARALLEL) {
    logger("Shutting down parallel backend...")
    stopCluster(p_cluster)
}

logger("\nScript done.\n")


##
##
##
## @script:       Churn Prediction Alpha test
## @created:      2017.03.23
## @lastmodified: 2017.04.05
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##

##
## FOR SQL SERVER: COPY ONLY "COPY-PASTE" SECTION AFTER "INPUT PARAMETERS" SECTION
##



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

###
### @INPUT parameter preparation
###

# sql input
Predictor.Fields <- c("RFCAM4C4Y14", 
                      "HYIT1F5C4Y14",
                      "TOPBR20C4Y14",
                      "RFMMITHY1TOPBR29C4Y14",
                      "tX",
                      "tY")
Target.Label <- "Churn"
Inspect.Fields <- c("MARGIN", "M")
ID.Field <- "PartnerId"
Table.Name <- "_Attrib_20170324_v1"
# end sql input

DO_PARALLEL <-FALSE

# tuning input
All.Fields <- c(ID.Field, Inspect.Fields, Predictor.Fields)

#
All.Models <- c(
    "svmRadial", 
    "lda", 
    "rpart", 
    "plr", 
    "xgbTree",
    "bayesglm"
    )
All.Libraries <- c(
    "caret", 
    "rpart", 
    "LogicReg",
    "stepPlr", 
    "MASS", 
    "e1071",
    "kernlab", 
    "xgboost", 
    "plyr",
    "fastAdaboost",
    "arm"
    )

Proposed.Libraries <- c(
    "caret", 
    "rpart", 
    "fastAdaboost",
    "xgboost", 
    "plyr",
    "MASS",
    "arm"
    )

Proposed.Models <- c(
    "lda",
    "bayesglm",
    "rpart", 
    "xgbTree",
    "adaboost"
    )[1:2]
Nr.Proposed.Models <- length(Proposed.Models)

Cross.Metric <- "Kappa"

Coded.Label <- "classe"
Dataset.Size <- 1.0     # % out of dataset (100% usually)
Training.Size <- 0.6    # % out of Dataset.Size
Validation.Size <- 0.5 # % out of (Dataset.Size - Training.Size)

USE_BEST_CROSS <- TRUE

NR_ROWS <- 2e5
# end tuning input

###
### END INPUT PARAMS
###

###
### 

###############
###############
###############
###############
###############
############### BEGIN COPY-PASTE SECTION CODE
###############
###############
###############
###############
###############

MODULE.VERSION <- "0.1.5"
MODULE.NAME <- "CHURN"
options(width = 300)

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
    ftime <- tm[3]
    stm <- sprintf("%.2f min", ftime / 60)
    logger(paste0(strmsg, " executed in ", stm, "\n"))
    return(ftime)
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


install_and_load <- function(libraries) {
    for (i in libraries) {
        if (!is.element(i, .packages(all.available = TRUE))) {
            logger(sprintf("Installing package: %s\n",i))
            install.packages(i)
        }
        logger(sprintf("Loading package: %s\n", i))
        library(i, character.only = TRUE, verbose = TRUE)
    }
}

setup_paralel_env <- function() {

    install_and_load(c("doParallel","foreach"))

    ### Register parallel backend
    avail_cores <- detectCores() # available cores
    p_cluster <<- makeCluster(avail_cores - 2)
    registerDoParallel(p_cluster)
    logger(sprintf("[Parallel backend] Cores registered: %d\n", getDoParWorkers()))
    logger(sprintf("[Parallel backend] Environment: %s\n", getDoParName()))
    logger(sprintf("[Parallel backend] Backend version: %s\n", getDoParVersion()))
}

save_df <- function(df) {
    logger("\nSaving data...\n")
    file_db_path <- dirname(get_scriptpath())
    logger(sprintf("Current workind directory: %s\n", file_db_path))
    FN <- paste0(MODULE.NAME, "_v", MODULE.VERSION, "_data.csv")
    FileName <- file.path(file_db_path, FN)
    logger(sprintf("Saving File: %s\n", FileName))
    timeit("Save: ", write.csv(x = df, file = FileName, row.names = TRUE))
}


##
## END HELPER FUNCTIONS
##



ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
logger(sprintf("[%s] BEGIN Churn multi-model cross-validation script v.%s SQL_CONNECT=%d\n", ctime, MODULE.VERSION, SQL_CONNECT))


svr <- "server=VSQL08\\HYPERLOOP;"
db  <- "database=MachineLearning3;"
uid <- "uid=andreidi;"
pwd <- "pwd=HypML2017"
conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)
sqls <- "SELECT * FROM _Attrib_20170324_v1"


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
logger(sprintf("Done data downloading for table  [%s]. Dataset size: %sMB\n",
                Table.Name,
                strs))
strs <- format(
            round(as.numeric(nrow(df)), 1),
            nsmall = 0, big.mark = ",",
            scientific = FALSE)
logger(sprintf("Loaded %s rows in memory dataframe\n", strs))
logger(sprintf("Loading Models:   %s\n", paste(Proposed.Models, collapse = " ")))
logger(sprintf("Loading Packages: %s\n", paste(Proposed.Libraries, collapse = " ")))



# first check loaded libraries
install_and_load(Proposed.Libraries)

if (DO_PARALLEL) {
    # setup parallel processing environment
    setup_paralel_env()
}

## preprocessing
PRE_VER <- "0.0.3"

logger(paste0("Begin data preprocessing v", PRE_VER, " ...\n"))
t0_prep <- proc.time()

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

Train.Accuracy <- c()
Cross.Accuracy <- c()
Cross.Recall <- c()
Cross.Kappa <- c()
Training.Time <- c()
Confusion.Matrices <- c()
Used.Data <- c()
Script.Ver <- c()
Tuning.Params <-c()

Best.Accuracy  <- 0

for (c_model in 1:Nr.Proposed.Models) {
    Current.Model.Method <- Proposed.Models[c_model]
    slabel <- paste0(Coded.Label, "~")
    sfactors <- paste(Predictor.Fields, collapse = "+")
    clf_formula = as.formula(paste(slabel,sfactors))
    sformula = paste(slabel,sfactors)
    logger(sprintf("\nTraining [%s: %s] model ...\n", Current.Model.Method, sformula))
    c_time <- timeit(Current.Model.Method,
                     Current.Model <- train(clf_formula,
                                            data = df_train,
                                            method = Current.Model.Method,
                                            metric = Cross.Metric
                                            )
                    )

    Training.Time[c_model] <- c_time

    preds_train <- predict(Current.Model, df_train)
    preds_valid <- predict(Current.Model, df_valid)

    cfm_train <- confusionMatrix(preds_train, df_train$classe, positive = "1")
    cfm_valid <- confusionMatrix(preds_valid, df_valid$classe, positive = "1")

    Train.Accuracy[c_model] <- cfm_train$overall["Accuracy"]
    Cross.Accuracy[c_model] <- cfm_valid$overall["Accuracy"]
    Cross.Kappa[c_model] <- cfm_valid$overall["Kappa"]
    Cross.Recall[c_model] <- cfm_valid$byClass["Sensitivity"]
    Script.Ver[c_model] <- paste0("ChurnModel v", MODULE.VERSION)
    Tuning.Params[c_model] <- get_tune_params(Current.Model)
 

    Confusion.Matrices <- c(Confusion.Matrices, cfm_valid)
    tacc <- Train.Accuracy[c_model]
    vacc <- Cross.Accuracy[c_model]
    vkap <- Cross.Kappa[c_model]
    vrec <- Cross.Recall[c_model]

    logger(sprintf(" Train acc: %.3f Valid acc: %.3f Valid kap: %.3f Valid rec: %.3f\n",
                    tacc, vacc, vkap, vrec))
    logger(sprintf(" Tune params: %s\n",
                    Tuning.Params[c_model]))
    logger(sprintf(" Done training [%s] model ...\n", Current.Model.Method))


    if (USE_BEST_CROSS) {
        if (Best.Accuracy < Cross.Recall[c_model]) {
            Best.Model <- Current.Model
            Best.Accuracy <- Cross.Recall[c_model]
        }
    } else {
        if (Best.Accuracy < Cross.Accuracy[c_model]) {
            Best.Model <- Current.Model
            Best.Accuracy <- Cross.Accuracy[c_model]
        }
    }
}

for (i in 1:Nr.Proposed.Models) {
    Used.Data[i] <- Table.Name
}

summary_info <- data.frame(Used.Data, Script.Ver,
                           Proposed.Models, Train.Accuracy,
                           Cross.Accuracy, Cross.Recall, Cross.Kappa,
                           Training.Time, Tuning.Params)
summary_info <- summary_info[order(summary_info$Cross.Accuracy),]
logger("\nTuning results:\n")
print(summary_info)

timeit(paste0("\nPredict Testing data with best model: ", Best.Model$method),
       testpred <- predict(Best.Model, df_test)
       ) 
test_conf <- confusionMatrix(testpred, df_test$classe, positive = "1")

df_test$Prediction <- testpred

logger(sprintf("Total positives: %d\n", nrow(df_test[df_test$classe == 1,])))


df_right <- df_test[df_test$classe == 1 & df_test$Prediction == 1,]
df_right <- df_right[order(df_right[, Inspect.Fields[1]],
                           decreasing = TRUE),]

print(head(df_right),3)

###
###

t1_prep <- proc.time()
prep_time <- (t1_prep[3] - t0_prep[3]) / 60

logger(sprintf("\nDone model training and cross-validation. Total time %.2f min\n", prep_time))
### END  model selection, training, validation and testing

if (DO_PARALLEL) {
    logger("Shutting down parallel backend...")
    stopCluster(p_cluster)
}

Log.Lines <- c()
for (c_model in 1:Nr.Proposed.Models) {
     Log.Lines[c_model] <- all_log
    }
summary_info$Log <- Log.Lines

logger("\nScript done.")

###
### OUTPUT IS: summary_info
### Must be inserted in DB
###

###############
###############
###############
###############
###############
############### END COPY-PASTE SECTION CODE
###############
###############
###############
###############
###############
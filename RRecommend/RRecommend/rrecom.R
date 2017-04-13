##
##
##
## @script:       Microsegmentation Content Based Recommender System
## @created:      2017.03.23
## @lastmodified: 2017.04.10
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##
##
##  TODO:
##      
##      chk NaN ptr 5 104   6   3  10 110 132   2 109 120  16  63 114 115  40  12 103   1 101
##      test std vs lm vs conf
##      add cost function !!! - list iterations
##      normal equation full batch
##      Full batch GD fara MEDIE ! NG style :)
##      add intercept to sgd
##      try logit ?
##


DEBUG = FALSE

DEMO = FALSE




###
### PLEASE REVIEW CONNECTION PARAMS
###
svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=StatisticalStore;"
uid <- "uid=andreidi;"
pwd <- "pwd=HypML2017"
Debug.Machine.Name <- "DAMIAN"
###
### END CONNECTION PARAMS SECTION
###






if (Sys.info()["nodename"] == Debug.Machine.Name) {
    DEBUG <- TRUE
}

if (DEBUG) {
    Table.Name <- "_RFMM16_sgmidXItemXAttrib1"
    SQL.ALLPRODUCTS <- "SELECT * FROM _ProductFeatures2016"
    TABLE.ALLPRODUCTS <- "_ProductFeatures2016"
    #Table.Name <- "_RFMM14_sgmidXItemXAttrib1"
    #SQL.ALLPRODUCTS <- "SELECT * FROM _ProductFeatures2014"
    #TABLE.ALLPRODUCTS <- "_ProductFeatures2014"
} else {
    Table.Name <- "_RFMM16_sgmidXItemXAttrib1"
    SQL.ALLPRODUCTS <- "SELECT * FROM _ProductFeatures2016"
    TABLE.ALLPRODUCTS <- "_ProductFeatures2016"
}

User.Field <- "MicroSegmentId"
Target.Field <- "Count"
Product.Field <- "ItemId"
Product.Name <- "ItemName"
NonPredictors.Fields <- c(
                        User.Field
                        , Target.Field
                        , Product.Field
                        , Product.Name)



conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)


SQL_CONNECT = 3
# 1: use RODBC to remotely connect to sql server
# 2: use RevoScaleR to remotely connect to sql server (Microsoft R ONLY!)
# 3: XDF




MODULE.VERSION <- "1.0.1"
MODULE.NAME <- "BEHAVIOR_RECOMMENDATIONS"

SQL.MICROSEGMENTLIST <- paste0("SELECT DISTINCT ", User.Field, " FROM ", Table.Name)
SQL.NR.OBS <- paste0("SELECT COUNT(*) FROM ", Table.Name)
SQL.LOADMICROSEGMENT <- paste0("SELECT * FROM ", Table.Name, " WHERE ", User.Field, " = ")
SQL.TOP100 <- paste0("SELECT TOP 100 * FROM ", Table.Name)

SQL.DEMOMICROSEG <- "SELECT * FROM bev5"

FILE.MATRIX = "BEHAVIOR_MATRIX"
FILE.SCORES = "SCORES_MATRIX"




SQL.DATASELECTION <- "SELECT * FROM"



DO_PARALLEL <- FALSE


Dataset.Size <- 1.0 # % out of dataset (100% usually)
Training.Size <- 0.6 # % out of Dataset.Size
Validation.Size <- 0.5 # % out of (Dataset.Size - Training.Size)

NR_ROWS <- 2e7



# use predefined repository folder or just "" for automatic current folder
USE_REPOSITORY <- "c:/_hyperloop_data/microbehavior"
USED_PROCESSING_POWER <- 0.65
##
## HELPER FUNCTIONS
##
##


HELPER_LIB_VER <- "1.4.0"


FULL_DEBUG <- FALSE

options(digits = 3)

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
    if (is.vector(stext)) {
        stext <- paste(stext, collapse = ",", sep = "")
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

timeit = function(strmsg, expr, NODATE = FALSE) {
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
    return(ftime)
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
            logger(sprintf("Installing package: %s\n", i))
            install.packages(i)
        }
        logger(sprintf("Loading package: %s\n", i))
        if (!(paste0("package:", i) %in% search()))
            library(i, character.only = TRUE, verbose = TRUE)
        }
}

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

save_df <- function(df, sfn = "") {
    if (FULL_DEBUG)
        logger("\nSaving data...\n")
    file_db_path <- get_data_dir()
    if (FULL_DEBUG)
        logger(sprintf("Used data directory: %s\n", file_db_path))
    FN <- paste0(MODULE.NAME, "_v", gsub("\\.", "_", MODULE.VERSION), "_", log_ctime, "_data.csv")
    if (sfn != "")
        FN <- paste0(sfn, "_", log_ctime, "_data.csv")
    FileName <- file.path(file_db_path, FN)
    timeit(sprintf("Saving File:[%s] ...", FileName),
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


load_df <- function(str_table, caching = TRUE, use_sql = FALSE) {

    Data.FileName <- gsub("\\[", "_", gsub("\\]", "_", str_table))
    Data.FileName <- gsub(" ", "_", gsub("\\*", "_", Data.FileName))
    Data.FileName <- gsub(",", "_", gsub("\\.", "_", Data.FileName))
    Data.FileName <- paste0(Data.FileName, ".xdf")
    str_sql <- paste(SQL.DATASELECTION, str_table)

    if (sql)
        str_sql <- str_table


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
        if ((SQL_CONNECT == 2) || !file.exists(dfXdfFileName) || !caching) {
            logger(sprintf("RevoScaleRConn MSSQLServer [%s]...\n", conns))

            dsOdbcSource <- RxOdbcData(sqlQuery = str_sql,
                                     connectionString = conns)


            # Import the data into the xdf file
            timeit(sprintf("Downloading [rxImport: %s]...\n", str_sql),
                 rxImport(dsOdbcSource, dfXdfFileName, overwrite = TRUE))
        } else
            logger("Bypassing SQL Server. Loading data directly from XDF file...\n")
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


rx_load_sql <- function(str_sql) {

    library(RevoScaleR)
    if (FULL_DEBUG)
        logger(sprintf("RevoScaleRConn MSSQLServer [%s]...\n", conns))

    dsOdbcSource <- RxOdbcData(sqlQuery = str_sql,
                                connectionString = conns)


    # Import the data into the xdf file
    timeit(sprintf("Loading dataframe [rxImport: %s]...", str_sql),
           df_in <- rxImport(inData = dsOdbcSource, reportProgress = 0))


    df_in[is.na(df_in)] <- 0
    debug_object_size(df_in)
    return(data.frame(df_in))
}

normalize <- function(x) {
    mx <- max(x)
    mn <- min(x)
    if(mx==0 && mn==0) mx <- 1
    return((x - mn) / (mx - mn))
}

##
## END HELPER FUNCTIONS
##

scale_minmax <- function(df) {
    df_norm <- as.data.frame(lapply(df, normalize))
    return(df_norm)
}

STOHASTIC <- TRUE

SCALE_MIN_MAX <- TRUE


TrainMicromodel <- function(data, target_col, var_cols) {

    if (DEBUG) {
        if(FALSE)
            data <- data[order(data[, target_col], decreasing = TRUE),]
    }


    micro_coefs_std <- as.vector(rep(0, length(var_cols)))
    names(micro_coefs_std) <- var_cols

    y_train <- data[, target_col]
    items_coefs <- as.data.frame(lapply(data[, var_cols],as.numeric))

    if (SCALE_MIN_MAX) {
        items_coefs <- scale_minmax(items_coefs)
    }

    alpha_std <- 0.01
    lambda_std <- 1.5

    if (STOHASTIC) {
        ###
        ### stohastic gradient descent for content based filtering
        ###
        steps <- nrow(data) %/% 5
        
        for (i in 1:nrow(data)) {
            if (DEBUG && (i %% steps)==0) {
                non_zero_coefs_std <- nonzero(micro_coefs_std)
                non_zero_obs <- nonzero(items_coefs[i,])
                cat("Non zero coefs std:\n")
                print(non_zero_coefs_std[1:5])
                #cat("Non zero obs:\n")
                #print(non_zero_obs[1:3])
            }
            item_coefs_vect <- as.vector(sapply(items_coefs[i,], as.numeric)) # unlist(x_train[i,])
            H_std <- (micro_coefs_std %*% item_coefs_vect)[1]

            Y_std <- y_train[i]
            grad_std <- (H_std - Y_std) * item_coefs_vect + lambda_std * micro_coefs_std

            micro_coefs_std <- micro_coefs_std - alpha_std * grad_std
        }
    } else {
        ###
        ### full batch gradient descent for content based filtering
        ###
    }

    return(micro_coefs_std)

}

TrainMicromodelConfidence <- function(data, target_col, var_cols) {
    ##
    ##  implements stohastic/batch implicit feedback filtering with confidence
    ##

    if (DEBUG) {
        if (FALSE)
            data <- data[order(data[, target_col], decreasing = TRUE),]
        }

    micro_coefs_cnf <- as.vector(rep(0, length(var_cols)))
    names(micro_coefs_cnf) <- var_cols


    y_train <- data[, target_col]
    items_coefs <- data[, var_cols]
    alpha_cnf <- 0.001
    lambda_cnf <- 10
    confidence_scaler <- 5

    if (STOHASTIC) {
        ###
        ### stohastic gradient descent for content based filtering with confidence intervals
        ###
        steps <- nrow(data) %/% 5

        for (i in 1:nrow(data)) {
            if (DEBUG && (i %% steps) == 0) {
                non_zero_coefs_cnf <- nonzero(micro_coefs_cnf)
                non_zero_obs <- nonzero(items_coefs[i,])
                cat("Non zero coefs cnf:\n")
                print(non_zero_coefs_cnf[1:5])
                #cat("Non zero obs:\n")
                #print(non_zero_obs[1:5])
            }
            item_coefs_vect <- as.vector(sapply(items_coefs[i,], as.numeric)) # unlist(x_train[i,])

            H_cnf <- (micro_coefs_cnf %*% item_coefs_vect)[1]

            conf <- 1 + confidence_scaler * y_train[i]

            Y_cnf <- 1
            grad_cnf <- conf * (H_cnf - Y_cnf) * item_coefs_vect + lambda_cnf * micro_coefs_cnf

            micro_coefs_cnf <- micro_coefs_cnf - alpha_cnf * grad_cnf
        }
    } else {
        ###
        ### full batch gradient descent for content based filtering
        ###
    }


    return(micro_coefs_cnf)

}


GetProductsScoring <- function(df_micro_behavior, df_products, vars) {

    user_list <- paste0("Micro_", df_micro_behavior[, User.Field])
    prod_matrix <- data.matrix(df_products[, vars])
    cust_matrix <- data.matrix(df_micro_behavior[, vars])
    cust_matrix_T <- base::t(cust_matrix)

    # compute:
    # MxN * N*K = MxK
    timeit("Behavior matrix ...",
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
    logger(sprintf("Generating %d microsegments:\n",nrow(df_testmicro)))
    print(head(df_testmicro[, 1:15]))

    start_col = 3
    end_col = 5
    for (i in start_col:min(length(colnames(df_scores)), end_col)) {
        logger(sprintf("\nTop for microcluster %s:\n", colnames(df_scores)[i]))
        print(head(df_scores[order(df_scores[, i], decreasing = TRUE),]))
    }
    decp <- 4
    logger(sprintf("Rounding to %d decimals...",decp))
    cell.is.num <- sapply(df_scores, is.numeric)
    df_scores[cell.is.num] <- lapply(df_scores[cell.is.num], round, decp)

    save_df(df_scores, FILE.SCORES)
    logger("Done generating recommendations.\n")
    return(df_scores)
}

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

    ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    logger(
        sprintf(
            "\n[%s] BEGIN Behavior/Recommendations stand-alonescript v.%s DEBUG=%d\n",
                ctime, MODULE.VERSION, DEBUG))



    if (DO_PARALLEL) {
        # setup parallel processing environment
        setup_paralel_env()
    }

    df <- rx_load_sql(SQL.TOP100) # only for inference of predictor fields

    df_nr <- rx_load_sql(SQL.NR.OBS) # COUNT(*) 

    df_microlist <- rx_load_sql(SQL.MICROSEGMENTLIST) # DISTINCT MicroSegmentId


    nr_all_obs <- df_nr[1, 1]

    Predictor.Fields <- setdiff(colnames(df), NonPredictors.Fields)

    if (DEBUG) {
        #All.Microsegments <- unique(df_microlist[, User.Field])[1:5]

        Microsegments.Bad <- c(315, 200, 247)
        Microsegments.Good <- c(13, 35, 162, 353)
        All.Microsegments <- Microsegments.Bad

    } else {
        All.Microsegments <- unique(df_microlist[, User.Field])
    }
    Nr.Microsegments <- length(All.Microsegments)
    logger(sprintf("Loaded %d microsegments\n", Nr.Microsegments))

    df_output <- data.frame()

    SCALE_FACTOR <- 1
    USE_CUSTOM_MODEL <- TRUE
    INSPECT_RESULTS <- FALSE

    status_prc <- 0
    nr_step <- 0
    total_loaded <- 0


    for (MicroSegment in All.Microsegments) {
        nr_step <- nr_step + 1

        ssql <- paste(SQL.LOADMICROSEGMENT, toString(MicroSegment))
        df_micro <- load_df(ssql, use_sql = TRUE)
        obj_size <- object.size(df_micro) / (1024 * 1024)
        total_loaded <- total_loaded + obj_size
        #df_micro[, Target.Field] <- df_micro[, Target.Field] * SCALE_FACTOR
        nr_observations <- nrow(df_micro)
        current_batch <- nr_observations / nr_all_obs
        status_prc <- status_prc + current_batch

        logger(sprintf("\n[Processing: %2.2f%% Step: %d/%d Batch/SoFar: %.1fMB/%.1fMB Microsegment:%d w. %d obs]",
                status_prc * 100, nr_step, Nr.Microsegments, obj_size, total_loaded,
                MicroSegment, nr_observations))

        if (!USE_CUSTOM_MODEL) {
            regr_fomula <- get_formula(Target.Field, Predictor.Fields)
            MicroModel <- lm(formula = regr_fomula, data = df_micro)
            ModelCoefficients <- MicroModel$coefficients[-1]
        } else {
            if (DEBUG) {

                timeit(" Training CNF micromodel...",
                        Model2 <- TrainMicromodelConfidence(df_micro, Target.Field, Predictor.Fields),
                        NODATE = TRUE)

                timeit(" Training STD micromodel...",
                        Model1 <- TrainMicromodel(df_micro, Target.Field, Predictor.Fields),
                        NODATE = TRUE)

                regr_fomula <- get_formula(Target.Field, Predictor.Fields)
                timeit(" Training LM micromodel...",
                        Micro3 <- lm(formula = regr_fomula, data = df_micro),
                        NODATE = TRUE)
                Model3 <- Micro3$coefficients[-1]

                cat("STD:\n")
                print(Model1[1:10])
                cat("CNF:\n")
                print(Model2[1:10])
                cat("LM:\n")
                print(Model3[1:10])
                cat("DF\n")
                print(df_micro[1:5, 1:15])

            } else {
                timeit(" Training micromodel...",
                ModelCoefficients <- TrainMicromodel(df_micro, Target.Field, Predictor.Fields),
                NODATE = TRUE)
            }
        }

        if (INSPECT_RESULTS) {
            NonZeroCoef <- show_non_zero(ModelCoefficients)
            NonZeroCols <- show_non_zero(colSums(df_micro[, Predictor.Fields]))

            df_res <- data.frame(Prods = names(NonZeroCols),
                        DifProds = as.vector(NonZeroCols),
                        Coeff = as.vector(NonZeroCoef))
        }

        df_output <- rbind(df_output, c(MicroSegment, nr_observations, ModelCoefficients))
        colnames(df_output) <- c("MicroSegmentId", "Obs", names(ModelCoefficients))
        save_df(df_output)
    }


    df_output <- df_output[order(df_output[, User.Field]),]
    save_df(df_output, FILE.MATRIX)

    dfo <- df_output[complete.cases(df_output),]
    df_scores <- generate_recommendations(dfo)
    df_melt_scores <- melt(df_scores, id = c("ProductID", "ProductName"))
    save_df(df_melt_scores)




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
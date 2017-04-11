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
##      add intercept to sgd
##      try logit ?
##      Full batch GD fara MEDIE ! NG style :)
##      Implement implicit feedback content based filtering
##      Procedura get recoms: df_users, df_products : result: df_scores (id_client, id_produs, score)
##      Test microsegment vs 26000 produse (scoring market basket)
##      Test new product launch
##      Regularization L2 !
##


DEBUG = TRUE

DEMO = FALSE




###
### PLEASE REVIEW CONNECTION PARAMS
###
svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=StatisticalStore;"
uid <- "uid=andreidi;"
pwd <- "pwd=HypML2017"

Table.Name <- "_RFMM14_sgmidXItemXAttrib1"
###
### END CONNECTION PARAMS SECTION
###
Debug.Machine.Name <- "DAMIAN"
User.Field <- "MicroSegmentId"
Target.Field <- "Count"
Product.Field <- "ItemId"
Product.Name <- "ItemName"
NonPredictors.Fields <- c(
                        User.Field
                        ,Target.Field
                        , Product.Field
                        , Product.Name)



conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)


SQL_CONNECT = 2
# 1: use RODBC to remotely connect to sql server
# 2: use RevoScaleR to remotely connect to sql server (Microsoft R ONLY!)
# 3: XDF




MODULE.VERSION <- "0.9.7"
MODULE.NAME <- "BEHAVIOR_RECOMMENDATIONS"

SQL.MICROSEGMENTLIST <- paste0("SELECT DISTINCT ", User.Field, " FROM ", Table.Name)
SQL.NR.OBS <- paste0("SELECT COUNT(*) FROM ", Table.Name)
SQL.LOADMICROSEGMENT <- paste0("SELECT * FROM ", Table.Name, " WHERE ", User.Field, " = ")
SQL.TOP100 <- paste0("SELECT TOP 100 * FROM ", Table.Name)

SQL.DEMOPRODUCTS <- "SELECT * FROM _RFMM14_sgmidXItemXAttrib2_Random"
SQL.DEMOMICROSEG <- "SELECT * FROM bev5 WHERE MicroSegmentId IN (13, 35, 162, 353)"

if (Sys.info()["nodename"] == Debug.Machine.Name) {
    DEBUG <- TRUE
}

if (!DEBUG) {
    SQL.DATASELECTION <- "SELECT * FROM"
    PROCESS_ONLY_ENABLED <- FALSE
} else {
    SQL.DATASELECTION <- "SELECT TOP 10000 * FROM"
    PROCESS_ONLY_ENABLED <- TRUE
    SQL_CONNECT = 3
}



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


HELPER_LIB_VER <- "1.3.10"


FULL_DEBUG <- FALSE

options(digits = 3)

nonzero <- function(v) {
    non_zero_ind <- which(!(v==0))
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

logger(sprintf("Preparing helper lib v%s...\n",HELPER_LIB_VER))

timeit = function(strmsg, expr, NODATE = FALSE) {
  prf <- ""
  if(substr(strmsg,1,1)=="\n")
  {
    prf <- "\n"
    strmsg <- gsub("\n","",strmsg)
  }
  if (!NODATE) {
      optime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      prf <- paste0(prf, sprintf("[%s] ", optime))
  }
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
  if (FULL_DEBUG)
    logger("\nSaving data...\n")
  file_db_path <- get_data_dir()
  if(FULL_DEBUG)
    logger(sprintf("Used data directory: %s\n", file_db_path))
  FN <- paste0(MODULE.NAME, "_v", MODULE.VERSION, "_", log_ctime, "_data.csv")
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


load_df <- function(str_table, caching = TRUE) {
  
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
  } else if (SQL_CONNECT > 1)
      {
        if ((SQL_CONNECT == 2) || !file.exists(dfXdfFileName) || !caching) {
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


rx_load_sql <- function(str_sql)
{

    library(RevoScaleR)
    if(FULL_DEBUG)
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

##
## END HELPER FUNCTIONS
##


STOHASTIC <- TRUE
USE_CONFIDENCE <- FALSE

TrainMicromodel <- function(data, target_col, var_cols, CFIFD = FALSE) {
    if (DEBUG) {
        data <- data[order(data[,target_col], decreasing = TRUE),]
    }
    micro_coefs <- as.vector(rep(0, length(var_cols)))
    names(micro_coefs) <- var_cols
    y_train <- data[, target_col]
    items_coefs <- data[, var_cols]
    alpha <- 0.1
    confidence_scaler <- 1
    if (STOHASTIC) {
        ###
        ### stohastic gradient descent for content based filtering
        ###
        for (i in 1:nrow(data)) {
            if (DEBUG) {
                non_zero_coefs <- nonzero(micro_coefs)
                non_zero_obs <- nonzero(items_coefs[i,])
                cat("Non zero coefs:\n")
                print(non_zero_coefs)
                cat("Non zero obs:\n")
                print(non_zero_obs)
            }
            item_coefs_vect <- as.vector(sapply(items_coefs[i,], as.numeric)) # unlist(x_train[i,])
            H <- (micro_coefs %*% item_coefs_vect)[1]
            if (USE_CONFIDENCE) {
                conf <- 1 + confidence_scaler * y_train[i]
            } else {
                conf <- 1
            }
            if (CFIFD)
            {
                Y <- 1
                grad <- (H - Y) * item_coefs_vect * conf
            } else {
                Y <- y_train[i]
                grad <- (H - Y) * item_coefs_vect
            }
            micro_coefs <- micro_coefs - alpha * grad
        }
    } else {
        ###
        ### full batch gradient descent for content based filtering
        ###
    }

    return(micro_coefs)

}

GetProductsScoring <- function(df_micro_behavior, df_products, vars) {

    user_list <- df_micro_behavior[, User.Field]
    prod_matrix <- data.matrix(df_products[, vars])
    cust_matrix <- data.matrix(df_micro_behavior[, vars])
    cust_matrix_T <- base::t(cust_matrix)

    # compute:
    # MxN * N*K = MxK
    score_matrix <- prod_matrix %*% cust_matrix_T
    df_res <- as.data.frame(score_matrix)
    colnames(df_res) <- user_list
    if ("ItemName" %in% colnames(df_products)) {
        df_res$Product <- df_products$ItemName
    } else{
        df_res$Product <- df_products$ItemId
    }
    nr_cols <- length(colnames(df_res))
    new_order <- c(nr_cols, 1:(nr_cols - 1))
    df_res <- df_res[,new_order]
    return(df_res)
}

run_demo <- function(df_mc = NULL)
{
    df_demoprods <- rx_load_sql(SQL.DEMOPRODUCTS)
    if (is.null(df_mc))
    {
        df_demomicro <- rx_load_sql(SQL.DEMOMICROSEG) 
    } else
    {
        df_demomicro <- df_mc
    }
       
    Predictor.Fields <- setdiff(colnames(df_demoprods), NonPredictors.Fields)
    df_scores <- GetProductsScoring(df_demomicro, df_demoprods, Predictor.Fields)
    cat("Demo microsegments:\n")
    print(df_demomicro[,1:15])
    for (i in 2:min(length(colnames(df_scores)), 5))
    {
        cat(sprintf("\nTop for microcluster %s:\n", colnames(df_scores)[i]))
        print(head(df_scores[order(df_scores[, i], decreasing = TRUE),]))
    }
    cat("Done demo.\n")
}

if (DEMO)
{
##
## DEMO only section
##
    run_demo()

} else
{
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

    df <- rx_load_sql(SQL.TOP100)

    df_nr <- rx_load_sql(SQL.NR.OBS)

    df_microlist <- rx_load_sql(SQL.MICROSEGMENTLIST)


    nr_all_obs <- df_nr[1, 1]

    Predictor.Fields <- setdiff(colnames(df), NonPredictors.Fields)

    if (DEBUG) {
        #All.Microsegments <- unique(df_microlist[, User.Field])[1:5]

        Microsegments.Bad <- c(315, 200, 247)
        Microsegments.Good <- c(13, 35, 162, 353)
        All.Segments <- Microsegments.Bad

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


    for (MicroSegment in All.Microsegments) {
        nr_step <- nr_step + 1

        ssql <- paste(SQL.LOADMICROSEGMENT, toString(MicroSegment))
        df_micro <- rx_load_sql(ssql)
        df_micro[, Target.Field] <- df_micro[, Target.Field] * SCALE_FACTOR
        nr_observations <- nrow(df_micro)
        current_batch <- nr_observations / nr_all_obs
        status_prc <- status_prc + current_batch

        logger(sprintf("\n[Completed:%2.2f%% Step:%d/%d] Processing microsegment:%d with %d observations  ",
                    status_prc * 100, nr_step, Nr.Microsegments, MicroSegment, nr_observations))

        if (!USE_CUSTOM_MODEL) {
            regr_fomula <- get_formula(Target.Field, Predictor.Fields)
            MicroModel <- lm(formula = regr_fomula, data = df_micro)
            ModelCoefficients <- MicroModel$coefficients[-1]
        } else {
            if (DEBUG) {

                timeit(" >> Training CFIFD micromodel...",
                   Model2 <- TrainMicromodel(df_micro, Target.Field, Predictor.Fields, CFIFD = TRUE),
                   NODATE = TRUE)
                timeit(" >> Training STD micromodel...",
                   Model1 <- TrainMicromodel(df_micro, Target.Field, Predictor.Fields),
                   NODATE = TRUE)
                print(Model1[1:14])
                print(Model2[1:14])
                print(df_micro[1:5, 1:15])

            } else {
                timeit(" >> Training micromodel...",
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

        df_output <- rbind(df_output, c(MicroSegment,nr_observations, ModelCoefficients))
        colnames(df_output) <- c("MicroSegment","Obs", names(ModelCoefficients))
        save_df(df_output)
    }


    df_output <- df_output[order(df_output[, User.Field]),]
    save_df(df_output)


    run_demo(df_output)




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




##
##
##
## @script:       Churn Prediction System
## @v1_created:   2017.03.23
## @v2_created:   2017.06.19
## @v3_created:   2017.08.09
## @lastmodified: 2017.08.10
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei SIMION, Laurentiu PICIU, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##
##


# NEW R KERAS IM
#install.packages("Rcpp")
#install.packages("devtools")
#devtools::install_github("rstudio/reticulate", force=TRUE)
#devtools::install_github("r-lib/processx")
#library(processx)
#devtools::install_github("rstudio/tfruns")
#devtools::install_github("rstudio/tensorflow")
#devtools::install_github("rstudio/keras")


LIBS <- c("e1071", "reshape2", "reshape2",
          "xgboost", "caret", "MASS", "plyr", "pROC", "Rtsne", "keras", "ggplot2",
          "dummies")


#3	2	R_KERAS_REC_0872_INP_56_NET_28_14_nadam.hdf5	        87.253	36.108
#4	2	R_KERAS_REC_0873_INP_56_NET_46_23_rmsprop.hdf5	      87.403	36.076
#5	2	R_KERAS_REC_0883_INP_56_NET_28_14_rmsprop.hdf5	      88.35	  35.603
#6	2	R_KERAS_REC_0909_INP_56_NET_56_28_rmsprop.hdf5	      90.879	34.193
#10	2	R_KERAS_REC_0913_INP_56_NET_40_20_rmsprop.hdf5	      91.298	33.968
#7	2	R_KERAS_REC_0913_INP_56_NET_56_28_rmsprop.hdf5	      91.311	33.873
#8	2	R_KERAS_REC_0918_INP_56_NET_28_14_rmsprop.hdf5	      91.827	33.554
#14	2	R_KERAS_REC_0921_INP_56_NET_56_28_rmsprop_P0331.hdf5	92.063	33.127
#20	2	RK_R_0927_P_0328_N_56_28_rms_bn0_e50.hdf5	            92.654	32.701
#16	2	RK_R_0935_INP_56_NET_56_28_Adam.hdf5      	          93.493	32.615
#17	2	R_KERAS_REC_0938_INP_56_NET_56_28_Adam.hdf5	          93.779	32.084
#15	2	R_KERAS_REC_0953_INP_56_NET_56_28_Adam.hdf5	          95.191	31.302
#12	2	R_KERAS_REC_0960_INP_56_NET_40_20_RMSprop.hdf5	      95.941	30.02
#13	2	R_KERAS_REC_0969_INP_56_NET_56_28_RMSprop.hdf5	      96.875	28.381
#22	2	RK_R_0991_P_024_INP_56_NET_56_28_RMSprop_bn0_e50.hdf5	99.17	24.346
#21	2	RK_R_0993_P_0235_N_56_28_rms_bn0_e50.hdf5	            99.199	23.305
#19	2	R_KERAS_REC_0993_INP_56_NET_56_28_RMSprop.hdf5	      99.324	22.388
#18	2	R_KERAS_REC_0997_INP_56_NET_56_28_RMSprop.hdf5	      99.668	22.414



##
## PARAMS
##

best_model_files <- c(
  'RK_R_0935_INP_56_NET_56_28_Adam.hdf5'
)

SAVE_RESULTS <- TRUE
SHOW_PLOTS <- TRUE

Churn.Threshold <- 0.40

RUN_THRESHOLD <- FALSE

Churn.Threshold.List <- seq(from = 0.4,to = 0.5, length.out = 10)


##
## DONE PARAMS
##


User.Field <- "CUST_ID"



# use predefined repository folder or just "" for automatic current folder
USE_REPOSITORY <- "d:/GoogleDrive/_hyperloop_data/churn_v3"
USED_PROCESSING_POWER <- 0.85

DEBUG = FALSE
DEBUG_HTSS = TRUE # if true then load even if cached==true

DEMO = FALSE





###
### PLEASE REVIEW CONNECTION PARAMS
###
svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=AIDB;"
uid <- "uid=andreidi;"
pwd <- "pwd=damian2017!"
Debug.Machine.Name <- "DAMIAN"
Debug.Machine2.Name <- "ANDREEA"
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
  USE_REPOSITORY <- "C:/Users/slash/Google Drive/_hyperloop_data/churn_v3"
}

if (Current.Machine.Name == Debug.Machine3.Name) {
  DEBUG <- TRUE
  USE_REPOSITORY <- "C:/GoogleDrive/_hyperloop_data/churn_v3"
}


SQL.GETCHURN <- "EXEC [SP_GET_CHURN] @TRAN_PER_ID = %d, @CHURN_PER_ID = %d, @SGM_ID = %d, @VER=20171005"
SQL.GETSTATS <- "EXEC [SP_CUST_LIST] @TRAN_PER_ID = %d"
SQL.INITMODEL.SP <- 'EXEC [SP_ADD_CHURN_MODEL] @SGM_ID=%d, @MODEL_NAME="%s", @MODEL_DESC="%s", @TRAN_PER_ID=%d, @CHURN_PER_ID=%d, @MODEL_FILE="%s", @SCORE1=%.3f, @SCORE2=%.3f'

SQL.DATASELECTION <- "SELECT * FROM "

Table.Results <- "MODEL_RESULTS"

Target.Field <- "CHURN"


Overall.Fields <- c("R", "F", "M")

NonPredictors.Fields <- c(
  User.Field
  , Target.Field
)


New.Fields <- c('SGM_NO', 'MICRO_SGM_ID', 'SEX', 'AGE', 'GFK_SGM', 'C_R', 'C_F', 'C_M', 
                'C_MARGIN', 'C_PHARMA', 'C_COSMETICE', 'C_DERMOCOSMETICE', 'C_BABY', 
                'C_NEASOCIATE', 'C_DR_HART', 'C_NUROFEN', 'C_APIVITA', 'C_AVENE', 'C_L_OCCITANE', 
                'C_VICHY', 'C_BIODERMA', 'C_LA_ROCHE_POSAY', 'C_L_ERBOLARIO', 'C_PARASINUS', 'C_TRUSSA', 
                'C_SORTIS', 'C_NESTLE', 'C_OXYANCE', 'C_TERTENSIF', 'C_ASPENTER', 'C_ALPHA', 'C_BATISTE', 
                'C_STREPSILS', 'C_CHAPTER', 'C_DR_ORGANIC', 'C_FARMEC', 'C_HERBOSOPHY', 'C_IVATHERM', 'C_KLORANE_BEBE', 
                'C_MELVITA', 'C_SPLAT', 'C_ZDROVITAL',
                'MARGIN')



Short.Predictor.Fields <- c("R_1", "M_1", "F_1",
                            "R_2", "M_2", "F_2",
                            "R_3", "M_3", "F_3",
                            "R_4", "M_4", "F_4"
)

Margin.Field <- "MARGIN"
Revenue.Field <-"R"



conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                svr, db, uid, pwd)



MODULE.VERSION <- "Predictor v3.4.3"
MODULE.NAME <- "CHURN_V3"
MODULE.NAME.SHORT <- "CHRN_PRD_3"





Dataset.Size <- 1.0 # % out of dataset (100% usually)
Training.Size <- 0.6 # % out of Dataset.Size
Validation.Size <- 0.5 # % out of (Dataset.Size - Training.Size)

NR_ROWS <- 2e7



##
## HELPER FUNCTIONS
##
##

PlotTimeStamp <- format(Sys.time(), "%Y%m%d%H%M")

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

get_script_file <- function() {
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

SCRIPT_FILE <- get_script_file()
SCRIPT_PATH <- dirname(SCRIPT_FILE)

get_data_dir <- function(new_subfolder = NULL) {
  s_data_dir <- ""
  if (USE_REPOSITORY != "") {
    s_data_dir <- USE_REPOSITORY
    if(!is.null(new_subfolder))
      s_data_dir <- file.path(s_data_dir,new_subfolder)
    dir.create(s_data_dir, recursive = TRUE, showWarnings = FALSE)
  } else {
    s_data_dir <- dirname(get_script_file())
  }
  return(s_data_dir)
}

all_log <- ""
log_fnpath <- get_data_dir()
log_ctime <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_FN <- paste0(log_ctime,"_log_R_", MODULE.NAME, ".txt")
LogFileName <- file.path(log_fnpath, "_logs",log_FN)

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

GetObjectOuput <- function(obj) {
  lst <- capture.output(obj)
  res <- ""
  for (s in lst) {
    res <- paste0(res, s, "\n")
  }
  return(res)
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
  file_db_path <- get_data_dir('_output')
  if (FULL_DEBUG)
    logger(sprintf(" Used data directory: %s\n", file_db_path))
  FN <- paste0(MODULE.NAME, "_v", gsub("\\.", "_", MODULE.VERSION), "_",
               log_ctime, "_data.csv")
  if (sfn != "")
    if (simple_name)
      FN <- sfn else
        FN <- paste0(log_ctime,"_",sfn, "_data.csv")
  FileName <- file.path(file_db_path, FN)
  timeit(sprintf(" Saving File:[%s] ...", FileName),
         write.csv(x = df, file = FileName, row.names = TRUE))
}

SaveResults <- function(df_r)
{
  save_df(df_r, sfn = "r_cv_res")
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
    logger(sprintf("  Loading %s", fn))
    df <- read.csv(fn, row.names = 1)
  } else {
    logger(sprintf("  Can't find file %s", fn))
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

UploadToSQL <- function(dest_table, source_df) {
  svrTable <- RxSqlServerData(table = dest_table, connectionString = conns)
  logger(sprintf(" rxDataStep Uploading dataframe with size (%s) to table [%s] ...", toString(dim(source_df)), dest_table))
  rxDataStep(inData = source_df, outFile = svrTable, append = "rows")
  logger(" Done uploading.")
}


normalize <- function(x) {
  mx <- max(x)
  mn <- min(x)
  if (mx == 0 && mn == 0) mx <- 1
  return((x - mn) / (mx - mn))
}



save_plot <- function(sfn) {
  
  sfn <- gsub("[- .%/\\\n]+","_", sfn)
  
  file_db_path <- get_data_dir()
  stime <- PlotTimeStamp 
  FN <- file.path(file_db_path,'_output', paste0(stime, "_", sfn, ".png"))
  logger(sprintf(" Saving plot: %s\n", FN))
  dev.print(device = png, file = FN, width = 800, height = 600)
  
  return(FN)
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

LoadCSV <- function(sFN) {
  logger(sprintf(" Loading CSV [%s]", sFN))
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


###
### Clustering lib v2
###

CustomKMeans = function(x, centers, nstart, USE_REVOSCALER_KMEANS = FALSE) {
  if (USE_REVOSCALER_KMEANS) {
    factors <- colnames(x)
    kformula = as.formula(paste("~", paste(factors, collapse = "+")))
    sformula = toString(kformula)
    logger(paste0("  Using RevoScaleR rxKmeans: [", sformula, "] "))
    model <- rxKmeans(formula = kformula,
                      data = x,
                      numClusters = centers,
                      numStarts = nstart,
                      reportProgress = 0)
  } else {
    logger("  Using standard kMeans: ")
    model <- kmeans(x = x,
                    nstart = nstart,
                    centers = centers
    )
  }
  return(model)
}

SimpleClustering <- function(df_input, column_list, n_centers, n_micros)
  ##
  ## clustering without any column scaling/norming
  ##
{
  centroid_labels <- c()
  subcluster_column_list <- c()
  
  for (i in 1:length(column_list)) {
    col <- column_list[i]
    new_col <- paste0("cen", i, "_", substr(col, 1, 5))
    sub_new_col <- paste0("sub", i, "_", substr(col, 1, 5))
    centroid_labels <- c(centroid_labels, new_col)
    subcluster_column_list <- c(subcluster_column_list, sub_new_col)
  }
  
  # base clusterization model
  clust_column <- "IDCLU" # IMPORTANT: segment ID (sorted from worst to best)
  label_column <- "LABEL" # IMPORTANT: inferred label for ID
  score_column <- "SCORE" # IMPORTANT: score of each segment
  loss_column <- "LOSSC" # loss of the model
  subcl_column <- "SCLID" # IMPORTANT: ID of the subcluster (sorted from worst to best)
  nrcus_column <- "NR_CL" # number of observation in main segment
  tempc_column <- "TMPCL" # temporary segment - should be removed at cleanup
  temp_scl_col <- "TMPSC" # temporary subcluster - should be removed at cleanup
  subscore_col <- "SUBSC" # IMPORTANT: subcluster score
  tsnex_column <- "TSNEX" # IMPORTANT: t-SNE X attribute
  tsney_column <- "TSNEY" # IMPORTANT: t-SNE Y attribute
  
  micro_fields <- c(clust_column, subcl_column,
                    centroid_labels, subcluster_column_list,
                    label_column, score_column,
                    tsnex_column, tsney_column)
  
  segment_labels <- c("1-Very Low", "2-Low", "3-Average", "4-Good", "5-Best")
  
  norm_columns <- column_list
  nr_input_fields <- length(column_list)
  
  logger(sprintf("Main clustering on data with shape: (%s)...\n",
                 toString(dim(df_input[, norm_columns]))))
  t0 <- proc.time()
  
  timeit(" Main kmeans: ",
         trained_model <- CustomKMeans(
           x = df_input[, norm_columns],
           centers = n_centers, nstart = 30))
  df_input[, tempc_column] <- trained_model$cluster
  
  df_centers <- trained_model$center
  scores <- rowMeans(df_centers)
  TScore <- trained_model$tot.withinss
  df_centers <- data.frame(df_centers, scores, TScore)
  colnames(df_centers) <- c(centroid_labels, score_column, loss_column)
  df_centers[, tempc_column] <- 1:nrow(df_centers)
  df_centers <- df_centers[order(df_centers[, score_column]),]
  df_centers[, clust_column] <- 1:nrow(df_centers)
  df_centers[, label_column] <- segment_labels[1:n_centers]
  
  logger("Merging...\n")
  timeit(" Merge", df_input <- merge(df_input, df_centers, by = tempc_column))
  
  
  
  clusterList <- unique(df_input[, clust_column])
  n_cl <- length(clusterList)
  
  for (i in 1:n_cl) {
    cl <- clusterList[i]
    df_centers[df_centers[, clust_column] == cl, nrcus_column] <- nrow(
      df_input[df_input[, clust_column] == cl,])
  }
  df_downsampled <- data.frame()
  # generate sub-clusters for each cluster
  for (i in 1:n_cl) {
    cl <- clusterList[i]
    logger(sprintf("Subclustering cluster %d...\n", cl))
    timeit(" Subcluster kmeans: ",
           cl_model <- CustomKMeans(
             df_input[df_input[, clust_column] == cl, norm_columns],
             centers = n_micros, nstart = 10))
    
    
    df_subcenters <- cl_model$center
    scores <- rowMeans(df_subcenters)
    
    
    df_subcenters <- data.frame(df_subcenters, scores)
    colnames(df_subcenters) <- c(subcluster_column_list, subscore_col)
    
    df_subcenters[, clust_column] <- cl
    df_subcenters[, temp_scl_col] <- 1:nrow(df_subcenters)
    df_subcenters <- df_subcenters[order(df_subcenters[, subscore_col]),]
    df_subcenters[, subcl_column] <- 1:nrow(df_subcenters)
    
    df_input[df_input[, clust_column] == cl, temp_scl_col] <- cl_model$cluster
    
    df_centers[df_centers[, clust_column] == cl, nrcus_column] <- nrow(
      df_input[df_input[, clust_column] == cl,])
    
    df_downsampled <- rbind(df_downsampled, df_subcenters)
  }
  
  
  library(Rtsne)
  logger("Applying t-SNE...\n")
  timeit(" t-SNE ",
         rtsne_res <- Rtsne(as.matrix(df_downsampled[, subcluster_column_list]),
                            check_duplicates = FALSE,
                            pca = TRUE))
  df_downsampled[, tsnex_column] <- rtsne_res$Y[, 1]
  df_downsampled[, tsney_column] <- rtsne_res$Y[, 2]
  
  logger("Final Merge...")
  df_input <- merge(df_input, df_downsampled, by = c(clust_column, temp_scl_col))
  
  
  dfa <- data.frame(df_centers[, c(label_column, clust_column, score_column,
                                   nrcus_column, centroid_labels)])
  
  t1 <- proc.time()
  elapsed <- t1[3] - t0[3]
  logger(sprintf("Total processing time %.2f min\n", elapsed / 60))
  
  result_list = list("dataframe" = df_input, "fields" = micro_fields,
                     "f1" = subcluster_column_list[1],
                     "f2" = subcluster_column_list[2],
                     "f3" = subcluster_column_list[3],
                     "seg" = clust_column,
                     "norm_columns" = norm_columns,
                     "label" = label_column)
  return(result_list)
  
}

FullClustering <- function(df, column_list, n_centers, n_micros,
                           log_list, log_all_columns = FALSE)
  ##
  ## full clustering including normalization
  ##
{
  norm_columns <- c()
  nr_input_fields <- length(column_list)
  
  logger(sprintf("Normalising %d columns...", nr_input_fields))
  for (i in 1:nr_input_fields) {
    col_name <- column_list[i]
    new_col <- paste0("P", i, "_", substr(col_name, 1, 5))
    df[, new_col] <- df[, col_name] #copy data
    
    norm_columns <- c(norm_columns, new_col)
    is_log = 0
    if (length(log_list) >= i)
      is_log <- log_list[i]
    if ((is_log == 1) || (log_all_columns)) {
      below_zero_idxs <- (df[, new_col] <= 0)
      EXP_BELOW_ZERO <- FALSE
      if (sum(below_zero_idxs) > 0) {
        if (EXP_BELOW_ZERO) {
          df[below_zero_idxs, new_col] <-
            exp(df[below_zero_idxs, new_col]) * 1e-3
        } else {
          fdelta <- min(df[below_zero_idxs, new_col])
          df[below_zero_idxs, new_col] <-
            (df[below_zero_idxs, new_col] - fdelta + 1) * 1e-5
        }
      }
      df[, new_col] <- log(df[, new_col])
    }
    
    min_c <- min(df[, new_col])
    max_c <- max(df[, new_col])
    df[, new_col] <- (df[, new_col] - min_c) / (max_c - min_c)
  }
  
  clustering_results <- SimpleClustering(df, norm_columns,
                                         n_centers = n_centers,
                                         n_micros = n_micros)
  return(clustering_results)
  
}

TestClustering <- function(df, columns) {
  logger("Testing clustering ...")
  err_list <- c()
  for (nr_centers in 2:10) {
    logger(sprintf(" Testing clustering for %d centers...", nr_centers))
    km <- CustomKMeans(df[, columns,], centers = nr_centers, nstart = 10)
    err_list <- c(err_list, km$tot.withinss)
  }
  return(err_list)
}


PlotClustering <- function(df_micro, field1, field2, seg_field, overall_field = NULL,
                           x_lab = NULL, y_lab = NULL, manual_colors = NULL) {
  
  shape_field = seg_field
  color_field = seg_field
  if(!is.null(overall_field)){
    color_field = overall_field
  }
  
  df_micro[, shape_field] <- as.factor(df_micro[, shape_field])
  df_micro[, color_field] <- as.factor(df_micro[, color_field])
  
  
  logger(sprintf("Plotting microsegmentation. Colors: %s, Shapes: %s ...",
                 toString(unique(df_micro[, color_field])), 
                 toString(unique(df_micro[, shape_field]))))
  
  current_plot <- qplot(df_micro[, field1], df_micro[, field2],
                        shape = df_micro[, shape_field],
                        color = df_micro[, color_field],
                        size = I(2.0),
                        alpha = 0.5)
  current_plot <- current_plot + labs(x = field1, y = field2)
  current_plot <- current_plot + scale_shape_discrete(name  ="Segment importance")
  if(!is.null(manual_colors))
  {
    
    current_plot <- current_plot + scale_color_manual(values=manual_colors, name= "Overall importance")
  } else{
    current_plot <- current_plot + scale_color_discrete(name  ="Segment importance")
  }
  if(!is.null(x_lab)){
    current_plot <- current_plot + xlab(x_lab)
  }
  if(!is.null(y_lab)){
    current_plot <- current_plot + ylab(y_lab)
  }
  return(current_plot)
}



SaveLastPlot <- function(sfn)
{
  file_db_path <- get_data_dir()
  stime <- PlotTimeStamp
  FN <- file.path(file_db_path, paste0(stime, "_", sfn, "_PLOT.png"))
  logger(sprintf(" Saving ggplot: %s...", FN))
  ggsave(FN)
  logger("Done saving ggplot.")
}
###
### END Clustering lib v2
###

##
## ChurnV2 lib
##

LoadChurnData <- function(iTransPer, iChurnPer, iSGM) {
  logger(sprintf("Loading period %d churn data (trans=%d, churn=%d, SGM=%d)...",
                 iTransPer, iTransPer, iChurnPer, iSGM))
  sSQL <- sprintf(SQL.GETCHURN, iTransPer, iChurnPer, iSGM)
  df <- LoadBatch(sSQL)
  logger(sprintf("Done loading period %d churn data (trans=%d, churn=%d).", iTransPer,
                 iTransPer, iChurnPer))
  return(df)
}

LoadCustomerStatInfo <- function(iPeriod) {
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

InstallKeras <- function() {
  logger("Installing keras...")
  
  if (!is.element("devtools", .packages(all.available = TRUE))) {
    logger("Installing devtools")
    install.packages("devtools")
  }
  library(devtools)
  if (!is.element("keras", .packages(all.available = TRUE))) {
    devtools::install_github("r-lib/processx")
    logger("Downloading keras from github")
    devtools::install_github("rstudio/keras")
    library(keras)
    logger("Installing TensorFlow...")
    install_tensorflow()
    logger("Done installing TensorFlow")
  } else {
    library(keras)
  }
}

GetActiv <- function(obj)
{
  return(strsplit(toString(obj$activation)," ")[[1]][2])
}

GetNNInfo <- function(model)
{
  str_res <- ""
  for(l in 0:(length(model$layers)-1)) 
  {
    pre <- "->"
    sunits <-0
    layer <- get_layer(model, index = l)
    sname <- toString(layer)
    if(l==0) {
      pre <- ""
      sunits <- layer$input_shape[[2]]
    }else if(sname=="Dropout")
    {
      sunits <- toString(layer$rate)  
    }else if(sname=="Dense")
    {
      sunits <- toString(layer$units) 
      if(l==(length(model$layers)-1))
        sunits <- paste(sunits,GetActiv(layer))
      
    }else if(sname=="Activation"){
      sunits <-  GetActiv(layer)
    }
    sshort <- substr(sname,1,3)
    str_res <- paste0(str_res,pre,sshort,"[",sunits,"]")
  }
  str_res <- paste0(str_res, " O:[", model$optimizer,"]")
  return(str_res)
}

ChurnClassifier <- function(hidden_layers = c(128), nr_inputs, dropout_level = 1, 
                            BN = TRUE, act = 'elu', opt ='rmsprop' ) 
{
  logger(sprintf("Preparing NN (input:%s layers:[%s])",
                 toString(nr_inputs), toString(hidden_layers)))
  nr_hiddens = length(hidden_layers)
  
  input_layer <- layer_input(shape=c(nr_inputs))
  last_layer <- input_layer
  h1_units = hidden_layers[1]
  
  
  for(hid in 1:nr_hiddens)
  {
    if(dropout_level>=1 && hid>1)
    {
      last_layer <- layer_dropout(last_layer, 0.5)
    }
    last_layer <- layer_dense(last_layer,units = hidden_layers[hid])
    if(BN)
    {
      last_layer <- layer_batch_normalization(last_layer)
    }
    if(act!='')
    {
      last_layer <- layer_activation(last_layer,activation = act)
    }
  }
  
  
  if(dropout_level>=2)
  {
    last_layer <- layer_dropout(last_layer, rate = 0.5)
  }
  output_layer <- layer_dense(last_layer, units = 1, activation = "sigmoid") 
  
  model <- keras_model(inputs = input_layer, outputs = output_layer)
  model %>%
    compile(
      optimizer = opt,
      loss = 'binary_crossentropy',
      metrics = c('accuracy')
    )
  if (FULL_DEBUG) {
    logger(sprintf("Neural Network layout:\n%s",GetObjectOuput(summary(model))))
  }else logger(sprintf("Model: %s", GetNNInfo(model)))
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


conf_mats <- list()
i_conf <- 0

ResetConfusionStats <- function() {
  conf_mats <<- list()
  i_conf <<- 0
}

conf_mat_train <- NULL
conf_mat_test <- NULL

GetPredStats <- function(id_model, yhat, y, yhat_train = NULL, y_train = NULL, hiddens = NULL) {
  logger(sprintf(" Computing confusion matrix for %d observations", length(y)))
  if(FULL_DEBUG)
    logger(sprintf("   yhat_test:[%s] y_test:[%s]", toString(yhat[1:5]), toString(y[1:5])))
  cf_test <- confusionMatrix(yhat, y, positive = "1")
  conf_mat_test <<- cf_test
  i_conf <<- i_conf + 1
  conf_mats[[i_conf]] <<- cf_test
  
  v_kappa_test <- cf_test$overall["Kappa"]
  v_accuracy_test <- cf_test$overall["Accuracy"]
  v_recall_test <- cf_test$byClass['Recall']
  v_precision_test <- cf_test$byClas[["Pos Pred Value"]]
  v_f1_test <- 2 * (v_precision_test*v_recall_test) / (v_precision_test + v_recall_test)
  
  v_kappa_train <- -1
  v_accuracy_train <- -1
  v_recall_train <- -1
  
  if (!is.null(yhat_train)) {
    logger(sprintf(" Computing confusion matrix for %d observations", length(y_train)))
    cf_train <- confusionMatrix(yhat_train, y_train, positive = "1")
    conf_mat_train <<- cf_train
    v_kappa_train <- cf_train$overall["Kappa"]
    v_accuracy_train <- cf_train$overall["Accuracy"]
    v_recall_train <- cf_train$byClass['Recall']
    
  }
  
  results <- data.frame(id_model, i_conf, 
                        v_f1_test,
                        v_precision_test, v_recall_test, 
                        v_kappa_test, v_accuracy_test, 
                        v_accuracy_train, v_kappa_train, v_recall_train)
  
  colnames(results) <- c("ModelID", "CnfMatID",
                         "TestF1",
                         "TestPrec","TestRecall",
                         "TestKappa", "TestAcc", 
                         "TrainAcc", "TrainKappa", "TrainRecall")
  
  if (!is.null(hiddens)) {
    results["Layout"] <- toString(hiddens)
  }
  
  
  return(results)
}

Get_DNN_Layouts <- function(nr_hid, QUICK = FALSE) {
  ##
  ## best churn models so far:
  ## /2 /4 /8
  ## /2 /4
  ## x2 /2
  ## x2 x1
  ##
  # ModelID TestAcc TestKappa TestRecall TrainAcc TrainKappa TrainRecall     Layout NPRED THRS  
  #       6   0.696     0.340     0.7775    0.697      0.340      0.7769     23, 11    46 0.40
  #       9   0.689     0.336     0.7940    0.690      0.337      0.7935     49, 24    49 0.40
  #      15   0.688     0.335     0.7947    0.689      0.335      0.7939     96, 24    12 0.45
  #      16   0.660     0.311     0.8290    0.661      0.311      0.8289      12, 6    12 0.40
  #      20   0.663     0.314     0.8299    0.663      0.315      0.8299       6, 3    12 0.40
  #       3   0.661     0.313     0.8328    0.662      0.314      0.8328     92, 46    46 0.40
  #      1
  
  
  #       2   0.623     0.282     0.8793    0.623      0.283      0.8787     46, 23    46 0.45
  #       5   0.546     0.220     0.9367    0.546      0.220      0.9359 46, 23, 11    46 0.40
  #       2   0.535     0.212     0.9430    0.536      0.212      0.9425     46, 23    46 0.40  
  
  #       4   0.686    0.3324     0.7948    0.686     0.3320      0.7951    112, 56    56 0.40
  #      20   0.685    0.3311     0.7968    0.684     0.3304      0.7978     29, 14    59 0.45
  #      28   0.679    0.3253     0.8026    0.679     0.3254      0.8039     24, 12    12 0.40
  #      16   0.666    0.3179     0.8294    0.665     0.3170      0.8295    118, 59    59 0.40
  #       8   0.626    0.2704     0.8337    0.625     0.2684      0.8323     28, 14    56 0.40
  #      10   0.654    0.3075     0.8442    0.653     0.3059      0.8434  28, 14, 7    56 0.45
  #       3   0.652    0.3064     0.8471    0.651     0.3050      0.8468     56, 28    56 0.40
  #      20   0.610    0.2726     0.8928    0.609     0.2707      0.8917     29, 14    59 0.40
  #      10   0.559    0.2314     0.9310    0.558     0.2309      0.9312  28, 14, 7    56 0.40
  #  ModelID TestAcc TestKappa TestRecall TrainAcc TrainKappa TrainRecall    Layout NPRED THRS  
  
  dnn_tests <- list()
  if (QUICK) {
    dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid %/% 2)
  } else 
  {
    if (Current.Machine.Name == HPC.Machine.Name) 
    {
      #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 8, nr_hid *4, nr_hid *2, nr_hid)
      #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 8, nr_hid *2)
      #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid, nr_hid %/% 2)
      dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid)
      dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid %/% 2)
      #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid, nr_hid %/% 2)
      
      if (nr_hid >= 8) 
      {
        #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid, nr_hid %/% 2, nr_hid %/% 4)
        dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid %/% 2, nr_hid %/% 4)
        #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid %/% 4)
        #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid, nr_hid %/% 2, 
        #                                        nr_hid %/% 4)
      }
      
      if (nr_hid >= 16) 
      {
        dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid %/% 2, nr_hid %/% 4, nr_hid %/% 8)
        #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid %/% 4, nr_hid %/% 8)
        #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid, nr_hid %/% 2, 
        #                                        nr_hid %/% 4, nr_hid %/% 8)
      }
    } else 
    {
      dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid %/% 2)
      #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid, nr_hid %/% 2)
      if (nr_hid >= 8) 
      {
        #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid * 2, nr_hid %/% 4)
        #dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid, nr_hid %/% 2, nr_hid %/% 4)
        dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid %/% 2, nr_hid %/% 4)
      }
      if (nr_hid >= 16) 
      {
        dnn_tests[[length(dnn_tests) + 1]] <- c(nr_hid %/% 2, nr_hid %/% 4, nr_hid %/% 8)
      }
    } # end machine not HPC
  }
  return(dnn_tests)
}

## CONFUSION MATRIX PLOT

PlotChurnConfusion <- function(cm, class1="Not Churn", class2="Churn", plot_title = 'Matricea confuziei modelului de churn') {
  
  layout(matrix(c(1,1,2)))
  par(mar=c(2,2,2,2))
  plot(c(100, 345), c(300, 450), type = "n", xlab="", ylab="", xaxt='n', yaxt='n')
  title(plot_title, cex.main=2)
  
  # create the matrix 
  rect(150, 430, 240, 370, col='#3F97D0')
  text(195, 435, class1, cex=1.5)
  rect(250, 430, 340, 370, col='#F7AD50')
  text(295, 435, class2, cex=1.5)
  text(125, 370, 'Predicted', cex=1.6, srt=90, font=2)
  text(245, 450, 'Actual', cex=1.6, font=2)
  rect(150, 305, 240, 365, col='#F7AD50')
  rect(250, 305, 340, 365, col='#3F97D0')
  text(140, 400, class1, cex=1.5, srt=90)
  text(140, 335, class2, cex=1.5, srt=90)
  
  # add in the cm results 
  res <- as.numeric(cm$table)
  text(195, 400, res[1], cex=1.6, font=2, col='white')
  text(195, 335, res[2], cex=1.6, font=2, col='white')
  text(295, 400, res[3], cex=1.6, font=2, col='white')
  text(295, 335, res[4], cex=1.6, font=2, col='white')
  
  # add in the specifics 
  plot(c(100, 0), c(100, 0), type = "n", xlab="", ylab="", main = "DETAILS", xaxt='n', yaxt='n')
  text(10, 85, names(cm$byClass[1]), cex=1.5, font=2)
  text(10, 70, round(as.numeric(cm$byClass[1]), 3), cex=1.5)
  text(30, 85, names(cm$byClass[2]), cex=1.5, font=2)
  text(30, 70, round(as.numeric(cm$byClass[2]), 3), cex=1.5)
  text(50, 85, names(cm$overall[2]), cex=1.5, font=2)
  text(50, 70, round(as.numeric(cm$overall[2]), 3), cex=1.5)
  text(70, 85, names(cm$overall[1]), cex=1.5, font=2)
  text(70, 70, round(as.numeric(cm$overall[1]), 3), cex=1.5)
  text(90, 85, names(cm$byClass[7]), cex=1.5, font=2)
  text(90, 70, round(as.numeric(cm$byClass[7]), 3), cex=1.5)
  
  # add in the accuracy information 
  text(30, 35, names(cm$byClass[6]), cex=1.8, font=2)
  text(30, 20, round(as.numeric(cm$byClass[6]), 3), cex=1.7)
  text(70, 35, names(cm$byClass[5]), cex=1.8, font=2)
  text(70, 20, round(as.numeric(cm$byClass[5]), 3), cex=1.7)
  save_plot(plot_title)
} 
##


## 
## PLOTS !!!
##



ShowSavePlot <- function(plot_title,gplot = NULL)
{
  if(is.null(gplot))
  {
    gplot = last_plot()
  }
  plot_title <- gsub("[- .%/\\\n]+","_", plot_title)
  FN <- file.path(USE_REPOSITORY, "_output") 
  stime <- PlotTimeStamp
  FN <- file.path(FN,paste0(stime,"_",plot_title,".png"))
  logger(sprintf("Plotting/saving %s", FN))
  print(gplot)
  ggsave(FN, gplot, device="png")
  
}

PlotBarChart <- function(df_in, col, plot_title = "PlotBarChart", show_prc = TRUE)
{
  if (plot_title=="")
  {
    plot_title <- sprintf("Barplot of %s",col)
  }
  df_sub <- as.data.frame(df_in[,c(User.Field,col)])
  df_sub[,col] <- as.factor(df_sub[,col])
  df_tab <- as.data.frame(table(df_sub[,col]))
  colnames(df_tab)[1] <- col
  df_tab$lab <- paste0(as.character(round(100 * df_tab$Freq / sum(df_tab$Freq),1)),"%")
  gplot <- ggplot(data = df_sub, aes_string(x=col, fill=col))
  gplot <- gplot + geom_bar(aes( y=(..count..))) #aes_string(x = col))
  if (show_prc)
  {
    gplot <- gplot + geom_text(data=df_tab,aes_string(x=col,y="Freq",label="lab"),vjust=0)
  }else
  {
    gplot <- gplot + geom_text(stat="count", aes(label=(..count..), y=(..count..)), vjust=0, color = "black")
  }
  gplot <- gplot + ggtitle(plot_title)
  ShowSavePlot(plot_title = plot_title, gplot = gplot)
  #return(gplot)
}

PlotValuesChart <- function(df_in, col, sum_col, plot_title = "PlotValuesChart")
{
  if(plot_title=="")
  {
    plot_title <- sprintf("Levels of %s",col)
  }
  df_in$label <- paste0(as.character(round(df_in[,sum_col] / 1000000,2)),"M")
  gplot <- ggplot(data=df_in, aes_string(x=col, y=sum_col, fill=col))
  gplot <- gplot + geom_bar(stat="identity")
  gplot <- gplot + geom_text(aes_string(label="label"), vjust=1.5, colour="white")
  gplot <- gplot + ggtitle(plot_title)
  
  ShowSavePlot(plot_title = plot_title, gplot = gplot)
  
  #return(gplot)
}

PlotHistChart <- function(df_in, col, plot_title = "PlotHistChart", show_numbers = T)
{
  if (plot_title=="")
  {
    plot_title <- sprintf("Histogram of %s",col)
  }
  #gplot <- qplot(df_in[,col],geom="histogram",binwidth = 0.5, xlab = col,fill=I("blue"))
  
  gplot <- ggplot(data=df_in, aes_string(x=col))
  gplot <- gplot + geom_histogram(#breaks=seq(20, 50, by =2), 
    col="blue"
    ,aes(fill=..count..)
  )
  if(show_numbers)
    gplot <- gplot + stat_bin(binwidth=1, geom="text", aes(label=..count..), vjust=-1.5) 
  gplot <- gplot + ggtitle(plot_title)
  
  ShowSavePlot(plot_title = plot_title, gplot = gplot)
  
  #return(gplot)
}

AddChurnModel <- function(model_name, model_desc, model_tper, model_cper, model_file,
                          model_sgm, model_score1, model_score2)
{
  #@SGM_ID=%d, @MODEL_NAME="%s", @MODEL_DESC="%s", @TRAN_PER_ID=%d, @CHURN_PER_ID=%d, @MODEL_FILE="%s", @SCORE1=%.3f, @SCORE2=%.3f'
  sSQL <- sprintf(SQL.INITMODEL.SP, model_sgm, model_name,model_desc,
                  model_tper,model_cper, model_file, model_score1, model_score2)
  
  logger(sprintf("Initializing churn model [%s] (trans=%d, churn=%d)...",
                 model_name, model_tper, model_cper))
  df <- LoadBatch(sSQL, cached = F)
  logger(sprintf("Done initializing churn model [%s] (trans=%d, churn=%d).",
                 model_name, model_tper, model_cper))
  return(df[[1]])  
}


##
## Keras Model Checkpoint helper functions
##

CurrentModelPrefix <- format(Sys.time(), "%Y%m%d%H%M%S")

GetSummaryData <- function(df_in, by_col, sum_col)
{
  df_t <- aggregate(x=df_in[,sum_col], by=list(df_in[,by_col]), FUN="sum")
  colnames(df_t) <-c(by_col, paste0(sum_col,"_SUM"))
  return(df_t)
}

GetModelsDir <- function()
{
  return(get_data_dir(new_subfolder = "_models"))
}



model_prefix <- "_rk_"

GetModels <- function()
{
  return(list.files(path=GetModelsDir(), pattern = model_prefix,  full.names = TRUE))
}

MoveFile <- function(from, to) {
  todir <- dirname(to)
  if (!isTRUE(file.info(todir)$isdir)) dir.create(todir, recursive=TRUE)
  file.rename(from = from,  to = to)
}

EmptyModelsDir <- function()
{
  
  model_files <- GetModels()
  target_dir <- file.path(GetModelsDir(),"_archive")
  nr_models <- length(model_files)
  if(nr_models>0)
  {
    logger(sprintf("Moving %d models to archive...",nr_models))
    for(model_file in model_files)
    {
      moved <- file.path(target_dir,basename(model_file))
      MoveFile(model_file, moved)
    }    
  }
  
}

GetModelFileTemplate <- function(info = '')
{
  fn <-paste0(CurrentModelPrefix,sprintf("%s%s_ep_{epoch:02d}_vl_{val_loss:.4f}.hdf5", 
                                         model_prefix, info))
  return(file.path(GetModelsDir(),fn))
}

GetRepositoryDir <- function()
{
  return(get_data_dir(new_subfolder = "_best_models"))
}

CopyModelToRepository <- function(source_file, dest_file)
{
  logger(sprintf("Copying %s to %s", source_file,dest_file))
  src_fn <- file.path(GetModelsDir(),source_file)
  dst_fn <- file.path(GetRepositoryDir(), dest_file)
  file.copy(src_fn,dst_fn)
}

SaveBestModel <-function(model_file, test_recall, input_size, netlayout = "") 
{
  if(netlayout != "")
  {
    dest_fn <- sprintf("R_KERAS_REC_%.3f_INP_%d_NET_%s",
                       test_recall,input_size,netlayout)
  }
  else
  {
    dest_fn <- sprintf("R_KERAS_REC_%.3f_INP_%d",test_recall,input_size)
  }
  dest_fn <- gsub(",", "_", gsub("[\\. ]", "", dest_fn))  
  dest_fn <- paste0(dest_fn, ".hdf5")
  CopyModelToRepository(model_file,dest_fn)
  return(dest_fn)
}

LoadBestModel <- function(model_file)
{
  logger(sprintf("Loading best model %s", model_file))
  model_fn <- file.path(GetRepositoryDir(), model_file)
  return(load_model_hdf5(filepath = model_fn))
}

##
## end ChurnV2 lib
##


#####
#####   Must experimentations:
#####     
#####   1. DNN with min-max
#####   2. XGB with scale, min-max,
#####
#####
#####
#####
#####
#####
#####

#df_stats_2015 <- LoadCustomerStatInfo(2)
#df_stats_2016 <- LoadCustomerStatInfo(3)

#setup_paralel_env()

# 3 4 39 # for 2015-2016
# 2 3 22 # for 2016-2017

use_condaenv("r-tensorflow")

TRAN_PER <- 3
CHRN_PER <- 4
SGM_ID <- 39

df_full <- LoadChurnData(TRAN_PER, CHRN_PER, SGM_ID) #load_file("CHURN_2016.csv") #

ZERO_VAR_CHECK <- FALSE
STANDARD_PREDS <- TRUE

initial_fields <- colnames(df_full)



sgm_zero <- df_full$SGM_NO == 0
if(sum(sgm_zero)>0)
{
  logger(sprintf("Removing %d zero segment observations...",sum(sgm_zero)))
  df_full <- df_full[!sgm_zero,]
}


if(SHOW_PLOTS)
{
  df_gsk_sex <- df_full[,c(User.Field,"SEX", "GFK_SGM")]
}



if(ZERO_VAR_CHECK)
{
  #Check Columns Variability !!!! 
  logger("Testing for zero-variance...")
  
  #df_nzv <- nearZeroVar(df_full, names = TRUE, saveMetrics = TRUE, allowParallel = TRUE)
  #zero_var_cols <- row.names(df_nzv[df_nzv$zeroVar == TRUE,])
  
  zero_var_cols <- ZeroVar(df_full)
  logger(sprintf("Found %d cols with zero-var: [%s]",
                 length(zero_var_cols), toString(zero_var_cols)))
  #logger(sprintf("ZeroVarAnalysis:\n%s"GetObjectOuput(df_nzv)))
  
  logger("Dropping zero-var cols...")
  df_full <- df_full[, setdiff(colnames(df_full), zero_var_cols)]
}





Predictor.Fields <- GetPredictors(df_full)
Old.Predictors <- setdiff(Predictor.Fields, New.Fields)
Standard.Predictors <-setdiff(Old.Predictors, Overall.Fields)


if(!STANDARD_PREDS)
{
  logger("Transforming factor variables to dummies...")
  df_full <- dummy.data.frame(df_full, sep="__")
}
All.Predictors <- GetPredictors(df_full)

Predictor.Configs <- c(
  Standard.Predictors,
  Old.Predictors,
  All.Predictors
)

#
##
## the whole grid-search and cross-validation has been moved to 
## churn3.py 
##
##

#    ModelID CnfMatID TestF1 TestPrec TestRecall TestKappa TestAcc TrainAcc TrainKappa TrainRecall    Layout NPRED THRS DROP PREP
#14       14       14  0.528    0.389      0.821     0.324   0.674    0.672      0.322       0.820    98, 24    49 0.40    1    0
#9         9        9  0.529    0.392      0.811     0.327   0.679    0.677      0.326       0.811    92, 46    46 0.40    1    0
#18       18       18  0.529    0.479      0.591     0.376   0.766    0.765      0.374       0.590    92, 23    46 0.40    0    0
#83       11       83  0.529    0.459      0.626     0.367   0.753    0.751      0.367       0.628    23, 11    46 0.45    1    0
#85       13       85  0.533    0.400      0.797     0.336   0.689    0.687      0.334       0.796    98, 49    49 0.45    1    0
#11       11       11  0.536    0.423      0.730     0.354   0.719    0.717      0.352       0.730    23, 11    46 0.40    1    0
#87       15       87  0.536    0.412      0.769     0.347   0.704    0.702      0.345       0.769    24, 12    49 0.45    1    0
#81        9       81  0.537    0.437      0.697     0.363   0.733    0.732      0.362       0.698    92, 46    46 0.45    1    0
#157      13      157  0.538    0.451      0.667     0.371   0.745    0.744      0.370       0.668    98, 49    49 0.50    1    0
#86       14       86  0.540    0.432      0.720     0.363   0.727    0.726      0.362       0.720    98, 24    49 0.45    1    0

###
### Parameters
###


Predictor.Fields <- Standard.Predictors

PRE_PROCESSING <- 0 # 0 nothing, 1 minmax, 2 normaliz


Test.One.Segment = 0



###
###
###


if(STANDARD_PREDS) 
{
  # force standard preds no matter :)
  Predictor.Fields <- Standard.Predictors
}

if(Test.One.Segment>0)
{
  logger(sprintf("Testing ONLY segment %d", Test.One.Segment))
  df_full <- df_full[df_full$SGM_NO==Test.One.Segment,]
}

Nr.Predictors <- length(Predictor.Fields)
df_res<- data.frame()

logger(sprintf("Excluded variables: %s", toString(setdiff(initial_fields, Predictor.Fields))))
logger(sprintf("Final predictors: %s",toString(Predictor.Fields)))
X_full <- df_full[, Predictor.Fields]
y_full <- as.numeric(df_full[, Target.Field])


logger(sprintf("Preparing train/test data on %d obs (shape=[%s])...", nrow(df_full), toString(dim(df_full))))
train_part <- createDataPartition(as.numeric(df_full[, Target.Field]), p = 0.9, list = FALSE)

X_train_f <- X_full[train_part,]
X_test_f <- X_full[-train_part,]
y_train <- y_full[train_part]
y_test <- y_full[-train_part]




nr_preds = Nr.Predictors
s_results <- c()


if (PRE_PROCESSING == 2) {
  timeit("Preprocessing norm training...",
         pp_model <- preProcess(X_train_f, method = c("center", "scale")))
  timeit("Trasforming norm training data ...",
         X_train <- predict(pp_model, X_train_f))
  timeit("Trasforming norm testing data ...",
         X_test <- predict(pp_model, X_test_f))
  
  timeit("Trasforming full data ...",
         X_full <- predict(pp_model, X_full))
  
} else if (PRE_PROCESSING == 1) {
  timeit("Preprocessing MinMax training...",
         pp_model <- preProcess(X_train_f, method = c("range")))
  timeit("Trasforming MinMax training data ...",
         X_train <- predict(pp_model, X_train_f))
  timeit("Trasforming MinMax testing data ...",
         X_test <- predict(pp_model, X_test_f))
  timeit("Trasforming MinMax full data ...",
         X_full <- predict(pp_model, X_full)) 
} else if (PRE_PROCESSING == 0) {
  logger("NO PreProcessing!")
  X_train <- X_train_f
  X_test <- X_test_f
}

df_overall <- data.frame(Models = best_model_files)
df_res<- data.frame()

for(i in 1:length(best_model_files))
{
  best_model_file = best_model_files[i]
  # perform inference with best model
  nn_best_clf <- LoadBestModel(best_model_file)
  timeit(sprintf("Predicting on FULL data with model %s...",best_model_file),
         yhat_nn_full <- round(predict(nn_best_clf, as.matrix(X_full)), digits = 3)
  )
  p_nn_full <- as.numeric(yhat_nn_full >= Churn.Threshold)

  conf_mat_full <- confusionMatrix(p_nn_full, y_full, positive = "1")
  
  logger(sprintf("FULL [%s] Confusion Matrix:\n%s", best_model_file,GetObjectOuput(conf_mat_full)))
  df_overall[i,"Recall"] <- conf_mat_full$byClass["Recall"]
  df_overall[i,"Precision"] <- conf_mat_full$byClass["Precision"]
  df_overall[i,"Kappa"] <- conf_mat_full$overall["Kappa"]
  df_overall[i,"Accuracy"] <- conf_mat_full$overall["Accuracy"]
  

  if(RUN_THRESHOLD)
  {
    timeit("Predicting on testing data...",
           yhat_nn_test <- round(predict(nn_best_clf, as.matrix(X_test)), digits = 3)
    )

    logger("\nRunning churn threshold search ...")
    for(chrn in Churn.Threshold.List)
    {
      logger(sprintf("Computing predictions for ChurnThreshold: %.2f", chrn)) 
      p_nn_test <- as.numeric(yhat_nn_test >= chrn)
      p_nn_full <- as.numeric(yhat_nn_full >= chrn)
      
      
      nn_res <- GetPredStats(id_model = i, yhat = p_nn_test, y = y_test,
                             yhat_train = p_nn_full, y_train = y_full,
                             hiddens = "NA")
      nn_res["NPRED"] <- nr_preds
      nn_res["THRS"] <- chrn
      #nn_res["DROP"] <- c_dropout
      #nn_res["PREP"] <- PRE_PROCESSING
      nn_res["FILE"] <- best_model_file #basename(model_file)
      
      df_res <- rbind(df_res, nn_res)
    }
  } else{
    logger("Skip running threshold search ...")
  }
  
  
  
  if(SAVE_RESULTS)
  {
    
    #
    # Begin uploading results
    #
    # add new model to db
    Value.Recall <- conf_mat_full$byClass["Recall"]*100
    Value.Precision <- conf_mat_full$byClass["Precision"]*100
    Value.ModelName <- sprintf("Churn Prediction (DNN %s) with Recall %.2f%%",
                               MODULE.VERSION,
                               Value.Recall)
    Value.ModelDesc <- sprintf("Chrn %d/%d; M: %s",
                               TRAN_PER,CHRN_PER,GetNNInfo(nn_best_clf))
    
    
    model_id <- AddChurnModel(model_name = Value.ModelName,
                              model_desc = Value.ModelDesc,
                              model_tper = TRAN_PER, 
                              model_cper = CHRN_PER,
                              model_file = best_model_file,
                              model_sgm = SGM_ID,
                              model_score1 = Value.Recall,
                              model_score2 = Value.Precision)
    logger(sprintf("Churn Model %d initialized.",model_id))
    
    #
    # output dataset preparation and uploading
    #
    df_output <- data.frame(MODEL_ID = df_full[,User.Field])
    colnames(df_output) <- c(User.Field)
    max_proba <- max(yhat_nn_full)
    offset <- 0 # (0.99 - max_proba)
    df_output$MODEL_ID <- model_id
    df_output$PRED <- as.integer(p_nn_full)
    df_output$PROB <- as.numeric((yhat_nn_full + offset) * p_nn_full)
    df_output <- df_output[,c("MODEL_ID","CUST_ID","PRED","PROB")]
    UploadToSQL(Table.Results, df_output)
    #
    # DONE UPLOADING RESULTS
    #
  # end IF SAVE_RESULTS 
  } else
  {
    logger("Skip save results ...")
  }
}# end for each model

if(RUN_THRESHOLD)
{
  df_res_sorted <- df_res[order(df_res$TrainRecall),]
  logger(sprintf("Results dataframe from churn threshold search:\n%s", GetObjectOuput(df_res_sorted)))  
}

logger(
        sprintf("Overall results for ChurnThreshold: %.2f:\n%s", Churn.Threshold, GetObjectOuput(df_overall))
      )

rap_year <- 2015+TRAN_PER-2

if(SHOW_PLOTS)
{
  df_full$PREDICTED <- p_nn_full
  
  df_churned <- df_full[df_full[,Target.Field]==1,]
  df_pred <- df_full[df_full$PREDICTED==1,]
  
  df_TP <- df_full[df_full$PREDICTED==1 & df_full[,Target.Field]==1,]
  
  df_FN <- df_full[df_full$PREDICTED==0 & df_full[,Target.Field]==1,]
  
  df_FP <- df_full[df_full$PREDICTED==1 & df_full[,Target.Field]==0,]
  
  ca_TP <- sum(df_TP$M)
  ca_FN <- sum(df_FN$M)
  ca_GT <- sum(df_churned$M)
  
  mg_TP <- sum(df_TP$MARGIN)
  mg_FN <- sum(df_FN$MARGIN)
  mg_GT <- sum(df_churned$MARGIN)
  
  df_stats <- data.frame(LAB=c("CORECTE","FALS NEG","REAL"),CA=c(ca_TP,ca_FN,ca_GT), PROFIT=c(mg_TP,mg_FN,mg_GT))  
  
  
  PlotBarChart(df_full, "SGM_NO", plot_title = sprintf("Fig.01 - Distributia pe segmente\n pentru anul%d",rap_year))
  
  PlotBarChart(df_gsk_sex, "SEX",plot_title = sprintf("Fig.02 - Distributia pe sexe\n pentru anul%d",rap_year))
  
  PlotBarChart(df_gsk_sex, "GFK_SGM", plot_title = sprintf("Fig.03 - Distributia pe segmentele GFK\n pentru anul%d",rap_year))
  
  PlotHistChart(df_full, "AGE", plot_title = sprintf("Fig.04 - Distributia pe varste\n pentru anul%d",rap_year), show_numbers = F)
  
  PlotValuesChart(GetSummaryData(df_full,by_col = "SGM_NO", sum_col = "M"), 
                  col = "SGM_NO", sum_col = "M_SUM",
                  plot_title = sprintf("Fig.05 - Cifra totala per segment\n pentru anul%d",rap_year))
  
  PlotValuesChart(GetSummaryData(df_full,by_col = "SGM_NO", sum_col = "MARGIN"), 
                  col = "SGM_NO", sum_col = "MARGIN_SUM",
                  plot_title = sprintf("Fig.06 - Marja totala per segment\n pentru anul%d",rap_year))
  
  
  
  PlotBarChart(df_churned, "SGM_NO", 
               plot_title = sprintf("Fig.07.1 - Distributia pe segmente a churn-ului\n  pentru anul %d (identificat in %d)",rap_year,rap_year+1),
               show_prc = FALSE)  
  PlotBarChart(df_pred, "SGM_NO", 
               plot_title = sprintf("Fig.07.2 - Distributia pe segmente a churn-ului prezis\n  pentru anul%d",rap_year),
               show_prc = FALSE)  
  PlotBarChart(df_TP, "SGM_NO", 
               plot_title = sprintf("Fig.07.3 - Distributia pe segmente a churn-ului prezis corect\n  pentru anul%d",rap_year),
               show_prc = FALSE)  
  
  
  
  PlotBarChart(df_churned, "SGM_NO", 
               plot_title = sprintf("Fig.08.1 - Distributia procentuala pe segmente a churn-ului\n  pentru anul %d (identificat in %d)",rap_year,rap_year+1))
  PlotBarChart(df_pred, "SGM_NO", 
               plot_title = sprintf("Fig.08.2 - Distributia procentuala pe segmente a churn-ului prezis\n  pentru anul%d",rap_year))
  PlotBarChart(df_TP, "SGM_NO", 
               plot_title = sprintf("Fig.08.3 - Distributia procentuala pe segmente a churn-ului prezis corect\n  pentru anul%d",rap_year))
  
  
  PlotValuesChart(GetSummaryData(df_churned,by_col = "SGM_NO", sum_col = "M"), 
                  col = "SGM_NO", sum_col = "M_SUM",
                  plot_title = sprintf("Fig.09.1 - Evaluarea veniturilor pierdute din churn \n per segment in %d (replicare venituri %d)",rap_year+1,rap_year))
  PlotValuesChart(GetSummaryData(df_pred,by_col = "SGM_NO", sum_col = "M"), 
                  col = "SGM_NO", sum_col = "M_SUM",
                  plot_title = sprintf("Fig.09.2 - Evaluarea veniturilor pierdute din churn prezis \n per segment in %d (replicare venituri %d)",rap_year+1,rap_year))
  PlotValuesChart(GetSummaryData(df_TP,by_col = "SGM_NO", sum_col = "M"), 
                  col = "SGM_NO", sum_col = "M_SUM",
                  plot_title = sprintf("Fig.09.3 - Evaluarea veniturilor pierdute din churn \n prezis corect per segment in %d (replicare venituri %d)",rap_year+1,rap_year))
  
  
  
  PlotValuesChart(GetSummaryData(df_churned,by_col = "SGM_NO", sum_col = "MARGIN"), 
                  col = "SGM_NO", sum_col = "MARGIN_SUM",
                  plot_title = sprintf("Fig.10.1 - Evaluarea marjei totala pierdute din churn \n per segment in %d (replicare venituri %d)",rap_year+1,rap_year))
  PlotValuesChart(GetSummaryData(df_pred,by_col = "SGM_NO", sum_col = "MARGIN"), 
                  col = "SGM_NO", sum_col = "MARGIN_SUM",
                  plot_title = sprintf("Fig.10.2 - Evaluarea marjei totala pierdute din churn prezis \n per segment in %d (replicare venituri %d)",rap_year+1,rap_year))
  PlotValuesChart(GetSummaryData(df_TP,by_col = "SGM_NO", sum_col = "MARGIN"), 
                  col = "SGM_NO", sum_col = "MARGIN_SUM",
                  plot_title = sprintf("Fig.10.3 - Evaluarea marjei totala aferente adevarat pozitivelor \n corect verificate per segment in %d (replicare venituri %d)",rap_year+1,rap_year))
  
  
  plot_title =  sprintf("Fig.11 - Matricea confuziei churn %d (iesit %d)",rap_year,rap_year+1)
  PlotChurnConfusion(conf_mat_full, plot_title = plot_title)
  
  
  
  PlotValuesChart(df_stats,col="LAB",sum_col="CA",
                  plot_title = sprintf("Fig.12 - Cifra totala de afaceri (estimata) a clientilor churn \n prezisa / ratata / reala"))
  PlotValuesChart(df_stats,col="LAB",sum_col="PROFIT",
                  plot_title = sprintf("Fig.13 - Marja totala (estimata) a clientilor churn \n prezisa / ratata / reala"))
  
  PlotBarChart(df_FN, "SGM_NO", 
               plot_title = sprintf("Fig.14.1 - Distributia pe segmente a falselor negative\n  pentru anul %d (identificat in %d)",rap_year,rap_year+1),
               show_prc = FALSE)  
  
  PlotValuesChart(GetSummaryData(df_FN,by_col = "SGM_NO", sum_col = "M"), 
                  col = "SGM_NO", sum_col = "M_SUM",
                  plot_title = sprintf("Fig.14.2 - Evaluarea veniturilor aferente falselor negative \n per segment in %d (replicare venituri %d)",rap_year+1,rap_year))
  
  PlotValuesChart(GetSummaryData(df_FN,by_col = "SGM_NO", sum_col = "MARGIN"), 
                  col = "SGM_NO", sum_col = "MARGIN_SUM",
                  plot_title = sprintf("Fig.14.3 - Evaluarea profitului aferent falselor negative \n per segment in %d (replicare venituri %d)",rap_year+1,rap_year))
  
  
  
}



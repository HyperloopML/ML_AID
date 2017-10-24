##
##
##
## @script:       Churn Prediction System
## @v1_created:   2017.03.23
## @v2_created:   2017.06.19
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

LIBS <- c("e1071", "reshape2", "reshape2",
  "xgboost", "caret", "MASS", "plyr", "pROC", "Rtsne", "keras", "ggplot2")




User.Field <- "CUST_ID"



# use predefined repository folder or just "" for automatic current folder
USE_REPOSITORY <- "d:/GoogleDrive/_hyperloop_data/churn_v2"
USED_PROCESSING_POWER <- 0.85

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
pwd <- "pwd=hyp2017!!"
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

Short.Predictor.Fields <- c("R_1", "M_1", "F_1",
                            "R_2", "M_2", "F_2",
                            "R_3", "M_3", "F_3",
                            "R_4", "M_4", "F_4"
                            )


Churn.Threshold <- 0.4



conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)



MODULE.VERSION <- "1.2.1.10"
MODULE.NAME <- "CHURN_V2"
MODULE.NAME.SHORT <- "CHURN2"





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
  file_db_path <- get_data_dir()
  if (FULL_DEBUG)
    logger(sprintf(" Used data directory: %s\n", file_db_path))
  FN <- paste0(MODULE.NAME, "_v", gsub("\\.", "_", MODULE.VERSION), "_",
                 log_ctime, "_data.csv")
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

normalize <- function(x) {
  mx <- max(x)
  mn <- min(x)
  if (mx == 0 && mn == 0) mx <- 1
  return((x - mn) / (mx - mn))
}

save_plot <- function(sfn) {

    file_db_path <- get_data_dir()
    stime <- format(Sys.time(), "%Y%m%d%H%M%S")
    FN <- file.path(file_db_path, paste0(stime, "_", sfn, "_PLOT.png"))
    logger(sprintf(" Saving plot: %s\n", FN))
    dev.print(device = png, file = FN, width = 1024, height = 768)

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
  stime <- format(Sys.time(), "%Y%m%d%H%M%S")
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

LoadChurnData <- function(iTransPer, iChurnPer) {
  logger(sprintf("Loading period %d churn data (trans=%d, churn=%d)...",
                 iTransPer, iTransPer, iChurnPer))
  sSQL <- sprintf(SQL.GETCHURN, iTransPer, iChurnPer)
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

GetNNInfo <- function(model)
{
  str_res <- ""
  for(l in 0:length(model$layers)) 
  {
    pre <- "->"
    layer <- get_layer(model, index = l)
    sname <- toString(layer)
    if(l==0) {
      pre <- ""
      sunits <- layer$input_shape[[2]]
    }else if(sname=="Dropout")
    {
      sunits <- toString(layer$rate)  
    } else
      {
        sunits <- toString(layer$units) 
      }
    sshort <- substr(sname,1,4)
    str_res <- paste0(str_res,pre,sshort,"[",sunits,"]")
  }
  return(str_res)
}

ChurnClassifier <- function(hidden_layers = c(128), nr_inputs, dropout_level = 1) {
  logger(sprintf("Preparing NN (input:%s layers:[%s])",
                 toString(nr_inputs), toString(hidden_layers)))
  nr_hiddens = length(hidden_layers)
  model <- keras_model_sequential()
  h1_units = hidden_layers[1]
  
  layer_dense(model,
              units = hidden_layers[1],
              activation = "elu",
              input_shape = c(nr_inputs))
  
  if(nr_hiddens>1)
  {
    for (nr_hidden in 2:nr_hiddens) 
      {
        if(dropout_level>=1)
        {
          layer_dropout(model, rate = 0.5)
        }
        layer_dense(model,
                    units = hidden_layers[nr_hidden],
                    activation = "elu")
      }
  }
  
  if(dropout_level>=2)
  {
    layer_dropout(model, rate = 0.5)
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

GetPredStats <- function(id_model, yhat, y, yhat_train = NULL, y_train = NULL, hiddens = NULL) {
  logger(sprintf(" Computing confusion matrix for %d observations", length(y)))
  if(FULL_DEBUG)
    logger(sprintf("   yhat_test:[%s] y_test:[%s]", toString(yhat[1:5]), toString(y[1:5])))
  cf_test <- confusionMatrix(yhat, y, positive = "1")
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

setup_paralel_env()

DEBUG_ONLY_FINAL_STAGE <- FALSE

df_full <- LoadChurnData(2, 3) #load_file("CHURN_2016.csv") #


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

Predictor.Fields <- GetPredictors(df_full)
Nr.Predictors <- length(Predictor.Fields)

if (DEBUG_ONLY_FINAL_STAGE) {
  PredictorSelector <- list(
                          setdiff(Predictor.Fields, Overall.Fields)
                          )
} else {
  PredictorSelector <- list(
                          setdiff(Predictor.Fields, Overall.Fields)
                          , Predictor.Fields
                           #,Short.Predictor.Fields
                          )
}


logger(sprintf("Preparing train/test data on %d obs...", nrow(X_full)))
set.seed(12345)
train_part <- createDataPartition(as.numeric(df_full[, Target.Field]), p = 0.85, list = FALSE)


PRE_PROCESSING <- 0 # 0 nothing, 1 minmax, 2 normaliz

if(DEBUG_ONLY_FINAL_STAGE) 
{
  #PRE_PROCESSING <- 0
  logger("\nFAST TRACK DEBUGGING FOR FINAL STAGE !\n")
}


logger(sprintf(" Train: %d, test %d", nrow(X_train), nrow(X_test)))


# XGB RESULTS:
#  max_depth = 9, eta = 0.15, gamma = 0.2, colsample_bytree = 0.6, 
#  min_child_weight = 5, subsample = 0.9 on full training set
v_max_depth = 9 #5
v_eta = 015 #0.1
v_gamma = 0.2 #0.5
v_colsample_bytree = 0.6 #0.9
v_min_child_weight = 5 #1
v_nrounds = 200
v_subsample = 0.9 #0.8

dropout_values <- c(2,1,0)

IS_QUICK = FALSE

if(DEBUG_ONLY_FINAL_STAGE) IS_QUICK <- TRUE

if (DEBUG && (Current.Machine.Name == HPC.Machine.Name)) {
  max_tests = length(PredictorSelector)
  nr_epochs = 10
  PREPROCESSING_RANGE <- 0:2
} else {
  max_tests = 1
  PREPROCESSING_RANGE <- 0
  if (!IS_QUICK)
    nr_epochs = 5 else nr_epochs = 1
  }

s_results <- c()

RUN_NN <- TRUE

if (RUN_NN) {

  nn_start_time <- Sys.time()


  logger(sprintf("Running a total of %d predictor selection tests for NN",
                 max_tests))

  dnn_res_train <- list()
  dnn_res_test <- list()
  dnn_res_preds <- list()
  dnn_res_layouts <- list()
  dnn_res_models <- list()
  dnn_res_fieldslist <- list()
  dnn_res_dropout <- list()
  dnn_res_preproc <- list()

  Selection.Fields <- Predictor.Fields
  curr_iter <- 0


  for(PRE_PROCESSING in PREPROCESSING_RANGE) ## preprocessing testing
  {
    logger(sprintf("\nPREPROCESSING STEP ..."))
    
    X_full <- df_full[, Predictor.Fields]
    y_full <- as.numeric(df_full[, Target.Field])
    X_train_f <- X_full[train_part,]
    X_test_f <- X_full[-train_part,]
    y_train <- y_full[train_part]
    y_test <- y_full[-train_part]

    if (PRE_PROCESSING == 2) {
      timeit("Preprocessing norm training...",
             pp_model <- preProcess(X_train_f, method = c("center", "scale")))
      timeit("Trasforming norm training data ...",
             X_train_f <- predict(pp_model, X_train_f))
      timeit("Trasforming norm testing data ...",
             X_test_f <- predict(pp_model, X_test_f))
      
      timeit("Trasforming full data ...",
             X_full <- predict(pp_model, X_full))
      
    } else if (PRE_PROCESSING == 1) {
      timeit("Preprocessing MinMax training...",
             pp_model <- preProcess(X_train_f, method = c("range")))
      timeit("Trasforming MinMax training data ...",
             X_train_f <- predict(pp_model, X_train_f))
      timeit("Trasforming MinMax testing data ...",
             X_test_f <- predict(pp_model, X_test_f))
      timeit("Trasforming MinMax full data ...",
             X_full <- predict(pp_model, X_full)) 
    } else if (PRE_PROCESSING == 0) {
      logger("NO PreProcessing!")
    }

    for(c_dropout in dropout_values) ## dropout testing
    { 
      for (i in 1:max_tests) { ## predictor testing
        Selection.Fields <- PredictorSelector[[i]]
        nr_preds = length(Selection.Fields)
        X_train <- X_train_f[, Selection.Fields]
        X_test <- X_test_f[, Selection.Fields]
        
        ##
        ##
        ##
        nr_hid <- ncol(X_train)
        logger(sprintf("\nPreparing and training DNN on %d preds, dropout:%d, prep:%d",
                       ncol(X_train), c_dropout, PRE_PROCESSING))
        
        nn_tests <- Get_DNN_Layouts(nr_hid = nr_hid, QUICK = IS_QUICK)
        nr_nn_tests <- length(nn_tests)
        
        logger(sprintf("Running %d tests...", length(nn_tests)))
        nn_t = 0
        for (nn_test_layers in nn_tests) {
          nn_t <- nn_t + 1
          curr_iter <- curr_iter + 1 
          logger(sprintf("\nTrain %d/%d pred_iter %d/%d  [test %d/%d]",
                         nn_t, nr_nn_tests, i, max_tests, curr_iter, 
                         max_tests * nr_nn_tests * length(dropout_values)*length(PREPROCESSING_RANGE)))
          nr_preds <- ncol(X_train)
          nn_clf <- ChurnClassifier(nn_test_layers, nr_preds, dropout_level = c_dropout)
          
          timeit(sprintf("Fitting (%s) for %d epochs...", GetNNInfo(nn_clf), nr_epochs),
                 nn_clf %>% fit(x = as.matrix(X_train), y = y_train,
                                batch_size = 512, epochs = nr_epochs, verbose = 1,
                                validation_data = list(as.matrix(X_test), y_test),
                                callbacks = list(callback_early_stopping(patience = 1,verbose = 1))
                 )
          )
          
          timeit("Predicting on training data...",
                 yhat_nn_train <- round(predict(nn_clf, as.matrix(X_train)), digits = 3)
          )
          
          timeit("Predicting on testing data...",
                 yhat_nn_test <- round(predict(nn_clf, as.matrix(X_test)), digits = 3)
          )
          c_item <- length(dnn_res_train) + 1
          dnn_res_train[[c_item]] <- yhat_nn_train
          dnn_res_test[[c_item]] <- yhat_nn_test
          dnn_res_layouts[[c_item]] <- nn_test_layers
          dnn_res_preds[[c_item]] <- ncol(X_train)
          dnn_res_models[[c_item]] <- nn_clf
          dnn_res_fieldslist[[c_item]] <- Selection.Fields
          dnn_res_dropout[[c_item]] <- c_dropout
          dnn_res_preproc[[c_item]] <- PRE_PROCESSING
        } # end layout testing
      } # end predictor list testing
    }# end dropout search
  } ### end preprocessing grid
  
  
  
  # end all NN tests




  rm(yhat_nn_test)
  rm(yhat_nn_train)
  rm(nn_test_layers)

  ResetConfusionStats()
  df_res <- data.frame()
  
  Churn.Threshold.List <- c(0.40, 0.45, 0.5)


  best_train_recall <- 0
  best_train_conf <- NULL
  best_test_recall <- 0
  best_test_conf <- NULL
  best_model <- -1

  nr_iter <- 0
  for (Churn.Threshold in Churn.Threshold.List) {
    logger(sprintf("Using Churn.Threshold of %1.2f%%", Churn.Threshold * 100))
    for (i in 1:length(dnn_res_test)) {
      nr_iter <- nr_iter + 1
      logger(sprintf("\nComputing confusion matrices %d/%d (cnf:%d)", nr_iter, 
                     length(Churn.Threshold.List)*length(dnn_res_test),i_conf+1))
      yh_train <- dnn_res_train[[i]]
      yh_test <- dnn_res_test[[i]]
      cnn_layers <- dnn_res_layouts[[i]]
      cnr_preds <- dnn_res_preds[[i]]
      if (FULL_DEBUG) {
        logger("Preparing ROC for training ...")
        nn_train_roc <- roc(predictor = as.vector(yh_train),
                            response = as.vector(y_train))
        logger("Preparing ROC for testing...")
        nn_test_roc <- roc(predictor = as.vector(yh_test),
                            response = as.vector(y_test))
        plot(nn_test_roc, main = sprintf("ROC NN layout: [%s]", toString(cnn_layers)))
        plot(hist)
      }

      p_nn_test <- as.numeric(yh_test > Churn.Threshold)
      p_nn_train <- as.numeric(yh_train > Churn.Threshold)

      nn_res <- GetPredStats(id_model = i, yhat = p_nn_test, y = y_test,
                             yhat_train = p_nn_train, y_train = y_train,
                             hiddens = cnn_layers)
      nn_res["NPRED"] <- cnr_preds
      nn_res["THRS"] <- Churn.Threshold
      nn_res["DROP"] <- dnn_res_dropout[[i]]
      nn_res["PREP"] <- dnn_res_preproc[[i]]

      df_res <- rbind(df_res, nn_res)

      s <-sprintf("TestRec: %.3f TestKap: %.3f TrainRec: %.3f TrainKap: %.3f on NN ([%s], %.1f thr)",
                   nn_res["TestRecall"], nn_res["TestKappa"],
                   nn_res["TrainRecall"], nn_res["TrainKappa"],
                   toString(cnn_layers), Churn.Threshold)

      if (best_train_recall < nn_res["TrainRecall"]) {
        best_train_recall <- nn_res["TrainRecall"]
        best_train_kappa <- nn_res["TrainKappa"]
        best_train_layers <- cnn_layers
      }
      
      if (best_test_recall < nn_res["TestRecall"]) {
        best_test_recall <- nn_res["TestRecall"]
        best_test_kappa <- nn_res["TestKappa"]
        best_test_layers <- cnn_layers
        best_test_conf <- i_conf
        best_model <- i
        BestFieldsSelection <- dnn_res_fieldslist[[i]]
      }


      logger(s)
      s_results = c(s_results, s)
    } # end grid search of models
  } # end grid search of thresholds
  rownames(df_res) <- NULL
  

  nn_end_time <- Sys.time()
  nn_time <- difftime(nn_end_time, nn_start_time, units = "sec")

  logger(sprintf("DNN processing time: %.1f min", nn_time / 60))

  logger("Summing results")
  for (s in s_results) {
    logger(s)
  }

  #display best train and test - show best test confusion matrix

  logger(sprintf("Best train Recall: %.3f Kappa: %.3f for %s",
                 best_train_recall, best_train_kappa, toString(best_train_layers)))
  logger(sprintf("Best test  Recall: %.3f Kappa: %.3f for %s",
                 best_test_recall, best_test_kappa, toString(best_test_layers)))

  logger(sprintf("NN Results:\n%s", GetObjectOuput(tail(df_res[order(df_res$TestF1),],50))))
  logger(sprintf("Best NN Confusion Matrix:\n%s", GetObjectOuput(conf_mats[[best_test_conf]])))
  rm(nn_res)
  ##
  ##
  ##
} else {
  logger("Bypassing NN procedure")
}


Churn.Threshold <- 0.4
CHECK_PREDS <- FALSE

if (Current.Machine.Name == HPC.Machine.Name && CHECK_PREDS) {
  logger("Running grid-search of predictors with XGB...")
  Selection.Fields <- Predictor.Fields
  for (i in 1:max_tests) {
    Selection.Fields <- PredictorSelector[[i]]
    nr_preds = length(Selection.Fields)
    X_train <- X_full[train_part, Selection.Fields]
    X_test <- X_full[-train_part, Selection.Fields]
    timeit(sprintf("\nTraining XGB Rnds:%d Md:%d Eta:%.2f Gamma:%.3f Cols:%.1f MinCh:%d",
                   v_nrounds, v_max_depth, v_eta, v_gamma, v_colsample_bytree,
                   v_min_child_weight),
           xgb0 <- xgboost(data = as.matrix(X_train),
                           label = y_train,
                           missing = 0,
                           verbose = 0,
                           nround = v_nrounds,
                           objective = "binary:logistic",
                           max_depth = v_max_depth,
                           eta = v_eta,
                           gamma = v_gamma,
                           colsample_bytree = v_colsample_bytree,
                           min_child_weight = v_min_child_weight,
                           subsample = v_subsample
    #,nthread = 30
                           ))

    yhat_train <- round(predict(xgb0, as.matrix(X_train)), digits = 3)
    p_train <- as.numeric(yhat_train > Churn.Threshold)
    train_roc <- roc(predictor = yhat_train, response = y_train)

    yhat_test <- round(predict(xgb0, as.matrix(X_test)), digits = 3)
    p_test <- as.numeric(yhat_test > Churn.Threshold)
    test_roc <- roc(predictor = yhat_test, response = y_test)

    xg_res <- GetPredStats(id_model = 100, yhat = p_test, y = y_test,
                           yhat_train = p_train, y_train = y_train,
                           hiddens = "XGB1")
    xg_res["NPRED"] <- nr_preds
    xg_res["THRS"] <- Churn.Threshold
    xg_res["DROP"] <- -1
    xg_res["PREP"] <- -1
    
    df_res <- rbind(df_res, xg_res)


    s<-sprintf("%d preds: TestRec: %.3f TestKap: %.3f TrainRec: %.3f TrainKap: %.3f on XGB (%.1f thr)",
                 nr_preds,
                 xg_res["TestRecall"], xg_res["TestKappa"],
                 xg_res["TrainRecall"], xg_res["TrainKappa"],
                 Churn.Threshold)
    logger(s)
    rm(xg_res)

    plot(train_roc, main = sprintf("XGB Training ROC th:%.1f p:%d",
                                   Churn.Threshold, nr_preds))
    plot(test_roc, main = sprintf("XGB Testing ROC th:%.1f p:%d",
                                  Churn.Threshold, nr_preds))
  }
} else {
  logger("Bypassing grid-search of predictors.")
}


RUN_GRID_SEARCH <- FALSE

if (Current.Machine.Name == HPC.Machine.Name && RUN_GRID_SEARCH) {
  Selection.Fields <- PredictorSelector[[1]]
  nr_preds = length(Selection.Fields)
  X_train <- X_full[train_part, Selection.Fields]
  X_test <- X_full[-train_part, Selection.Fields]
  Churn.Threshold <- 0.4

  logger(sprintf("\nRunning grid-search cv on machine [%s]: fields: %d",
                 Current.Machine.Name, nr_preds))

  #Train and Tune the SVM
  RUN_SVM = FALSE
  if (RUN_SVM) {
    logger(sprintf("Running SVM grid..."))
    svm_trCtrl <- trainControl(method = "repeatedcv", # 10fold cross validation
                               repeats = 5, # do 5 repetitions of cv
                               summaryFunction = twoClassSummary, # Use AUC to pick the best model
                               classProbs = TRUE)
    timeit(" Tuning SVM ...",
           svm_tuner <- train(x = X_full,
                              y = as.factor(make.names(y_full)),
                              method = "svmRadial", # Radial kernel
                              tuneLength = 9, # 9 values of the cost function
                              preProc = c("center", "scale"), # Center and scale data
                              metric = "ROC",
                              trControl = svm_trCtrl)
    )

    final_svm <- sv_tuner$finalModel

    timeit("Test predict with SVM",
           yhat_svm_test <- predict(final_svm, X_test))
    timeit("Train predict with SVM",
           yhat_svm_train <= predict(final_svm, X_train))

    sv_res <- GetPredStats(id_model = 200, yhat = yhat_svm_test, y = y_test,
                           yhat_train = yhat_svm_train, y_train = y_train,
                           hiddens = "SVM1")
    sv_res["NPRED"] <- nr_preds
    sv_res["THRS"] <- Churn.Threshold
    sv_res["DROP"] <- -1
    sv_res["PREP"] <- -1
    
    df_res <- rbind(df_res, sv_res)

    s <- sprintf("TestRec: %.3f TestKap: %.3f TrainRec: %.3f TrainKap: %.3f on XGB (%.1f thr)",
                 sv_res["TestRecall"], sv_res["TestKappa"],
                 sv_res["TrainRecall"], sv_res["TrainKappa"],
                 Churn.Threshold)
    logger(s)
    rm(sv_res)
  }

  #Train and Tune the XGB
  xgb.grid <- expand.grid(
        nrounds = c(25),
        max_depth = c(3, 6, 9),
        eta = c(0.15), #, 0.2, 0.3, 0.5),
        gamma = c(0.0, 0.2), # 1, 2, 4),
        colsample_bytree = c(0.6, 0.9),
        min_child_weight = c(1, 2, 5),
        subsample = c(0.6, 0.9))

  xgb_trCtrl <- trainControl("cv", #'adaptive_cv', 
                             number = 5,
  #repeats = 3,
                             summaryFunction = twoClassSummary,
                             classProbs = TRUE,
                             verboseIter = TRUE,
                             allowParallel = TRUE)
  timeit(" Tuning XGB ...",
         xgb_tuner <- train(x = X_full,
                            y = as.factor(make.names(y_full)),
                            method = "xgbTree",
                            tuneGrid = xgb.grid,
                            trControl = xgb_trCtrl,
                            metric = "kappa"))
  plot(xgb_tuner, main = "Train results for XGB")

  logger(sprintf("XGB tuning results:\n %s", toString(xgb_tuner$bestTune)))
  xgb1 <- xgb_tuner$finalModel
  cv_max_depth = xgb_tuner$bestTune[1, 2]
  cv_eta = xgb_tuner$bestTune[1, 3]
  cv_gamma = xgb_tuner$bestTune[1, 4]
  cv_colsample_bytree = xgb_tuner$bestTune[1, 5]
  cv_min_child_weight = xgb_tuner$bestTune[1, 6]
  cv_subsample = xgb_tuner$bestTune[1, 7]
  cv_nrounds = 500

  timeit(sprintf(" Directly training TUNED XGB for %d rounds", cv_nrounds),
         xgb2 <- xgboost(data = as.matrix(X_full),
                         label = y_full,
                         missing = 0,
  #verbose = 0,
                         objective = "binary:logistic",
                         nround = cv_nrounds,
                         max_depth = cv_max_depth,
                         eta = cv_eta,
                         gamma = cv_gamma,
                         colsample_bytree = cv_colsample_bytree,
                         min_child_weight = cv_min_child_weight,
                         subsample = cv_subsample))

  yhat_xgb1_train <- predict(xgb1, as.matrix(X_train))
  x1_train_pred <- as.numeric(yhat_xgb1_train > Churn.Threshold)

  yhat_xgb2_train <- predict(xgb2, as.matrix(X_train))
  x2_train_pred <- as.numeric(yhat_xgb2_train > Churn.Threshold)

  yhat_xgb1_test <- predict(xgb1, as.matrix(X_test))
  x1_test_pred <- as.numeric(yhat_xgb1_test > Churn.Threshold)

  yhat_xgb2_test <- predict(xgb2, as.matrix(X_test))
  x2_test_pred <- as.numeric(yhat_xgb2_test > Churn.Threshold)

  x1_res <- GetPredStats(id_model = 500, yhat = x1_test_pred, y = y_test,
                         yhat_train = x1_train_pred, y_train = y_train,
                         hiddens = "XGB_C")
  x1_res["NPRED"] <- nr_preds
  x1_res["THRS"] <- Churn.Threshold
  x1_res["DROP"] <- -1
  x1_res["PREP"] <- -1
  
  df_res <- rbind(df_res, x1_res)

  s1 <- sprintf("TestRec: %.3f TestKap: %.3f TrainRec: %.3f TrainKap: %.3f on XGB (%.1f thr)",
               x1_res["TestRecall"], x1_res["TestKappa"],
               x1_res["TrainRecall"], x1_res["TrainKappa"],
               Churn.Threshold)
  rm(x1_res)

  x2_res <- GetPredStats(id_model = 500, yhat = x2_test_pred, y = y_test,
                         yhat_train = x2_train_pred, y_train = y_train,
                         hiddens = "XGB_D")
  x2_res["NPRED"] <- nr_preds
  x2_res["THRS"] <- Churn.Threshold
  x2_res["DROP"] <- -1
  x2_res["PREP"] <- -1
  
  df_res <- rbind(df_res, x2_res)

  s2 <- sprintf("TestRec: %.3f TestKap: %.3f TrainRec: %.3f TrainKap: %.3f on XGB (%.1f thr)",
               x2_res["TestRecall"], x2_res["TestKappa"],
               x2_res["TrainRecall"], x2_res["TrainKappa"],
               Churn.Threshold)
  rm(x2_res)


  logger(sprintf("Caret : %s", s1))
  logger(sprintf("Direct: %s", s2))
} else {
  logger("Bypassing grid-search of classifiers.")
}

logger("Script done.")



# Kmeans results with curve

# kmeans results fixed

# plot results kmeans

# plot population kmeans

# select top 25% micros

# generate RRecom2 
SHOW_PIPELINE <- TRUE

SHOW_CLUSTERING <-TRUE

if (SHOW_PIPELINE) {
  logger(sprintf("\nLoading final model with layout [%s]...", toString(best_test_layers)))
  final_model <- dnn_res_models[[best_model]]
  logger(sprintf("Computing full predictions on data with shape (%s)...",
                  toString(dim(df_full[, BestFieldsSelection]))))
  yhat_full <- predict(final_model, as.matrix(df_full[, BestFieldsSelection]))
  Churn.Threshold <- 0.4

  pred_full <- as.integer(yhat_full >= Churn.Threshold)
  nr_churn <- sum(pred_full)
  logger(sprintf("Total %d churned out of %d customers at %.1f threshold",
                 nr_churn, nrow(df_full), Churn.Threshold))
  logger(sprintf("Computing confusion matrix for threshold %.2f ...", Churn.Threshold))
  nnfull_res <- GetPredStats(id_model = 1000, yhat = pred_full, y = y_full,
                             hiddens = paste0("nn_full:", toString(best_test_layers)))
  nnfull_res["NPRED"] <- length(BestFieldsSelection)
  nnfull_res["THRS"] <- Churn.Threshold
  nnfull_res["DROP"] <- -1
  nnfull_res["PREP"] <- -1
  

  df_res <- rbind(df_res, nnfull_res)

  s <- sprintf("FullRec: %.3f FullKap: %.3f on NN ([%s], %.1f thr)",
                   nnfull_res["TestRecall"], nnfull_res["TestKappa"],
                   toString(best_test_layers), Churn.Threshold)
  logger(sprintf("Final results:\n%s", GetObjectOuput(df_res)))
  logger(sprintf("Confusion Matrix:\n%s", GetObjectOuput(conf_mats[[i_conf]])))

  df_full["PRED_CHURN"] <- pred_full

  cluster_fields <- c("R", "F", "M")

  results <- FullClustering(df_full, cluster_fields,
                            n_centers = 4, n_micros = 100,
                            log_list = c(0, 1, 1))
  
  

  df_cl <- results$dataframe
  normed_cols <- results$norm_columns

  df_micros <- unique(df_cl[, results$fields])
  
  x_axis = results$f1
  y_axis = results$f2
  x_label = "Recency"
  y_label = "Frequency"

  # plot micros
  all_segments_plot <- PlotClustering(df_micros,
                          field1 = x_axis, field2 = y_axis,
                          seg_field = results$label,
                          x_lab = x_label, y_lab = y_label)
  all_segments_plot
  SaveLastPlot("full_clusters")


  df_churn <- df_cl[df_cl["PRED_CHURN"] == 1,]

  err_list <- TestClustering(df_churn[, normed_cols], normed_cols)
  
  test_seg_plot <- qplot(x = 2:10, y = err_list)
  test_seg_plot <- test_seg_plot + geom_line() + ylab("Cluster Error (wsos)") +xlab("Nr Segments")
  test_seg_plot
  SaveLastPlot("clustering_test")
  


  # now pass ONLY precalculated/scaled columns dataframe (initial one only contains the clustering)
  churn_res <- SimpleClustering(df_churn[,normed_cols], normed_cols, n_centers = 4, n_micros = 25)

  df_churn_cl <- churn_res$dataframe
  df_churn_micros <- unique(df_churn_cl[, churn_res$fields])

  # plot churn micros
  x_axis_churn = results$f1
  y_axis_churn = results$f2
  x_label_churn = "Recency"
  y_label_churn = "Frequency"
  

  churn_segments_plot <- PlotClustering(df_churn_micros,
                          field1 =x_axis_churn, field2 = y_axis_churn,
                          seg_field = churn_res$label,
                          x_lab = x_label_churn, y_lab = y_label_churn)
  churn_segments_plot
  SaveLastPlot("churn_segments")
  
  TWO_LAYER_FIELD = "OVERALL"
  INITIAL_LABEL = "ALL"
  lbl_field <- churn_res$label
  df_micros[TWO_LAYER_FIELD] = INITIAL_LABEL
  df_churn_micros[TWO_LAYER_FIELD] = df_churn_micros[lbl_field]
  all_levels <- unique(df_micros[lbl_field])
  
  df_twolayer = rbind(df_micros,df_churn_micros)
  
  color_names <- c(INITIAL_LABEL)
  
  for(lvl in all_levels[[1]]) color_names <- c(color_names, lvl)
  
  special_colors <- c("gray","green","yellow","blue","red")
  names(special_colors) <- color_names
  
  
  full_plot <- PlotClustering(df_twolayer,
                              field1 =x_axis_churn, field2 = y_axis_churn,
                              seg_field = churn_res$label, overall_field = TWO_LAYER_FIELD,
                              x_lab = x_label_churn, y_lab = y_label_churn,
                              manual_colors = special_colors)
  
  full_plot
  SaveLastPlot("churn_vs_all")
  
  # analize all churned 

  bar_churn_plot <- qplot(x=df_churn[,lbl_field], geom="bar", 
                          fill = as.factor(df_churn[,lbl_field]))
  bar_churn_plot <- bar_churn_plot + scale_fill_discrete(name  ="Predicted churn for each original segment")
  bar_churn_plot <- bar_churn_plot + geom_text(stat='count',aes(label=..count..),vjust=-1)
  bar_churn_plot
  SaveLastPlot("churn_bars")
  
  df_orig_churn <- df_cl[df_cl$CHURN==1,]
  bar_orig_churn_plot <- qplot(x=df_orig_churn[,lbl_field], geom="bar", 
                               fill = as.factor(df_orig_churn[,lbl_field]))
  bar_orig_churn_plot <- bar_orig_churn_plot + scale_fill_discrete(name  ="Training reality")
  bar_orig_churn_plot <- bar_orig_churn_plot +  geom_text(stat='count',aes(label=..count..),vjust=-1)
  bar_orig_churn_plot
  SaveLastPlot("churn_orig")
  
  bar_churnsize_plot <- qplot(x=df_churn_cl[,lbl_field], geom="bar", 
                          fill = as.factor(df_churn_cl[,lbl_field]))
  bar_churnsize_plot <- bar_churnsize_plot + scale_fill_discrete(name  ="Size of churned segments")
  bar_churnsize_plot <- bar_churnsize_plot +  geom_text(stat='count',aes(label=..count..),vjust=-1)
  bar_churnsize_plot
  SaveLastPlot("size_churned")

  # select top 25% micros

  # generate RRecom2 
}
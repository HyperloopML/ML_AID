##
##
##
## @script:Full Behaviour exploration and inference
## @created:      2017.03.16
## @lastmodified: 2017.04.20
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, Power BI
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##
MODULE.VERSION <- "1.0.2"
MODULE.NAME <- "CLUSTERING"

ALL_CATEGS <- c("BATISTE", "STREPSILS", "PHARMA", "COSMETICE",
                "DERMOCOSMETICE", "BABY", "NEASOCIATE")
ALL_BRANDS <- c("BrandGeneral", "DR_HART", "NUROFEN", "APIVITA", "AVENE", "L_OCCITANE", "VICHY",
                "BIODERMA", "LA_ROCHE_POSAY", "L_ERBOLARIO", "PARASINUS", "TRUSSA", "SORTIS", "NESTLE",
                "OXYANCE", "TERTENSIF", "ASPENTER", "ALPHA")
ALL_COLUMNS <- c(ALL_BRANDS, ALL_CATEGS)

NR_ROWS = 2e6 # 5e4 or 2e5 or 2e6 - IGNORED IF RUN ON SERVER

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

n_centers <- 4
n_downsample <- 100
log_list <- c(0, 1, 1)
column_list <- c("R", "F", "M") #ALL_COLUMNS #c("Recency", "DailyFrequency", "Revenue")
segment_labels <- c("1-Very Low", "2-Low", "3-Average", "4-Good", "5-Best")
cust_column <- "PartnerId"
log_all_columns <- FALSE #set this to FALSE if log_list is used (then empty log_list means no logs)
NO_LOG_SAVE <- FALSE

###
### END INPUT PARAMS
###


# use predefined repository folder or just "" for automatic current folder
USE_REPOSITORY <- "c:/_hyperloop_data/clustering"
USED_PROCESSING_POWER <- 0.65
##
## HELPER FUNCTIONS
##
##


HELPER_LIB_VER <- "1.4.1"


FULL_DEBUG <- FALSE

options(digits = 3)

get_formula <- function(lbl, vars) {
    slabel <- paste0(lbl, "~")
    sfactors <- paste(vars, collapse = "+")
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
        cat(sprintf("[Script %s]\n", get_scriptpath()))
        s_data_dir <- dirname(get_scriptpath())
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
    if (!NO_LOG_SAVE) {
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
    }

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
    Data.FileName <- gsub("=", "_", Data.FileName)
    Data.FileName <- gsub("_+", "_", Data.FileName)

    Data.FileName <- paste0(Data.FileName, ".xdf")

    if (use_sql)
        str_sql <- str_table else
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
    if (mx == 0 && mn == 0) mx <- 1
    return((x - mn) / (mx - mn))
}

##
## END HELPER FUNCTIONS
##



centroid_labels <- c()
subcluster_column_list <- c()

ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
logger(sprintf("[%s] BEGIN Clustering/Exploration script [%s] v%s SQL_CONNECT=%d\n",
                MODULE.NAME, MODULE.VERSION, ctime, SQL_CONNECT))

SHOW_FULL_PLOT = FALSE
SHOW_DOWN_PLOT = TRUE
SHOW_HEAT_MAPP = TRUE

USE_REVOSCALER = FALSE # FALSE if using standard kmeans/etc


svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=StatisticalStore;"
uid <- "uid=andreidi;"
pwd <- "pwd=HypML2017"
conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)
sqls <- "select  PartnerId, R, F, M from [vwPartnerPropertyValue] where PeriodId=3"


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

if (SQL_CONNECT != 0) {
    df <- load_df(sqls, use_sql = TRUE)
}

##
## END DATA INPUT
##

debug_object_size(df)
logger(sprintf("Total '-1' customers: %d\n", sum(df[, cust_column] == -1)))
df <- df[df[, cust_column] != -1,]

for (i in 1:length(column_list)) {
    col <- column_list[i]
    new_col <- paste0("cen", i, "_", substr(col, 1, 3))
    sub_new_col <- paste0("sub", i, "_", substr(col, 1, 3))
    centroid_labels <- c(centroid_labels, new_col)
    subcluster_column_list <- c(subcluster_column_list, sub_new_col)
}
###
### end input parameter preparation
###

## preprocessing
PRE_VER <- "1.1"

logger(paste0("Begin preprocessing v", PRE_VER, " ...\n"))
t0_prep <- proc.time()

norm_columns <- c()
nr_input_fields <- length(column_list)


for (i in 1:nr_input_fields) {
    col_name <- column_list[i]
    new_col <- paste0("P", i, "_", substr(col_name, 1, 3))
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
    max_c <- max(df[, new_col]) + 1e-1 # add "epsilon" for numerical safety
    df[, new_col] <- (df[, new_col] - min_c) / max_c
}

## add 20170405 - truncate input column fields
strs <- format(round(as.numeric(object.size(df) / (1024 * 1024)), 1), nsmall = 1, big.mark = ",")
logger(sprintf("Dataset size before cleaning: %sMB\n", strs))

all_new_fields <- setdiff(names(df),column_list)
df <- df[,all_new_fields]

strs <- format(round(as.numeric(object.size(df) / (1024 * 1024)), 1), nsmall = 1, big.mark = ",")
logger(sprintf("Dataset size AFTER cleaning: %sMB\n", strs))
## end add 20170405

t1_prep <- proc.time()
prep_time <- (t1_prep[3] - t0_prep[3]) / 60

logger(sprintf("Done preprocessing. Total time %.2f min\n", prep_time))
## end preprocessing


# base clusterization model
clust_column <- "IDCLU" # IMPORTANT: segment ID (sorted from worst to best)
label_column <- "LABEL" # IMPORTANT: inferred label for ID
score_column <- "SCORE" # IMPORTANT: score of each segment
loss_column <-  "LOSSC" # loss of the model
subcl_column <- "SCLID" # IMPORTANT: ID of the subcluster (sorted from worst to best)
nrcus_column <- "NR_CL" # number of observation in main segment
tempc_column <- "TMPCL" # temporary segment - should be removed at cleanup
temp_scl_col <- "TMPSC" # temporary subcluster - should be removed at cleanup
subscore_col <- "SUBSC" # IMPORTANT: subcluster score
tsnex_column <- "TSNEX" # IMPORTANT: t-SNE X attribute
tsney_column <- "TSNEY" # IMPORTANT: t-SNE Y attribute
##
## additionally the following columns are generated:
##  cenX_YYY: center parameter X for YYY attribute (main segment centroid)
##  subX_YYY:  - IMPORTANT - subcenter parameter X for YYY attribute (subcluster centroid
##

pivot_columns <- c(label_column, centroid_labels)
acols <- c(clust_column, norm_columns, subcl_column, column_list,
    centroid_labels, subcluster_column_list)

logger("Main clustering...\n")
t0 <- proc.time()

timeit("Main kmeans: ",
        trained_model <- CustomKMeans(
            x = df[, norm_columns],
            centers = n_centers, nstart = 30))
df[, tempc_column] <- trained_model$cluster

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
timeit("Merge",df <- merge(df, df_centers, by = tempc_column))



clusterList <- unique(df[, clust_column])
n_cl <- length(clusterList)

for (i in 1:n_cl) {
    cl <- clusterList[i]
    df_centers[df_centers[, clust_column] == cl, nrcus_column] <- nrow(
        df[df[, clust_column] == cl,])
}
df_downsampled <- data.frame()
# generate sub-clusters for each cluster
for (i in 1:n_cl) {
    cl <- clusterList[i]
    logger(sprintf("Subclustering cluster %d...\n", cl))
    timeit("Subcluster kmeans: ",
           cl_model <- CustomKMeans(
               df[df[, clust_column] == cl, norm_columns],
               centers = n_downsample, nstart = 10))


    df_subcenters <- cl_model$center
    scores <- rowMeans(df_subcenters)
    df_subcenters <- data.frame(df_subcenters, scores)
    colnames(df_subcenters) <- c(subcluster_column_list, subscore_col)

    df_subcenters[, clust_column] <- cl
    df_subcenters[, temp_scl_col] <- 1:nrow(df_subcenters)
    df_subcenters <- df_subcenters[order(df_subcenters[, subscore_col]),]
    df_subcenters[, subcl_column] <- 1:nrow(df_subcenters)

    df[df[, clust_column] == cl, temp_scl_col] <- cl_model$cluster

    df_centers[df_centers[, clust_column] == cl, nrcus_column] <- nrow(
                                            df[df[, clust_column] == cl,])

    df_downsampled <- rbind(df_downsampled, df_subcenters)
}

library(Rtsne)
logger("Applying t-SNE...\n")
timeit("t-SNE ",
        rtsne_res <- Rtsne(as.matrix(df_downsampled[, subcluster_column_list]),
                           check_duplicates = FALSE,
                           pca = TRUE))
df_downsampled[, tsnex_column] <- rtsne_res$Y[,1]
df_downsampled[, tsney_column] <- rtsne_res$Y[,2]

timeit("Final Merge ",
    df <- merge(df, df_downsampled, by = c(clust_column, temp_scl_col)))


dfa <- data.frame(df_centers[, c(label_column, clust_column, score_column,
                                    nrcus_column, centroid_labels)])

t1 <- proc.time()
elapsed <- t1[3] - t0[3]
logger(sprintf("Total processing time %.2f min\n",elapsed/60))

strs <- format(round(as.numeric(object.size(df) / (1024 * 1024)), 1), nsmall = 1, big.mark = ",")
logger(sprintf("Final dataset size: %sMB\n", strs))

ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
logger(sprintf("[%s] END Clustering script SQL_CONNECT=%d\n", ctime, SQL_CONNECT))

###
### END SQL SCRIPT
### @outputs: 
###     df:  full dataset including downsampled subclustering information
###     dfa: segment inferred information
###     all_log: varchar data with full log information
###



##
## Power BI client side R script for visualizations 
##
## plot clusters using R Server ggplot2 (CRAN or Microsoft Open R)
##
library(ggplot2)

clust_column <- clust_column
label_column <- label_column
score_column <- score_column
subcl_column <- subcl_column
nrcus_column <- nrcus_column

column_list <- column_list

calculated_column_list <- norm_columns
subcluster_column_list <- subcluster_column_list

x_dim <- subcluster_column_list[1]
y_dim <- subcluster_column_list[2]
z_dim <- subcluster_column_list[3]
x_dim_lbl <- column_list[1]
y_dim_lbl <- column_list[2]
z_dim_lbl <- column_list[3]
c_dim <- label_column
n_dim <- clust_column
s_dim <- subcl_column


library(rgl)
library(car)
col_df_prep <- c(column_list, subcluster_column_list,
                 clust_column, label_column, subcl_column, score_column,
                 nrcus_column)

use_dplyr = FALSE
if (use_dplyr) {
    library(dplyr)
    #group_columns = c(clust_column,label_column,subcl_column)
    #dots <- lapply(group_columns, as.symbol)
    #agg_df <- group_by_(.data = df, .dots = dots)
    #mean_columns <- c(calculated_column_list,subcluster_column_list)
    #dots2 <- lapply(mean_columns, as.symbol)
    #down_df <- data.frame(summarise_(agg_df, .dots = dots2))
    #down_df <- data.frame(summarise(agg_df, 
    #                                X=mean(df[,x_dim]), 
    #                                Y=mean(df[,y_dim])))

    agg_df <- group_by(
                       df,
                       df[, clust_column],
                       df[, label_column],
                       df[, subcl_column]
                       )

    down_df <- data.frame(summary(agg_df, X = mean(df[, x_dim]), Y = ))

    down_df[, c_dim] <- factor(down_df[, clust_column])
    down_df[, subcl_column] <- factor(down_df[, subcl_column])

} else {
    down_df <- aggregate(x = df,
                         by = list(ID  = df[, label_column],
                                   SID = df[, subcl_column],
                                   NID = df[, clust_column]),
                         FUN = mean)
    down_df$ID <- as.factor(down_df$ID)
    down_df$NID <- as.factor(down_df$NID)
    c_dim = "ID"
    s_dim = "SID"
}

dataset <- down_df


#dataset <- distinct(df, df[,x_dim],df[,y_dim],df[,c_dim], df[s_dim])
### BEGIN PBI
#plot1 <- ggplot(dataset, aes_string(x=x_dim,y=y_dim,color=c_dim))
#plot1 <- plot1 + geom_point(aes_string(color=c_dim,  
#                                       shape = c_dim, 
#                                       size=5, 
#                                       alpha=0.5))

cl_labels <- sort(unique(dataset[, c_dim]))
plot1 <- qplot(dataset[, x_dim], dataset[, y_dim],
               shape = dataset$ID, #dataset[,c_dim], 
               color = dataset$ID, #dataset[,c_dim], 
               size = I(5.0),
               alpha = 0.5)
plot1 <- plot1 + labs(x = x_dim, y = y_dim)
#plot1 <- plot1 + scale_shape_discrete(name  ="Segments",
#                                      labels=dataset[,c_dim])
#plot1 <- plot1 + scale_colour_discrete(name  ="Segments",labels=cl_labels)
plot1 <- plot1 + geom_text(aes_string(label = dataset$SID #dataset[,s_dim]
                                      ),
                           color = "black", size = 3)

plot1

### END PBI

##
## R Client-side 3D plot example
##
NR_3D_PLOT = 1
if (NR_3D_PLOT == 1) {
    plot3d <- scatter3d(
            down_df[,x_dim], down_df[,y_dim], down_df[,z_dim],
            xlab = x_dim_lbl,
            ylab = y_dim_lbl,
            zlab = z_dim_lbl,
            groups = down_df[,c_dim],
            surface = FALSE, grid = FALSE, ellipsoid = TRUE,
            sphere.size = 2)
} else if (NR_3D_PLOT == 2) {
    plot3d <- scatter3d(
            down_df[, x_dim], down_df[, y_dim], down_df[, z_dim],
            xlab = x_dim,
            ylab = y_dim,
            zlab = z_dim,
            point.col = down_df$NID,
            surface = FALSE,
            labels = down_df$SID,
            id.n = nrow(down_df),
            sphere.size = 2)
}

### end 3D plot

### BEGIN HEAT PLOT

if (SHOW_HEAT_MAPP) {
    library(reshape)

    piv_df <- melt(df_centers[, pivot_columns], id = label_column)

    heat_plot <- ggplot(piv_df, aes_string(label_column, "variable"))
    heat_plot <- heat_plot + geom_tile(aes(fill = c(value)),
                                       colour = "white")
    heat_plot <- heat_plot + scale_fill_gradient(low = "white",
                                                 high = "steelblue")
    heat_plot <- heat_plot + labs(x = "Segments", y="Variables")


    heat_plot
}

### END HEAT PLOT

### Begin GENERATIVE CLUSTERING
SHOW_TSNE_KMPL = TRUE
SHOW_TSNE_FULL = FALSE

t0_tsne <- proc.time()

timeit("Labeling down_df ", down_df <- GetMaxes(down_df, "BESTOF", ALL_BRANDS, ALL_CATEGS))

if (SHOW_TSNE_KMPL) {
    STD_TSNE <- FALSE
    if (STD_TSNE) {
        library(tsne)
        tsne_res <- tsne(as.matrix(down_df[, column_list]))
        tsne_kmeans <- kmeans(tsne_res, centers = 4, nstart = 100)
        tsne_colors <- as.factor(tsne_kmeans$cluster)
        down_df$VisualClustering <- tsne_kmeans$cluster
        plot2 <- qplot(tsne_res[, 1], tsne_res[, 2],
                       shape = down_df$AssignmentID,
                       color = tsne_colors)
        plot2 <- plot2 + theme(legend.position = "none")
        plot2

    } else {
        library(Rtsne)
        rtsne_res <- Rtsne(as.matrix(down_df[, subcluster_column_list]),
                       check_duplicates = FALSE,
                       pca = FALSE)
        tsne_kmeans <- kmeans(rtsne_res$Y, centers = 4, nstart = 100)
        tsne_colors <- as.factor(tsne_kmeans$cluster)
        down_df$VisualClustering <- tsne_kmeans$cluster
        
  
        
        plot2 <- qplot(rtsne_res$Y[, 1], rtsne_res$Y[, 2])
        
        plot2 <- plot2 + geom_point(aes(shape = down_df$ID, color = down_df$ID), 
                                    size = 4)

        plot2 <- plot2 + geom_point(aes(alpha = 0.5))

        plot2 <- plot2 + geom_text(aes(label = down_df$BESTOF),
                                   color = "black", size = 2)
        plot2 <- plot2 + theme(legend.position = "none")
        plot2
    }
}




if (SHOW_TSNE_FULL) {
    rtsne_res <- Rtsne(as.matrix(df[, column_list]),
                       check_duplicates = FALSE)
    plot3 <- qplot(rtsne_res$Y[, 1], rtsne_res$Y[, 2],
                   shape = df$cluster, color = df$cluster)
    plot3 <- plot2 + theme(legend.position = "none")
    plot3
}

t1_tsne <- proc.time()
t_tsne <- t1_tsne[3] - t0_tsne[3]
logger(sprintf("TSNE executed in %.2f sec",t_tsne))

### END Generative clustering

# end script

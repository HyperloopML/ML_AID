##
##
##
## @script:       PLOT Full Behaviour exploration and inference
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

MODULE.VERSION <- "0.0.2"
MODULE.NAME <- "PLOT"

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


NR_ROWS = 2e6 # 5e4 or 2e5 or 2e6 - IGNORED IF RUN ON SERVER

USE_REVOSCALER = FALSE # FALSE if using standard kmeans/etc


svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=StatisticalStore;"
uid <- "uid=andreidi;"
pwd <- "pwd=HypML2017"
conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)
sqls <- "SELECT [ClusterId],[R],[M] FROM [dbo].[z2016rfm4]"



# use predefined repository folder or just "" for automatic current folder
USE_REPOSITORY <- "" #"c:/_hyperloop_data/clustering"
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
        cat(sprintf("[Script %s]\n",get_scriptpath()))
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
    Data.FileName <- gsub("_+","_", Data.FileName)
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

ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
logger(sprintf("[%s] BEGIN PLOT Clustering/Exploration script SQL_CONNECT=%d\n", ctime, SQL_CONNECT))


df <- load_df(sqls, use_sql = TRUE)

df <- df[sample(nrow(df), nrow(df) * 0.03),]
logger(sprintf("Plotting %d rows",nrow(df)))

library(ggplot2)
df$ClusterId <- as.factor(df$ClusterId)
plt <- qplot(x = df$R, y= df$M, 
                shape = df$ClusterId,
                color = df$ClusterId,
                size = I(5.0),
                alpha = 0.5)



###
### END SQL SCRIPT
### @outputs:
###     df:  full dataset including downsampled subclustering information
###     dfa: segment inferred information
###     all_log: varchar data with full log information
###
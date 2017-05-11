##
##
##
## @script:       Full Behaviour exploration and inference
## @created:      2017.03.16
## @lastmodified: 2017.03.17
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, Power BI
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##
 
###
###
### BEGIN SQL Server R code
###
### OBS: set SQL_CONNECT=0 for MS SQL Server Server side R scripts
###
SQL_CONNECT = 0
# 0: Run from SLQ Server;
# 1: use RODBC to remotely connect to sql server
# 2: use RevoScaleR to remotely connect to sql server (Microsoft R ONLY!)
# 3: use RevoScaleR to load from cache local file
# 4: csv - just load simple csv file
 
all_log <- " "
logger <- function(text) {
    all_log <<- paste0(all_log,text)
    cat(text)
}
 
###
### @INPUT parameter preparation
###
###n_centers <- 4
###n_downsample <- 100
###log_list <- c(0, 1, 1)
###custid_column <-"PartnerId"
###column_list <- c("R","F","M")
n_centers <- @n_centers
n_downsample <- @n_downsample
log_list <- c(@log_list)
custid_column <-"@custid_column"
column_list <- c(@column_list)
log_all_columns <- TRUE #set this to FALSE if log_list is used (then empty log_list means no logs)
###
### END INPUT PARAMS
###
 
centroid_labels <- c()
subcluster_column_list <- c()
 
ctime <- format(Sys.time(),"%Y-%m-%d %H:%M:%S")
logger(sprintf("[%s] BEGIN Clustering/Exploration script SQL_CONNECT=%d\n",ctime , SQL_CONNECT))
 
NR_ROWS = 5e4 # 5e4 or 2e5 or 2e6 - IGNORED IF RUN ON SERVER
 
SHOW_FULL_PLOT = FALSE
SHOW_DOWN_PLOT = TRUE
SHOW_HEAT_MAPP = TRUE
SHOW_TSNE_KMPL = TRUE
SHOW_TSNE_FULL = FALSE
 
USE_REVOSCALER = FALSE # FALSE if using standard kmeans/etc
 
 
svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=StatisticalStore;"
uid <- "uid=andreidi;"
pwd <- "pwd=HypML2017"
conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                        svr, db, uid, pwd)
sqls <- "SELECT * FROM _BEV_2015v1"
 
timeit = function(strmsg, expr) {
    tm <- system.time(expr)
    stm <- sprintf("%.2f min",tm[3]/60)
    logger(paste0(strmsg," executed in ",stm,"\n"))
}
 
 
if ("RevoScaleR" %in% rownames(installed.packages())) {
    library(RevoScaleR)
    # Create an xdf file name
    dfXdfFileName <- file.path(getwd(), "hyperloop_saved_data.xdf")
}
 
CustomKMeans = function(x, centers, nstart) {
    kparams <- sprintf(" centers: %d rows: %d", centers, nrow(x))
    if (USE_REVOSCALER) {
        factors <- colnames(x)
        kformula = as.formula(paste("~", paste(factors, collapse = "+")))
        sformula = toString(kformula)
        logger(paste0("Using RevoScaleR rxKmeans ", kparams,": [",sformula,"] "))
        model <- rxKmeans(formula = kformula,
                          data = x,
                          numClusters = centers,
                          numStarts = nstart,
                          reportProgress = 0)
   } else {
        logger(paste0("Using standard kMeans ",kparams,": "))
        model <- kmeans(x = x,
                        nstart = nstart,
                        centers = centers
                        )
    }
    return(model)
}
 
 
if (SQL_CONNECT == 1) {
    logger("Connecting to SQL Server...")
    library(RODBC)
    channel <- odbcDriverConnect(conns)
    logger("Downloading data...")
 
    timeit("sqlQuery", dfi <- sqlQuery(channel, paste(sqls)))
 
    odbcClose(channel)
    logger("Done data downloading.")
} else if (SQL_CONNECT == 4)
{
 
    mypath <- dirname(sys.frame(1)$ofile)
    dfi <- read.csv(paste0(mypath, "/test.csv"))
} else if ((SQL_CONNECT == 2) || (SQL_CONNECT == 3))
{
    if (SQL_CONNECT == 2) {
        logger("Microsoft RevoScaleR connect to SQL Server...")
        timeit("RxOdbcData ",
            dsOdbcSource <- RxOdbcData(sqlQuery = sqls,
                                       connectionString = conns))
 
        # Import the data into the xdf file
        timeit("rxImport ",
            rxImport(dsOdbcSource, dfXdfFileName, overwrite = TRUE))
    }
    # Read xdf file into a data frame
    logger("RevoScaleR loading dataframe...")
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
logger(sprintf("Done data downloading. Dataset size: %sMB\n", strs))
strs <- format(
            round(as.numeric(nrow(df)), 1),
            nsmall = 0, big.mark = ",",
            scientific = FALSE)
logger(sprintf("Loaded %s rows in memory dataframe\n", strs))
 
df <- df[df[,custid_column] != -1,]
 
 
for (i in 1:length(column_list)) {
    col <- column_list[i]
    new_col <- paste0("cen", i,"_", substr(col, 1, 3))
    sub_new_col <- paste0("sub", i,"_", substr(col, 1, 3))
    centroid_labels <- c(centroid_labels, new_col)
    subcluster_column_list <- c(subcluster_column_list, sub_new_col)
}
segment_labels <- c("1-Very Low", "2-Low", "3-Average", "4-Good", "5-Best")
###
### end input parameter preparation
###
 
## preprocessing
 
logger("Begin preprocessing ...\n")
t0_prep <- proc.time()
 
norm_columns <- c()
nr_input_fields <- length(column_list)
 
 
for (i in 1:nr_input_fields) {
    col_name <- column_list[i]
    new_col <- paste0("P", i, "_", substr(col_name, 1, 3))
    norm_columns <- c(norm_columns, new_col)
    is_log = 0
    if (length(log_list) >= i)
        is_log <- log_list[i]
    if ((is_log == 1) || (log_all_columns)) {
        EXP_BELOW_ZERO <- FALSE
        if (EXP_BELOW_ZERO) {
            df[df[, col_name] <= 0, col_name] <-
                exp(df[df[, col_name] <= 0, col_name]) * 1e-3
        } else {
            fdelta <- min(df[df[, col_name] <= 0, col_name])
            df[df[, col_name] <= 0, col_name] <-
                (df[df[, col_name] <= 0, col_name] - fdelta + 1) * 1e-5
       
        }
        df[, new_col] <- log(df[, col_name])
    } else {
        df[, new_col] <- df[, col_name]
    }
 
    df[, new_col] <- df[, new_col] - min(df[, new_col])
    max_c <- max(df[, new_col]) + 1e-1 # add "epsilon" for numerical safety
    df[, new_col] <- df[, new_col] / max_c
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
prep_time <- (t1_prep[3] - t0_prep[3]) /60
 
logger(sprintf("Done preprocessing. Total time %.2f min\n", prep_time))
## end preprocessing
 
 
# base clusterization model
clust_column <- "IDCLU" # segment ID (sorted from worst to best)
label_column <- "LABEL" # inferred label for ID
score_column <- "SCORE" # score of each segment
loss_column <-  "LOSSC" # loss of the model
subcl_column <- "SCLID" # ID of the subcluster (sorted from worst to best)
nrcus_column <- "NR_CL" # number of observation in main segment
tempc_column <- "TMPCL" # temporary segment - should be removed at cleanup
temp_scl_col <- "TMPSC" # temporary subcluster - should be removed at cleanup
subscore_col <- "SUBSC" # subcluster score
tsnex_column <- "TSNEX" # t-SNE X attribute
tsney_column <- "TSNEY" # t-SNE Y attribute
 
pivot_columns <- c(label_column, centroid_labels)
acols <- c(clust_column, norm_columns, subcl_column, column_list,
    centroid_labels, subcluster_column_list)
 
logger("Main clustring...")
t0 <- proc.time()
 
timeit("Main kmeans",
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
 
logger("Merging...")
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
    logger(sprintf("Subclustering cluster %d...", cl))
    timeit("Subcluster kmeans",
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
logger("Applying t-SNE... ")
timeit("t-SNE ",
        rtsne_res <- Rtsne(as.matrix(df_downsampled[, subcluster_column_list]),
                           check_duplicates = FALSE,
                           pca = FALSE))
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
 
print(head(df))
col_names <- colnames(df)
 
###
### END SQL SCRIPT
### @outputs:
###     df:  full dataset including downsampled subclustering information
###     dfa: segment inferred information
###     all_log: varchar data with full log information
###
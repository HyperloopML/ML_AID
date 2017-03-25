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
MODULE.VERSION <- "1.0.1"

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
    stm <- sprintf("%.2f min", tm[3] / 60)
    logger(paste0(strmsg, " executed in ", stm, "\n"))
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
                                            typeof(obj),
                                            strs1,
                                            strs2))
}


##
## END HELPER FUNCTIONS
##

###
### @INPUT parameter preparation
###
n_centers <- 4
n_downsample <- 100
log_list <- c() #c(0, 1, 1)
column_list <- ALL_COLUMNS #c("Recency", "DailyFrequency", "Revenue")
segment_labels <- c("1-Very Low", "2-Low", "3-Average", "4-Good", "5-Best")
cust_column <- "PartnerId"
log_all_columns <- TRUE #set this to FALSE if log_list is used (then empty log_list means no logs)
###
### END INPUT PARAMS
###

centroid_labels <- c()
subcluster_column_list <- c()

ctime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
logger(sprintf("[%s] BEGIN Clustering/Exploration script SQL_CONNECT=%d\n", ctime, SQL_CONNECT))

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
sqls <- "SELECT * FROM _BEV_2015v1"



if ("RevoScaleR" %in% rownames(installed.packages())) {
    library(RevoScaleR)
    # Create an xdf file name
    dfXdfFileName <- file.path(getwd(), "hyperloop_saved_data.xdf")
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
    logger("Connecting to SQL Server...")
    library(RODBC)
    channel <- odbcDriverConnect(conns)
    logger("Downloading data...")

    timeit("sqlQuery", dfi <- sqlQuery(channel, paste(sqls)))

    odbcClose(channel)
    logger("Done data downloading.")
} else if (SQL_CONNECT == 4) {

    mypath <- dirname(sys.frame(1)$ofile)
    dfi <- read.csv(paste0(mypath, "/test.csv"))
} else if ((SQL_CONNECT == 2) || (SQL_CONNECT == 3)) {
    if ((SQL_CONNECT == 2) || (!file.exists(dfXdfFileName))) {
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

df <- df[df[,cust_column] != -1,]

for (i in 1:length(column_list)) {
    col <- column_list[i]
    new_col <- paste0("cen", i,"_", substr(col, 1, 3))
    sub_new_col <- paste0("sub", i,"_", substr(col, 1, 3))
    centroid_labels <- c(centroid_labels, new_col)
    subcluster_column_list <- c(subcluster_column_list, sub_new_col)
}
###
### end input parameter preparation
###

## preprocessing
PRE_VER <- "1.1"

logger(paste0("Begin preprocessing v",PRE_VER," ...\n"))
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
        plot2 <- qplot(rtsne_res$Y[, 1], rtsne_res$Y[, 2],
                       shape = down_df$ID,
                       color = tsne_colors)
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

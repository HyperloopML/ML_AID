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

get_scriptpath <- function() {
  # location of script can depend on how it was invoked:
  # source() and knit() put it in sys.calls()
  path <- NULL
  
  if(!is.null(sys.calls())) {
    # get name of script - hope this is consisitent!
    path <- as.character(sys.call(1))[2] 
    # make sure we got a file that ends in .R, .Rmd or .Rnw
    if (grepl("..+\\.[R|Rmd|Rnw]", path, perl=TRUE, ignore.case = TRUE) )  {
      return(path)
    } else { 
      message("Obtained value for path does not end with .R, .Rmd or .Rnw: ", path)
    }
  } else{
    # Rscript and R -f put it in commandArgs
    args <- commandArgs(trailingOnly = FALSE)
  }
  return(path)
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
sqls <- "select * from vwkMeansMicroCluster where ModelId=11"



if ("RevoScaleR" %in% rownames(installed.packages())) {
  library(RevoScaleR)
  # Create an xdf file name
  file_db_path <- dirname(get_scriptpath()) #getwd()
  logger(sprintf("Current workind directory: %s\n",file_db_path))
  dfXdfFileName <- file.path(file_db_path, "hyperloop_saved_data.xdf")
  logger(sprintf("XDF File: %s\n",dfXdfFileName))
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




df$Label <- as.factor(df$Label)
down_df <- df

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


x_dim <- "R"
y_dim <- "F"
z_dim <- "M"
x_dim_lbl <- "Recency"
y_dim_lbl <- "Frequency"
z_dim_lbl <- "Monetary"
c_dim <- "Label"
n_dim <- "ClusterId"
s_dim <- "MicroClusterId"

if (!("rgl" %in% installed.packages()))
  install.packages("rgl")
if(!("car" %in% installed.packages()))
  install.packages("car")

library(rgl)
library(car)
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


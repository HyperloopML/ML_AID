##
##
##
## @script:       Full clustering script including client-side visualizaion
## @created:      2017.02.16
## @lastmodified: 2017.03.15
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, Microsoft SQL Server, Microsoft Power BI
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##

SQL_CONNECT = TRUE

SHOW_FULL_PLOT = FALSE
SHOW_DOWN_PLOT = TRUE
SHOW_HEAT_MAPP = TRUE
SHOW_TSNE_KMPL = TRUE
SHOW_TSNE_FULL = FALSE


##
## SQL Server connection section (client-side only!)
##
if(SQL_CONNECT)
{
  print("Connecting to SQL Server...")
  svr <- "server=VSQL08\\HYPERLOOP;"
  db <- "database=StatisticalStore;"
  uid <- "uid=andreidi;"
  pwd <- "pwd=HypML2017"
  conns <- paste0("driver={ODBC Driver 13 for SQL Server};",svr,db,uid,pwd)
  sqls <-"SELECT * FROM _BEV_2015"
  library(RODBC)
  channel <- odbcDriverConnect(conns)
  print("Downloading data...")
  dfi <- sqlQuery(channel,paste(sqls))
  dfi[is.na(dfi)] <- 0
  odbcClose(channel)  
  df <- data.frame(dfi)
  df <- df[df$PartnerId != -1,]
  print("Done data downloading.")
}else
{

  mypath <- dirname(sys.frame(1)$ofile)
  exploreData <- read.csv(paste0(mypath,"/test.csv"))
  finalData <- data.frame(exploreData)
  df <- finalData
}

##
## END DATA INPUT
##

### 
### BEGIN SQL Server R code
### 
###
#### @INPUT parameter preparation
n_centers <- 5
n_downsample <- 100
log_list <- c(0,1,1)
column_list <- c("Recency","DailyFrequency","Revenue")
centroid_labels <- c("c_R","c_F","c_M")
subcluster_column_list <- c("sub_R","sub_F","sub_M")
segment_labels <- c("1-Very Low","2-Low","3-Average","4-Good","5-Best")
###
### end input parameter preparation
###

## preprocessing
kmeans_columns <- c()
nr_input_fields <- length(column_list)
for(i in 1:nr_input_fields)
{
  col_name <- column_list[i]
  new_col <- paste0(col_name,"_P")
  kmeans_columns <- c(kmeans_columns,new_col)
  is_log <- log_list[i]
  if(is_log==1)
  {
    df[,new_col] <- log(df[,col_name])
  }else
  {
    df[,new_col] <- df[,col_name]
  }
  
  min_c <- min(df[,new_col])
  max_c <- max(df[,new_col])
  df[,new_col] <- (df[,new_col] - min_c) / max_c
}
## end preprocessing

# base clusterization model
clust_column <- "AssignmentID"
label_column <- "AssignmentLabel"
score_column <- "AssignmentScore"
loss_column <- "TScore"
subcl_column <- "SubCluster"
nrcus_column <- "AssignmentNoClients"
tempc_column <- "TempCluster"
pivot_columns <- c(label_column, centroid_labels)
acols <- c(clust_column, kmeans_columns, subcl_column,column_list,centroid_labels,subcluster_column_list)

trained_model <- kmeans(x=df[, kmeans_columns], centers=n_centers,nstart=100)
df[,tempc_column] <- trained_model$cluster

df_centers <- trained_model$center
scores  <- rowMeans(df_centers)
TScore <- trained_model$tot.withinss
df_centers <- data.frame(df_centers,scores,TScore)
colnames(df_centers) <- c(centroid_labels, score_column, loss_column )
#TempCluster <- rownames(df_centers)
df_centers[,tempc_column] <- 1:nrow(df_centers)

df_centers <- df_centers[order(df_centers[,score_column]),]
df_centers[,clust_column] <- 1:nrow(df_centers)
df_centers[,label_column] <- segment_labels

df <- merge(df,df_centers, by=tempc_column)



clusterList <- unique(df[,clust_column])
n_cl <- length(clusterList)

for(i in 1:n_cl)
{
  cl <- clusterList[i]
  df_centers[df_centers[,clust_column]==cl,nrcus_column] <- nrow(df[df[,clust_column]==cl,])
}
df_downsampled <- data.frame()
# generate sub-clusters for each cluster
for(i in 1:n_cl){
  cl <- clusterList[i]
  print(sprintf("Clustering cluster %d",cl))
  cl_model <- kmeans(df[df[,clust_column] == cl,kmeans_columns], centers = n_downsample, nstart = 30)
  df_sub <- data.frame(cl_model$centers)
  df_sub[,subcl_column] <- rownames(cl_model$centers)
  df_sub[,clust_column] <- cl
  colnames(df_sub) <- c(subcluster_column_list, subcl_column, clust_column)
  df[df[,clust_column] == cl,subcl_column] <- cl_model$cluster
  df_centers[df_centers[,clust_column]==cl,nrcus_column] <- nrow(df[df[,clust_column]==cl,])
  
  df_downsampled <- rbind(df_downsampled,df_sub)
}

df <- merge(df, df_downsampled, by=c(clust_column,subcl_column))


dfa <- data.frame(df_centers[,c(label_column,clust_column,score_column,nrcus_column,centroid_labels)])

###
### END SQL SCRIPT
### @outputs: 
###     df:  full dataset including downsampled subclustering information
###     dfa: segment inferred information
###



##
## Power BI client side R script for visualizations 
##
## plot clusters using R Server ggplot2 (CRAN or Microsoft Open R)
##
library(ggplot2)

x_dim <- "sub_R"
y_dim <- "sub_M"
z_dim <- "sub_F"
c_dim <- label_column
n_dim <- clust_column
s_dim <- subcl_column


clust_column <- "AssignmentID"
label_column <- "AssignmentLabel"
score_column <- "AssignmentScore"
subcl_column <- "SubCluster"
nrcus_column <- "AssignmentNoClients"

column_list <- c("Recency","DailyFrequency","Revenue")

calculated_column_list <- kmeans_columns
subcluster_column_list <- c("sub_R","sub_F","sub_M")

library(rgl)
library(car)
col_df_prep <- c(column_list,subcluster_column_list, 
                 clust_column, label_column, subcl_column, score_column, nrcus_column)

use_dplyr = FALSE
if(use_dplyr)
{
  library(dplyr)
  #group_columns = c(clust_column,label_column,subcl_column)
  #dots <- lapply(group_columns, as.symbol)
  #agg_df <- group_by_(.data = df, .dots = dots)
  #mean_columns <- c(calculated_column_list,subcluster_column_list)
  #dots2 <- lapply(mean_columns, as.symbol)
  #down_df <- data.frame(summarise_(agg_df, .dots = dots2))
  #down_df <- data.frame(summarise(agg_df, X=mean(df[,x_dim]), Y=mean(df[,y_dim])))
  
  agg_df <- group_by(df, df[,clust_column], df[,label_column],df[,subcl_column])
  
  down_df <- data.frame(summary(agg_df,X=mean(df[,x_dim]), Y=))
  
  down_df[,c_dim] <- factor(down_df[,clust_column])
  down_df[,subcl_column] <- factor(down_df[,subcl_column])
  
}else 
{
  down_df <- aggregate(x = df, 
                       by = list(ID=df$AssignmentLabel,
                                 SID=df$SubCluster, 
                                 NID = df$AssignmentID), 
                       FUN=mean)
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

cl_labels <- sort(unique(dataset[,c_dim]))
plot1 <- qplot(dataset[,x_dim],dataset[,y_dim],
               shape = dataset$ID,#dataset[,c_dim], 
               color = dataset$ID,#dataset[,c_dim], 
               size = I(5.0),
               alpha = 0.5)
plot1 <- plot1 + labs(x=x_dim, y=y_dim)
#plot1 <- plot1 + scale_shape_discrete(name  ="Segments",labels=dataset[,c_dim])
#plot1 <- plot1 + scale_colour_discrete(name  ="Segments",labels=cl_labels)
plot1 <- plot1 + geom_text(aes_string(label=dataset$SID #dataset[,s_dim]
                                      ),
                           color="black", size=3)

plot1

### END PBI

##
## R Client-side 3D plot example
##
NR_3D_PLOT = 1
if(NR_3D_PLOT ==1)
{
  scatter3d(down_df$sub_R,down_df$sub_F,down_df$sub_M,
            xlab="Recency", 
            ylab="Frequency",
            zlab="Revenue(M)",
            groups=down_df$ID, 
            surface=FALSE, grid = FALSE, ellipsoid = TRUE,
            sphere.size = 2)
}else if(NR_3D_PLOT==2)
{
  scatter3d(down_df$sub_R,down_df$sub_F,down_df$sub_M,
            xlab="Recency", 
            ylab="Frequency",
            zlab="Revenue(M)",
            
            point.col = down_df$NID, 
            surface = FALSE,
            labels = down_df$SID,
            id.n = nrow(down_df),
            sphere.size = 2)
}

### end 3D plot

### BEGIN HEAT PLOT

if (SHOW_HEAT_MAPP)
{
  library(reshape)
  
  piv_df <- melt(df_centers[,pivot_columns], id = label_column)
  
  heat_plot <- ggplot(piv_df, aes(AssignmentLabel,variable)) + 
    geom_tile(aes(fill = c(value)),colour = "white") + 
    scale_fill_gradient(low = "white",high = "steelblue")
  
  heat_plot
}

### END HEAT PLOT

### Begin GENERATIVE CLUSTERING


if(SHOW_TSNE_KMPL)
{
  STD_TSNE <- FALSE
  if(STD_TSNE)
  {
    library(tsne)
    tsne_res <- tsne(as.matrix(down_df[, column_list]))
    tsne_kmeans <- kmeans(tsne_res,centers = 4, nstart = 100)
    tsne_colors <- as.factor(tsne_kmeans$cluster)
    down_df$VisualClustering <- tsne_kmeans$cluster
    plot2 <- qplot(tsne_res[,1],tsne_res[,2],shape = down_df$AssignmentID, color = tsne_colors)
    plot2 <- plot2 + theme(legend.position = "none")
    plot2
    
  }else
  {
    library(Rtsne)
    rtsne_res <- Rtsne(as.matrix(down_df[, subcluster_column_list]), 
                       check_duplicates = FALSE,
                       pca = FALSE)
    tsne_kmeans <- kmeans(rtsne_res$Y,centers = 4, nstart = 100)
    tsne_colors <- as.factor(tsne_kmeans$cluster)
    down_df$VisualClustering <- tsne_kmeans$cluster
    plot2 <- qplot(rtsne_res$Y[,1],rtsne_res$Y[,2],shape = down_df$ID, color = tsne_colors)
    plot2 <- plot2 + theme(legend.position = "none")
    plot2
  }
}




if (SHOW_TSNE_FULL)
{
  rtsne_res <- Rtsne(as.matrix(df[, column_list]), check_duplicates = FALSE)
  plot3 <- qplot(rtsne_res$Y[,1],rtsne_res$Y[,2],shape = df$cluster, color = df$cluster)
  plot3 <- plot2 + theme(legend.position = "none")
  plot3
}

### END Generative clustering

# end script

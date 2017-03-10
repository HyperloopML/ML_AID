

mypath <- dirname(sys.frame(1)$ofile)
exploreData <- read.csv(paste0(mypath,"/test.csv"))
finalData <- data.frame(exploreData)
df <- finalData

# begin script (Microsoft R Server 9.0.3 using Microsoft R Open 3.3.2)

# parameter preparation
n_centers <- 5
n_downsample <- 100
column_list <- c("R","F","M")
centroid_labels <- c("c_R","c_F","c_M")
        
# base clusterization model
trained_model <- kmeans(x=df[, column_list], centers=n_centers,nstart=100)
df$cluster <- trained_model$cluster
centre <- trained_model$center
scoruri <- rowMeans(centre)
TScore <- trained_model$tot.withinss
centre <- cbind(centre,scoruri,TScore)
colnames(centre) <- c(centroid_labels,"Scor", "TScore")
cluster <- rownames(centre)
centre <- cbind(cluster, centre)
df <- merge(df,centre)

clusterList <- unique(df$cluster)
n_cl <- length(clusterList)

# generate sub-clusters for each cluster
for(i in 1:n_cl){
  cl <- clusterList[i]
  print(sprintf("Clustering cluster %d",cl))
  cl_model <- kmeans(df[df$cluster == cl,column_list], centers = n_downsample, nstart = 2)
  df[df$cluster == cl,"SubCluster"] <- cl_model$cluster
}

#plot clusters using R Server ggplot2 (CRAN or Microsoft Open R)
library(ggplot2)

use_dplyr = TRUE
if(use_dplyr)
{
  library(dplyr)
  agg_df <- group_by(df, cluster,SubCluster)
  down_df <- data.frame(summarise(agg_df, R_mean=mean(R), F_mean=mean(F), M_mean=mean(M)))
  
}else 
{
  down_df <- aggregate(. ~ SubCluster + cluster, FUN = mean, data = df)
}

down_df$cluster <- factor(down_df$cluster)
down_df$SubCluster <- factor(down_df$SubCluster)


qplot(down_df$R,down_df$M,
      shape = down_df$cluster, 
      color = down_df$SubCluster,
      size = I(5.0),
      alpha = 0.5)+geom_text(aes(labels=down_df$SubCluster),color="black")

# end script
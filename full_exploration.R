library(caret)


cat("\nIncarcare dataset initial...\n")
df_init <- read.csv("D:\\GoogleDrive\\_hyperloop_data\\sample.csv")
cat("Done incarcare dataset initial.\n")
print(df_init[1:5,1:15])

selectie_coloane <- c("R","F","M", "MARGIN")

subcluster_column_list <- c()
centroid_labels <- c()

for (i in 1:length(selectie_coloane)) {
  col <- selectie_coloane[i]
  new_col <- paste0("cen", i, "_", substr(col, 1, 5))
  sub_new_col <- paste0("sub", i, "_", substr(col, 1, 5))
  centroid_labels <- c(centroid_labels, new_col)
  subcluster_column_list <- c(subcluster_column_list, sub_new_col)
}

cat("\nSelectie subset date...\n")
df_simplu <- df_init[, selectie_coloane]
print(df_simplu[1:5,])

cat("\nPreprocesare\n")
prep <- preProcess(df_simplu, method = c("range"))
df_scal <- predict(prep, df_simplu)
print(df_scal[1:5,])

cat("\nInspectie date scalate...\n")
print(summary(df_scal))

cat("\nAjustare margine negativa si valori nule...\n")
df_simplu[df_simplu<0] <- 1e-5
df_simplu[df_simplu==0] <- 1e-3

cat("\nLogaritmare ...\n")
df_scal <- log(df_simplu)
print(df_scal[1:5,])


cat("\nRe-preprocesare si inspectare...\n")
prep <- preProcess(df_scal, method = c("range"))
df_scal <- predict(prep, df_scal)
print(df_scal[1:5,])
print(summary(df_scal))

cat("\nKMeans...\n")
km <- kmeans(df_scal[,selectie_coloane], centers = 4, nstart = 15)
print(km$centers)
cat("\nScoruri segmente principale...\n")
print(rowSums(km$centers))

df_centers <- data.frame(km$centers)
df_centers["SCORE"] <- rowSums(df_centers)

df_centers["ORIG_CL"] <- 1:4

df_centers <- df_centers[order(df_centers$SCORE),]
df_centers["CLUSTER"] <- 1:4
colnames(df_centers) <- c(centroid_labels, "SCORE","ORIG_CL","CLUSTER")
print(df_centers)

clust_column <- "CLUSTER"

df_scal["KM_CLUSTER"] <- km$cluster
df_t <- merge(x=df_scal, y=df_centers, by.x = "KM_CLUSTER", by.y = "ORIG_CL")
print(df_t[1:10,])



df_centers["NR_CLIENTS"] <- 0

for (i in 1:4) {
  df_centers[df_centers[, "ORIG_CL"] == i, "NR_CLIENTS"] <- nrow(
    df_scal[df_scal[, "KM_CLUSTER"] == i,])
}
cat("\nStatistica finala segmente:\n")
print(df_centers)

clusterList <- unique(df_t[, "CLUSTER"])
n_cl <- length(clusterList)

n_downsample <- 10

df_downsampled <- data.frame()
df_t[, "KM_MICRO"] <- -1

# generate sub-clusters for each cluster
for (i in 1:n_cl) {
  cl <- clusterList[i]
  cat(sprintf("\nSubclustering cluster %d...\n", cl))
  cl_model <- kmeans(df_t[df_t[, "CLUSTER"] == cl, selectie_coloane],
           centers = n_downsample, nstart = 10)
  
  
  df_subcenters <- cl_model$center
  scores <- rowMeans(df_subcenters)
  df_subcenters <- data.frame(df_subcenters, scores)
  colnames(df_subcenters) <- c(subcluster_column_list, "MICRO_SCORE")
  
  df_subcenters[, "CLUSTER"] <- cl
  df_subcenters[, "KM_MICRO"] <- 1:nrow(df_subcenters)
  df_subcenters <- df_subcenters[order(df_subcenters[, "MICRO_SCORE"]),]
  df_subcenters[, "MICRO"] <- 1:nrow(df_subcenters)
  
  df_t[df_t[, "CLUSTER"] == cl, "KM_MICRO"] <- cl_model$cluster
  

  df_downsampled <- rbind(df_downsampled, df_subcenters)
}

df_tm <- merge(x = df_t, y= df_downsampled, 
               by=c("CLUSTER", "KM_MICRO"))
print(df_tm[1:5,])


plt <- qplot(x=df_downsampled[,"sub3_M"], 
             y=df_downsampled[,"sub4_MARGI"], 
             color= as.factor(df_downsampled[,"CLUSTER"]),
             size = I(5))

plt <- plt  + geom_text(aes_string(label = df_downsampled[,"MICRO"]),
                        color = "black", size = 5)

plt

df_train <- df_tm[,c("R","F","M","MARGIN","CLUSTER")]

predictors_names <- c("PREDICTOR_1", "PREDICTOR_2", "PREDICTOR_3", "PREDICTOR_4")

colnames(df_train) <- c(predictors_names, "TARGET")
df_train[,"TARGET"] <- as.factor(df_train[,"TARGET"])

tree <- rpart(formula = TARGET~PREDICTOR_1+PREDICTOR_2+PREDICTOR_3+PREDICTOR_4,
              data = df_train)

#df_train_check <- df_train[c(27,25000,50000), predictors_names]
predictions <- predict(tree, df_train, type = c("class"))
confusionMatrix(data = predictions, reference = df_train[,"TARGET"])


#lin_model <- glm(formula = TARGET~PREDICTOR_1+PREDICTOR_2+PREDICTOR_3+PREDICTOR_4,
#                 family = binomial(link='logit'),
#                 data = df_train)

#predictions <- predict(lin_model, df_train)


### de facut corect train-test split



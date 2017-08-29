
###
### Script explorare si analiza descriptiva a proiectului Hyperloop
### Data creere:  2017-07-05
### Ultima modif: 2017-08-28
###

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

predictions <- predict(tree, df_train, type = c("class"))
cnf <- confusionMatrix(data = predictions, reference = df_train[,"TARGET"])
cat(sprintf("Acuratetea  modelului: %.1f%%\n", cnf$overall["Accuracy"]*100))
cat(sprintf("Capacitatea modelului: %.1f%%\n", cnf$overall["Kappa"]*100))
cat("Tabela de confuzie:\n")
print(cnf$table)
cat("Recall, precizie, medie armonica F1:\n")
print(cnf$byClass[,c("Recall","Precision","F1")])

### abordare corecta de validare 

train_partition <- createDataPartition(df_train[,"TARGET"], p = 0.85, list = FALSE)
df_new_train <- df_train[train_partition, ]
df_new_test <- df_train[-train_partition, ]

tree2 <- rpart(formula = TARGET~PREDICTOR_1+PREDICTOR_2+PREDICTOR_3+PREDICTOR_4,
               data = df_new_train)
cat("Rulam predictia pe datele de training:\n")
predictions <- predict(tree, df_new_train, type = c("class"))
cnf1 <- confusionMatrix(data = predictions, reference = df_new_train[,"TARGET"])
cat(sprintf("Acuratetea  modelului: %.1f%%\n", cnf1$overall["Accuracy"]*100))
cat(sprintf("Capacitatea modelului: %.1f%%\n", cnf1$overall["Kappa"]*100))
cat("Tabela de confuzie:\n")
print(cnf1$table)
cat("Recall, precizie, medie armonica F1:\n")
print(cnf1$byClass[,c("Recall","Precision","F1")])

cat("Rulam predictia pe datele de test:\n")
predictions <- predict(tree, df_new_test, type = c("class"))
cnf2 <- confusionMatrix(data = predictions, reference = df_new_test[,"TARGET"])
cat(sprintf("Acuratetea  modelului: %.1f%%\n", cnf2$overall["Accuracy"]*100))
cat(sprintf("Capacitatea modelului: %.1f%%\n", cnf2$overall["Kappa"]*100))
cat("Tabela de confuzie:\n")
print(cnf2$table)
cat("Recall, precizie, medie armonica F1:\n")
print(cnf2$byClass[,c("Recall","Precision","F1")])





# NLP processing, BoW, tokenization, etc
library(tm)
cat("\nIncarcare dataset produse/descrieri...\n")
df_desc <- read.csv("D:\\GoogleDrive\\_hyperloop_data\\text_items.csv")
cat(sprintf("Dimensiunea initiala %.1f MB", object.size(df_desc) / (1024*1024)))
cat("Done incarcare dataset produse/descrieri.\n")

timeit("Removing unknown chars Text: ", df_desc$AllText <- iconv(df$AllText, 'Latin-9'))
timeit("Removing unknown chars Name: ", df_desc$ItemName <- iconv(df$ItemName, 'Latin-9'))

toMatch <- c("invalid", "inactiv")
excluded_filter <- paste(toMatch, collapse = "|")
cat(paste0("Curatam toate produsele invalide/inactive ",excluded_filter,"...\n"))
excluded_recs <- grep(excluded_filter, df_desc$ItemName)
df_desc <- df_desc[-excluded_recs,]
cat(sprintf("Dimensiunea finala %.1f\n", object.size(df_desc)/(1024*1024)))

vector_src <- VectorSource(df_desc[, "AllText"])
corpus <- Corpus(vector_src)
dtm <- DocumentTermMatrix(corpus)
mtxt1 <- as.matrix(dtm)
cat(sprintf("Total produse %d cu total de %d atribute\n",nrow(mtxt1),ncol(mtxt1)))
sparsitate = 0.001
cat(sprintf("Inlaturam atributele care apar la mai putin de %.1f%% din produse\n",sparsitate*100))
dtm <- removeSparseTerms(dtm, 1-sparsitate)
mtxt2 <- as.matrix(dtm)
cat(sprintf("Total produse %d cu total de %d atribute\n",nrow(mtxt2),ncol(mtxt2)))


df_items <- data.frame(ID = df_desc$ItemId, Name = df_desc$ItemName, mtxt2)

s_atribute <- "cosm"
s_produs <- "chapter"
coloane_gasite = grep(s_atribute, colnames(df_items), ignore.case = T)

df_subset <- df_items[grep(s_produs, df_items$Name, ignore.case = T),]

print(df_subset[1:20, c(1,2,coloane_gasite)])


### construirea dataset-ului de tranzactii per microsegment
### se asociaza proprietatile cu valoarea agregata a microsegmentului (per produs)
### incarcam datele (sample) si urmeaza sa construim modelul de regresie

# incarcam sample 2
# concatenam date
# construim model liniar si vedem de este varza
# construim model non-liniar si ne bucuram
# analiza rezultatelor cu R2 si explicam ce inseamna analiza regresiei




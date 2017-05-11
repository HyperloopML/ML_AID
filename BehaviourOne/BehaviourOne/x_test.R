library(caret)
library(xgboost)
data(iris)

## deparse best tuning parameters
get_tune_params <- function(caret_model) {
    best <- caret_model$bestTune
    sparams <- ""
    for (i in 1:length(names(best))) {
        sparams <- paste0(sparams,sprintf("%s=%.2f ", names(best[i]), best[i]))
    }
 return(sparams)
}
## end deparse tuning params

all_log <- " "
logger <- function(text) {
    all_log <<- paste0(all_log, text)
    cat(text)
}

timeit = function(strmsg, expr) {
    tm <- system.time(expr)
    ftime <- tm[3]
    stm <- sprintf("%.2f min", ftime / 60)
    logger(paste0(strmsg, " executed in ", stm, "\n"))
    return(ftime)
}

iris$Species <- as.factor(iris$Species)

logger("partitioning...\n")
timeit("cPart: ",
    intrain <- createDataPartition(iris$Species, list = FALSE, p = 0.7))
logger("done partitioning.\n")

pred_vars <- setdiff(names(iris),"Species")

x_train <- iris[intrain, pred_vars]
y_train <- iris[intrain, "Species"]
x_valid <- iris[-intrain, pred_vars]
y_valid <- iris[-intrain, "Species"]






# set up the cross-validated hyper-parameter search
xgb_grid_1 = expand.grid(
  nrounds       = c(50,100),
  eta           = c(0.3, 0.01, 0.001, 0.0001),
  max_depth     = c(2, 3, 5, 7, 10),
  gamma         = c(0, 1),
  colsample_bytree = c(0.30, 0.60),
  min_child_weight = c(1.00,5.00)
)

# pack the training control parameters
xgb_trcontrol_1 = trainControl(
  method = "cv",
  number = 5,
  verboseIter = TRUE,
  returnData = FALSE,
  returnResamp = "all", # save losses across all models
  classProbs = TRUE, # set to TRUE for AUC to be computed
  #summaryFunction = twoClassSummary,
  allowParallel = TRUE
)

# train the model for each parameter combination in the grid, 
#   using CV to evaluate
logger("training 2...\n")
timeit("xgbt: ",
       xgb_train_1 <- train(
                            x = x_train,
                            y = y_train,
                            trControl = xgb_trcontrol_1,
                            tuneGrid = xgb_grid_1,
                            method = "xgbTree"
                          )
                        )


logger("done training 2.\n")

# scatter plot of the AUC against max_depth and eta
ggplot(xgb_train_1$results, aes(x = as.factor(eta), y = max_depth, size = ROC, color = ROC)) +
  geom_point() +
  theme_bw() +
scale_size_continuous(guide = "none")

t2 <- get_tune_params(xgb_train_1)


logger("training 1...\n")
timeit("xgbt: ",
       xclf <- train(x = x_train, y = y_train,
                   method = "xgbTree", metric = "kappa"
                   )
      )
logger("done training 1.\n")

t1 <- get_tune_params(xclf)

y_1 <- predict(xclf, x_valid)
y_2 <- predict(xgb_train_1, x_valid)

c1 <- confusionMatrix(y_valid, y_1)
c2 <- confusionMatrix(y_valid, y_2)

logger("automatic:")
c1
logger("tune controls:")
c2

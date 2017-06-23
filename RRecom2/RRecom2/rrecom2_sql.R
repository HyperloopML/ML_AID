##
##
##
## @script:       Microsegmentation Content Based Recommender V2 System
## @v1_created:   2017.03.23
## @v2_created:   2017.05.23
## @lastmodified: 2017.06.15
## @project:      Hyperloop
## @subproject:   Machine Learning module
## @platform:     Microsoft R Server, SQL Server, RevoScaleR
## @author:       Alexndru PURDILA, Andrei Ionut DAMIAN
## @company:      High-Tech Systems and Services SRL
##
##
##
##

cat("\nStarting script...\n")

library(reshape2)



User.Field <- "MICRO_SGM_ID"
MicroSegment.Field <- "MICRO_SGM_ID"



DEBUG = TRUE


SGD.ALPHAS <- c(0.0001) #, 0.0005, 0.0001)
SGD.EPOCHS <- c(100) #, 90)
SGD.LAMBDAS <- c(0.001) #, 0.1)
SGD.SCALE_MIN_MAX <- c(0)
SGD.MOMENTUMS <- c(1)
NORMALEQ.LAMBDAS <- c(0.5) #,1,1.5,2,2.5,3,3.5)
LAMBDA_NORMAL_EQ <- 0.5

test_df <- NULL




Target.Field <- "AMOUNT"
Target.Field.2 <- "QTY"
Target.Field.3 <- "COUNT"
Product.Field <- "ITEM_ID"
Product.Name <- "ITEM_NAME"
Enabled.Field <- "ENABLED"
Period.Field <- "PER_ID"
Segmentation.Field <- "SGM_ID"
Model.Field <- "MODEL_ID"

NonPredictors.Fields <- c(
                        User.Field
                        , Target.Field
                        , Product.Field
                        , Product.Name
                        , Target.Field.2
                        , Target.Field.3
                        , Period.Field
                        , Model.Field
                        , Segmentation.Field
                        , Enabled.Field
                        )




MODULE.VERSION <- "2.0.2.3"
MODULE.NAME <- "BEHAVIOR_RECOMMENDATIONS_V2_SQL"
MODULE.NAME.SHORT <- "BE_SQL"


TABLE.OUTPUTVECTORS <- "BEV_SGM_MICRO"

FILE.MATRIX = "BEHAVIOR_MATRIX"
FILE.SCORES = "SCORES_MATRIX"



##
## HELPER FUNCTIONS
##
##


HELPER_LIB_VER <- "1.5.1"


FULL_DEBUG <- FALSE

options(scipen = 999)
options(digits = 3)

get_formula <- function(lbl, vars) {
    slabel <- paste0(lbl, "~")
    sfactors <- paste(vars, collapse = "+")
    clf_formula = as.formula(paste(slabel, sfactors))
    return(clf_formula)
}

get_formula_nobias <- function(lbl, vars) {
    slabel <- paste0(lbl, "~")
    sfactors <- paste(vars, collapse = "+")
    sfactors <- paste0("0+", sfactors)
    clf_formula = as.formula(paste(slabel, sfactors))
    return(clf_formula)
}

nonzero <- function(v) {
    non_zero_ind <- which(!(v == 0))
    non_zero_list <- v[non_zero_ind]
}



all_log <- ""

log_ctime <- format(Sys.time(), "%Y%m%d%H%M%S")

logger <- function(stext) {
    prefix <- ""
    if (substr(stext, 1, 1) == "\n") {
        prefix <- "\n"
        stext <- substr(stext, 2, 10000)
    }
    slog_ctime <- format(Sys.time(), "[%Y-%m-%d %H%:%M:%S]")
    slog_ctime <- sprintf("[%s]%s", MODULE.NAME.SHORT, slog_ctime)
    if (is.vector(stext)) {
        stext <- paste(stext, collapse = ",", sep = "")
    }
    stext <- paste0(prefix, slog_ctime, " ", stext)
    if (substr(stext, nchar(stext), nchar(stext) + 1) != "\n") {
        stext <- paste0(stext, "\n")
    }

    cat(stext)
    flush.console()
    all_log <<- paste(all_log, stext, sep = "")

}

logger(sprintf("Preparing helper lib v%s...\n", HELPER_LIB_VER))

timeit = function(strmsg, expr, NODATE = TRUE) {
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
    ftime <- round(ftime, digits = 2)
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


#########################
##
##
## END HELPER FUNCTIONS
##
#########################




RSquared <- function(y, yhat) {
    ymean <- mean(y)
    rsq <- 1 - sum((yhat - y) ** 2) / sum((y - ymean) ** 2)
    return(rsq)
}

SimpleCost <- function(coefs, data, y) {
    H <- data %*% coefs
    h2 <- (H - y) ** 2
    nr_obs <- length(h2)
    cost <- sum(h2) / nr_obs
    #cost <- cost + 
    if (!is.finite(cost)) {
        logger("CNF Cost generated not finite value!\n")
        cost <- 5000
    }
    return(cost)
}


library(MASS)

TrainNormalEquation <- function(data, target_col, var_cols) {
    items_coefs <- as.data.frame(data[, var_cols])
    y_train <- data[, target_col]




    coefs_names <- c("BIAS", var_cols)
    items_coefs <- as.matrix(items_coefs)
    X <- cbind(rep(1, nrow(items_coefs)), items_coefs)
    colnames(X) <- coefs_names
    if (DEBUG && FALSE) {
        logger("\nNormal Eq X:\n")
        print(X[1:5, 1:9])
    }

    lambda = LAMBDA_NORMAL_EQ
    I <- diag(ncol(X))

    Theta <- base::t(ginv(base::t(X) %*% X + lambda * I) %*% base::t(X) %*% y_train)
    micro_coefs_neq <- as.vector(Theta)
    names(micro_coefs_neq) <- coefs_names
    if (DEBUG) {
        #test_df <<- X %*% micro_coefs_neq
        y_train <- data[, target_col]
        J <- SimpleCost(micro_coefs_neq, X, y_train)
        logger(paste0("\nNormalEq with lambda: ", lambda, " MSE ERROR: ", J, "\n"))
    }
    return(micro_coefs_neq)
}



GetScores <- function(df_items, coefs) {
    X <- cbind(rep(1, nrow(df_items)), df_items)

    if (DEBUG) {
        logger(sprintf("GetScores(Items: %s , Coefs: %d)", toString(dim(X)), length(coefs)))
    }

    scores <- as.matrix(X) %*% as.vector(coefs)
    return(scores)
}



##
## start demo and behavior vectors prep !
##



All.Microsegments <- unique(df[, User.Field])
df_output <- data.frame()
Predictor.Fields <- setdiff(colnames(df), NonPredictors.Fields)



for (MicroSegment in All.Microsegments) {
    logger(sprintf("Trainin on ID %d",MicroSegment))
    df_micro <- df[df[, User.Field] == MicroSegment,]
    logger("Converting to numerical ...")
    df_micro[, Predictor.Fields] <- sapply(df_micro[, Predictor.Fields], as.numeric)
    logger("Done converting to numerical.")
    ModelID <- df_micro[1, Model.Field]
    SegmentationID <- df_micro[1, Segmentation.Field]
    y <- df_micro[, Target.Field]
    MicroCoefs <- TrainNormalEquation(df_micro, Target.Field, Predictor.Fields)

    yhat <- GetScores(df_micro[, Predictor.Fields], MicroCoefs)
    RMSE <- sqrt(mean((yhat - y) ** 2))
    RSQUARED <- RSquared(y, yhat)
    logger(sprintf("NORMAL EQUATION MICRO=%d RMSE=%.1f R2=%.2f",
                        MicroSegment, RMSE, RSQUARED))

    df_output <- rbind(df_output, c(ModelID, SegmentationID,
                                    MicroSegment, RMSE, RSQUARED, MicroCoefs))

}


colnames(df_output) <- c(Model.Field, Segmentation.Field, MicroSegment.Field, "RMSE",
                         "RSQUARED", names(MicroCoefs))

logger(sprintf("Output 1: %s", toString(dim(df_output))))


df_output_melted <- melt(df_output,
    id = c(Model.Field, Segmentation.Field, MicroSegment.Field, "RMSE", "RSQUARED"))

logger(sprintf("Output 2(final): %s", toString(dim(df_output_melted))))

logger("Script done.")
all_log <- " "
logger <- function(text) {
    all_log <<- paste0(all_log, text)
    cat(text)
}
df <- data.frame(X = c(-100, -10, -3, -2, -1, 0, 1, 0, 1), Y = c(-200, -20, -10, -7, -3, 2, 3, 0, 1))
column_list <- colnames(df)
df$ORIG_X <- df$X
df$ORIG_Y <- df$Y
log_list <- c(1,1)
#####################################################################3

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
t1_prep <- proc.time()
prep_time <- (t1_prep[3] - t0_prep[3]) /60

logger(sprintf("Done preprocessing. Total time %.2f min\n", prep_time))
## end preprocessing

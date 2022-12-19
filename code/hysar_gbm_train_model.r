
gc()


# Parameter settings for the second phase GBM method.
params <- list(num_leaves = 256,
               learning_rate = 0.005,
               objective = "binary",
               max_depth = 8,
               metric = "auc",
               feature_fraction = 0.8,
               bagging_fraction = 0.8,
               bagging_freq = 1,
               min_data_in_leaf = 1000,
               num_threads = -1
               )

max_rounds <- 5000

lgbm_early_stopping_rounds_val <- 50


targetDT <- clickstreamEventsExpandedDT

rm(clickstreamEventsExpandedDT); gc()

targetDT <- targetDT[((time_step %in% timestepSubset) & !(event == "add_to_cart" & future_event == "buy")),]

gc()

featureNames <- readRDS(paste0(cachePath, "featureNames", "_", extendedFileSuffix, ".rds"))


if (filterSameItem) {

    targetDT <- targetDT[same_item != 1,]

    featureNames <- setdiff(featureNames, "same_item")

}

gc()

targetDT <- targetDT[!(event == "add_to_cart" & future_event == "buy")]

baseRecDT <- baseRecDT[!duplicated(baseRecDT[, list(session_id, timestamp_int, current_item_id, future_item_id)])]
gc()

try(setnames(baseRecDT, "future_item_rank", "predicted_future_item_rank", skip_absent=TRUE))

try(setnames(baseRecDT, "future_item_rank_base_1", "predicted_future_item_rank_base_1", skip_absent=TRUE))

try(setnames(baseRecDT, "future_item_rank_base_2", "predicted_future_item_rank_base_2", skip_absent=TRUE))

try(setnames(baseRecDT, "future_item_score", "predicted_future_item_score", skip_absent=TRUE))

targetDT[, orig_row_number := 1:.N]

targetBaseRecMergedDT <- merge(targetDT[, list(session_id, timestamp_int, current_item_id, future_item_id, orig_row_number)],
                                baseRecDT[, list(session_id, timestamp_int, current_item_id, future_item_id, predicted_future_item_rank,
                                                    predicted_future_item_rank_base_1, predicted_future_item_rank_base_2)],
                                by = c("session_id", "timestamp_int", "current_item_id", "future_item_id"),
                                all.x = TRUE, all.y = FALSE, sort = FALSE)

targetDT <- targetDT[(orig_row_number %in% targetBaseRecMergedDT$orig_row_number),]

setorder(targetDT, orig_row_number)

setorder(targetBaseRecMergedDT, orig_row_number)

colsToAdd <- setdiff(colnames(targetBaseRecMergedDT), c("session_id", "timestamp_int", "current_item_id", "future_item_id", "orig_row_number"))

targetDT[, (colsToAdd) := targetBaseRecMergedDT[, colsToAdd, with = FALSE]]

rm(targetBaseRecMergedDT); gc()


if (!exists("baseRec1DT")) {

    baseRec1DT <- readRDS(paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_1, ".rds"))

    try(setnames(baseRec1DT, "from_item_id", "current_item_id", skip_absent = TRUE))

}

try(setnames(baseRec1DT, "future_item_rank", "predicted_future_item_rank", skip_absent=TRUE))

try(setnames(baseRec1DT, "future_item_score", "predicted_future_item_score", skip_absent=TRUE))


baseRec1DT[, predicted_future_item_rank := NULL]

gc()

targetDT[, orig_row_number := 1:.N]

targetBaseRecMergedDT <- merge(targetDT[, list(session_id, timestamp_int, current_item_id, future_item_id, orig_row_number)], baseRec1DT,
                                by = c("session_id", "timestamp_int", "current_item_id", "future_item_id"), all.x = TRUE, all.y = FALSE, sort = FALSE)

targetDT <- targetDT[(orig_row_number %in% targetBaseRecMergedDT$orig_row_number),]

setorder(targetDT, orig_row_number)

setorder(targetBaseRecMergedDT, orig_row_number)

colsToAdd <- setdiff(colnames(targetBaseRecMergedDT), c("session_id", "timestamp_int", "current_item_id", "future_item_id", "orig_row_number"))

setorder(targetBaseRecMergedDT, orig_row_number)

targetBaseRecMergedDT <- targetBaseRecMergedDT[!duplicated(targetBaseRecMergedDT$orig_row_number),]

targetDT[, (colsToAdd) := targetBaseRecMergedDT[, colsToAdd, with = FALSE]]

rm(targetBaseRecMergedDT, baseRec1DT); gc()

if (!exists("baseRec2DT")) {

    baseRec2DT <- readRDS(paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_2, ".rds"))

    try(setnames(baseRec2DT, "from_item_id", "current_item_id", skip_absent = TRUE))

}

try(setnames(baseRec2DT, "future_item_rank", "predicted_future_item_rank", skip_absent=TRUE))

try(setnames(baseRec2DT, "future_item_score", "predicted_future_item_score", skip_absent=TRUE))

baseRec2DT[, predicted_future_item_rank := NULL]

gc()

targetDT[, orig_row_number := 1:.N]

targetBaseRecMergedDT <- merge(targetDT[, list(session_id, timestamp_int, current_item_id, future_item_id, orig_row_number)], baseRec2DT,
                                by = c("session_id", "timestamp_int", "current_item_id", "future_item_id"), all.x = TRUE, all.y = FALSE, sort = FALSE)

targetDT <- targetDT[(orig_row_number %in% targetBaseRecMergedDT$orig_row_number),]

setorder(targetDT, orig_row_number)

setorder(targetBaseRecMergedDT, orig_row_number)

colsToAdd <- setdiff(colnames(targetBaseRecMergedDT), c("session_id", "timestamp_int", "current_item_id", "future_item_id", "orig_row_number"))

setorder(targetBaseRecMergedDT, orig_row_number)

targetBaseRecMergedDT <- targetBaseRecMergedDT[!duplicated(targetBaseRecMergedDT$orig_row_number),]


targetDT[, (colsToAdd) := targetBaseRecMergedDT[, colsToAdd, with = FALSE]]

rm(targetBaseRecMergedDT, baseRec2DT); gc()


includeFeatureNames <- c(setdiff(includeFeatureNames, c("predicted_future_item_rank_base_1", "predicted_future_item_rank_base_2")),
                                                        "predicted_future_item_rank_base_1", "predicted_future_item_rank_base_2")

try(featureNames <- setdiff(featureNames, c("base_rec_pred_rank", "base_rec_pred_rank_1", "base_rec_pred_rank_2",
                                            "future_item_rank_base_1", "future_item_rank_base_2")))

featureNames <- c(setdiff(featureNames, c("predicted_future_item_rank_base_1", "predicted_future_item_rank_base_2")),
                    "predicted_future_item_rank_base_1", "predicted_future_item_rank_base_2")


if (includeFeatureNames[1] != "") {

    featureNames <- intersect(featureNames, c(includeFeatureNames))

}

if (excludeFeatureNames != "") {

    featureNames <- setdiff(featureNames, excludeFeatureNames)

}


featureNames <- intersect(featureNames, colnames(targetDT))

categoricalVariables <- intersect(featureNames, categoricalVariables)

if (length(categoricalVariables) == 0) {

    categoricalVariables <- NULL

}


trainTimeMin <- min(targetDT$timestamp_int)

validTimeMax <- max(targetDT[time_step %in% validTimesteps]$timestamp_int)

validTimeMin <- min(targetDT[time_step %in% validTimesteps]$timestamp_int)

trainTimeMax <- validTimeMin - 1

testTimeMax <- max(targetDT[time_step %in% testTimesteps]$timestamp_int)

testTimeMin <- min(targetDT[time_step %in% testTimesteps]$timestamp_int)


for (column in featureNames[grepl("time", featureNames) & !grepl("window|start", featureNames)]) {

    targetDT[, (column) := pmin(get(column), max(targetDT[timestamp_int <= validTimeMax - 1, c(column), with = FALSE], na.rm = TRUE))]

}


for (column in featureNames) {

    if (!is.numeric(targetDT[[column]]) | is.integer(targetDT[[column]])) {

        targetDT[[column]] <- as.numeric(targetDT[[column]])

    }

    targetDT[is.nan(get(column)), (column) :=   NA]

    targetDT[get(column) == Inf, (column)  :=   NA]

    targetDT[get(column) == -Inf, (column) :=   NA]

}


gc()

xValid <- targetDT[timestamp_int >= validTimeMin & timestamp_int <= validTimeMax, featureNames, with = FALSE]

yValid <- targetDT[timestamp_int >= validTimeMin & timestamp_int <= validTimeMax, target]

truePredValidDT <- data.table(targetDT[timestamp_int >= validTimeMin & timestamp_int <= validTimeMax,
                                c("session_id", "timestamp_int", "timestamp", "user_id", "user_session_number",
                                    if ("add_to_cart" %in% eventTypes) "session_number_of_add_to_carts" else character(0),
                                    "current_item_id", "future_item_id", "predicted_future_item_rank", "event", "future_event"), with = FALSE],
                                    true = yValid)

gc()

xTest <- targetDT[timestamp_int >= testTimeMin & timestamp_int <= testTimeMax, featureNames, with = FALSE]

yTest <- targetDT[timestamp_int >= testTimeMin & timestamp_int <= testTimeMax, target]

gc()

truePredTestDT <- data.table(targetDT[timestamp_int >= testTimeMin & timestamp_int <= testTimeMax,
                                c("session_id", "timestamp_int", "timestamp", "user_id", "user_session_number",
                                    if ("add_to_cart" %in% eventTypes) "session_number_of_add_to_carts" else character(0),
                                    "current_item_id", "future_item_id", "predicted_future_item_rank", "event", "future_event"), with = FALSE],
                                    true = yTest)

gc()

# A custom delete function can be used instead if there are problems with the working memory amount.
# The same applies to other filtering steps involving the assignment of a subset of rows.
targetDT <- targetDT[timestamp_int >= trainTimeMin & timestamp_int <= trainTimeMax]

gc()

yTrain <- targetDT$target

xTrain <- targetDT


rm(targetDT)

xTrain[, (setdiff(colnames(xTrain), featureNames)) := NULL]

setcolorder(xTrain, featureNames)

setcolorder(xValid, featureNames)

setcolorder(xTest, featureNames)

gc()

xTestMat <- as.matrix(xTest)

rm(xTest)

xValidMat <- as.matrix(xValid)

rm(xValid)

gc()

xTrainMat <- as.matrix(xTrain)

rm(xTrain)

gc()

if (!exists("reorderSeedVal")) {

    reorderSeedVal <- seedVal

}

gbmMaxIt <- 1

if (useGbmSeedAvg) {

    reorderSeedVal <- gbmSeedVec
    gbmMaxIt <- length(gbmSeedVec)

}

bestNumberOfTreesVec <- numeric(gbmMaxIt)

predictedValidList <- list(NULL)

predictedTestList <- list(NULL)


for (gbmIt in 1:gbmMaxIt) {

    if (gbmIt == 1) {

        set.seed(reorderSeedVal[gbmIt])

        neworder <- sample(1:nrow(xTrainMat), nrow(xTrainMat), replace = FALSE)

        xTrainMat <- xTrainMat[neworder,]

        yTrain <- yTrain[neworder]

        set.seed(reorderSeedVal[gbmIt])

        neworder <- sample(1:nrow(xValidMat), nrow(xValidMat), replace = FALSE)

        xValidMat <- xValidMat[neworder,]

        yValid <- yValid[neworder]

        truePredValidDT <- truePredValidDT[neworder,]

        set.seed(reorderSeedVal[gbmIt])

        neworder <- sample(1:nrow(xTestMat), nrow(xTestMat), replace = FALSE)

        xTestMat <- xTestMat[neworder,]

        yTest <- yTest[neworder]

        truePredTestDT <- truePredTestDT[neworder,]

    }

    gc()

    dtrain <- lgb.Dataset(xTrainMat, label = yTrain, categorical_feature = categoricalVariables)

    dvalid <- lgb.Dataset(xValidMat, label = yValid, categorical_feature = categoricalVariables)

    dtest <- lgb.Dataset(xTestMat, categorical_feature = categoricalVariables)

    valids <- list(test = dvalid)

    set.seed(gbmSeedVec[gbmIt])

    params$seed <- gbmSeedVec[gbmIt]

    lgbmModel <-
        lgb.train(params = params, dtrain, nrounds = max_rounds, valids = valids, early_stopping_rounds = lgbm_early_stopping_rounds_val, verbose = -1)

    bestNumberOfTrees <- lgbmModel$best_iter

    bestNumberOfTreesVec[gbmIt] <- bestNumberOfTrees

    predictedValid <- predict(lgbmModel, xValidMat, num_iteration = bestNumberOfTrees)

    predictedTest <- predict(lgbmModel, xTestMat, num_iteration = bestNumberOfTrees)

    predictedValidList[[gbmIt]] <- predictedValid

    predictedTestList[[gbmIt]] <- predictedTest

    gc()

}

gc()

if (useGbmSeedAvg) {

    predictedValid <- Reduce("+", predictedValidList) / length(predictedValidList)

    predictedTest <- Reduce("+", predictedTestList) / length(predictedTestList)

}


truePredValidDT[, pred := predictedValid]

truePredTestDT[, pred := predictedTest]

try(rm(xTrainMat)); gc();


truePredValidDT[, gbm_pred_rank := rank(-pred, ties.method = "first"), by = c("session_id", "timestamp_int", "current_item_id")]

truePredTestDT[, gbm_pred_rank := rank(-pred, ties.method = "first"), by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeRecsValidDT <- truePredValidDT[, list(future_item_id = future_item_id[match(1:.N, gbm_pred_rank)],
                                                    gbm_pred_rank = gbm_pred_rank[match(1:.N, gbm_pred_rank)]),
                                                    by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeRecsTestDT <- truePredTestDT[, list(future_item_id = future_item_id[match(1:.N, gbm_pred_rank)],
                                                    gbm_pred_rank = gbm_pred_rank[match(1:.N, gbm_pred_rank)]),
                                                    by = c("session_id", "timestamp_int", "current_item_id")]

rm(xValidMat, xTestMat); gc()

setnames(baseRecDT, "predicted_future_item_rank", "base_rec_pred_rank")

baseRecDT[, is_in_base_rec_recs := 1]

gc()

sessionTimeRecsValidDT <- merge(sessionTimeRecsValidDT, baseRecDT,
                                by = c("session_id", "timestamp_int", "current_item_id", "future_item_id"),
                                all.x = TRUE, all.y = FALSE, sort = FALSE)

sessionTimeRecsValidDT[is.na(is_in_base_rec_recs), is_in_base_rec_recs := 0]

sessionTimeRecsValidDT[, number_of_observations := .N, by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeRecsValidDT[, sum_is_in_base_rec_recs := sum(is_in_base_rec_recs), by = c("session_id", "timestamp_int", "current_item_id")]

setorder(sessionTimeRecsValidDT, session_id, timestamp_int, current_item_id, gbm_pred_rank)

gc()

sessionTimeRecsTestDT <- merge(sessionTimeRecsTestDT, baseRecDT,
                               by = c("session_id", "timestamp_int", "current_item_id", "future_item_id"),
                               all.x = TRUE, all.y = FALSE, sort = FALSE)

sessionTimeRecsTestDT[is.na(is_in_base_rec_recs), is_in_base_rec_recs := 0]

sessionTimeRecsTestDT[, number_of_observations := .N, by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeRecsTestDT[, sum_is_in_base_rec_recs := sum(is_in_base_rec_recs), by = c("session_id", "timestamp_int", "current_item_id")]

setorder(sessionTimeRecsTestDT, session_id, timestamp_int, current_item_id, gbm_pred_rank)

rm(baseRecDT); gc()


sessionTimeTrueValidDT <- truePredValidDT[, list(future_item_id = future_item_id[true == 1]),
                                            by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeTrueValidDT[, number_of_observations := .N, by = c("session_id", "timestamp_int", "current_item_id")]

if (grepl("current_item_id != future_item_id", sessionTimeRecsFilterExpression)) {

    sessionTimeTrueValidDT <- sessionTimeTrueValidDT[current_item_id != future_item_id]

}


setorder(sessionTimeTrueValidDT, session_id, timestamp_int, current_item_id, future_item_id)

sessionTimeTrueTestDT <- truePredTestDT[, list(future_item_id = future_item_id[true == 1]), by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeTrueTestDT[, number_of_observations := .N, by = c("session_id", "timestamp_int", "current_item_id")]

if (grepl("current_item_id != future_item_id", sessionTimeRecsFilterExpression)) {

    sessionTimeTrueTestDT <- sessionTimeTrueTestDT[current_item_id != future_item_id]

}

setorder(sessionTimeTrueTestDT, session_id, timestamp_int, current_item_id, future_item_id)


sessionTimeRecsValidFilteredDT <- sessionTimeRecsValidDT[eval(sessionTimeRecsFilterExpression)]

sessionTimeRecsValidFilteredDT[, number_of_observations_after_filter := .N, by = c("session_id", "timestamp_int", "current_item_id")]

eval(parse(text = paste0("setorder(sessionTimeRecsValidFilteredDT, session_id, timestamp_int, current_item_id, ", "gbm_pred_rank", ")")))

gbmRecsValidAggDT <-
    sessionTimeRecsValidFilteredDT[!is.na(gbm_pred_rank),
                                    list(gbm_rec_future_item_list_str = paste0("c(", paste0(future_item_id, collapse = ","), paste0(")")),
                                            gbm_number_of_recs = min(number_of_observations)),
                                    by = c("session_id", "timestamp_int", "current_item_id")]

eval(parse(text = paste0("setorder(sessionTimeRecsValidFilteredDT, session_id, timestamp_int, current_item_id, ", "base_rec_pred_rank", ")")))

baseRecRecsValidAggDT <-
    sessionTimeRecsValidFilteredDT[!is.na(base_rec_pred_rank),
                                    list(base_rec_rec_future_item_list_str = paste0("c(", paste0(future_item_id, collapse = ","), paste0(")")),
                                         base_rec_number_of_recs = min(number_of_observations)),
                                  by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeRecsValidAggDT <- merge(gbmRecsValidAggDT, baseRecRecsValidAggDT,
                                    by = c("session_id", "timestamp_int", "current_item_id"), all = FALSE, sort = FALSE)

sessionTimeTrueValidAggDT <-
    sessionTimeTrueValidDT[, list(true_next_future_item_list_str = paste0("c(", paste0(future_item_id, collapse = ","), paste0(")")),
                                    number_of_next_true = min(number_of_observations)),
                            by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeRecsTrueValidDT <- merge(sessionTimeRecsValidAggDT, sessionTimeTrueValidAggDT,
                                    by = c("session_id", "timestamp_int", "current_item_id"), all = TRUE, sort = FALSE)

sessionTimeRecsTrueValidDT[is.na(true_next_future_item_list_str), number_of_next_true := 0]

if (!exists("evalUseAllTrueItemsAfterNext")) {

    evalUseAllTrueItemsAfterNext <- TRUE

}


if (!evalUseAllTrueItemsAfterNext) {

    sessionTimeRecsTrueValidDT[, true_future_item_list_str := true_next_future_item_list_str]

    sessionTimeRecsTrueValidDT[, number_of_true := number_of_next_true]

} else {

    uniquePasteCollapse <- function(x_val) {paste0("c(", paste0(unique(x_val), collapse = ","), ")")}

    clickstreamEventsDT[, current_item_id := item_id]

    sortedClickstreamEventsDT <- setorder(clickstreamEventsDT[, list(session_id, timestamp_int, current_item_id, event)],
                                            "session_id", "timestamp_int", "current_item_id")

    remainingItemsPerSessionDT <-
        sortedClickstreamEventsDT[, list(session_id, timestamp_int, timestamp_int_x = timestamp_int, event_x = event,
                                        item_id = current_item_id)][sortedClickstreamEventsDT[,
                                            list(session_id, timestamp_int, timestamp_int_i = timestamp_int, event_i = event, current_item_id)],
                                            , on = .(session_id, timestamp_int > timestamp_int)]

    remainingItemsPerSessionDT <- remainingItemsPerSessionDT[event_x %in% targetEventsVec]


    remainingItemsPerSessionDT <- remainingItemsPerSessionDT[item_id != current_item_id]

    remainingItemsPerSessionDT <- remainingItemsPerSessionDT[!is.na(item_id)]

    remainingItemsPerSessionAggDT <- remainingItemsPerSessionDT[, list(true_future_item_list_str = uniquePasteCollapse(item_id),
                                                                        number_of_true = length(unique(item_id))),
                                                                by = c("session_id", "timestamp_int", "current_item_id")]

    sessionTimeRecsTrueValidDT <- merge(sessionTimeRecsTrueValidDT, remainingItemsPerSessionAggDT,
                                        by = c("session_id", "timestamp_int", "current_item_id"), all.x = TRUE, all.y = FALSE, sort = FALSE)

    sessionTimeRecsTrueValidDT[is.na(true_future_item_list_str), number_of_true := 0]

}

sessionTimeRecsTrueValidFilteredDT <- sessionTimeRecsTrueValidDT[gbm_number_of_recs >= numberOfTrainingItemsPerSession - 10 & number_of_true > 0]


sessionTimeRecsTestFilteredDT <- sessionTimeRecsTestDT[eval(sessionTimeRecsFilterExpression)]

sessionTimeRecsTestFilteredDT[, number_of_observations_after_filter := .N, by = c("session_id", "timestamp_int", "current_item_id")]

eval(parse(text = paste0("setorder(sessionTimeRecsTestFilteredDT, session_id, timestamp_int, current_item_id, ", "gbm_pred_rank", ")")))

gbmRecsTestAggDT <-
    sessionTimeRecsTestFilteredDT[!is.na(gbm_pred_rank),
                                    list(gbm_rec_future_item_list_str = paste0("c(", paste0(future_item_id, collapse = ","), paste0(")")),
                                            gbm_number_of_recs = min(number_of_observations)),
                                    by = c("session_id", "timestamp_int", "current_item_id")]

eval(parse(text = paste0("setorder(sessionTimeRecsTestFilteredDT, session_id, timestamp_int, current_item_id, ", "base_rec_pred_rank", ")")))

baseRecRecsTestAggDT <-
    sessionTimeRecsTestFilteredDT[!is.na(base_rec_pred_rank),
                                    list(base_rec_rec_future_item_list_str = paste0("c(", paste0(future_item_id, collapse = ","), paste0(")")),
                                         base_rec_number_of_recs = min(number_of_observations)),
                                  by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeRecsTestAggDT <- merge(gbmRecsTestAggDT, baseRecRecsTestAggDT,
                                    by = c("session_id", "timestamp_int", "current_item_id"), all = FALSE, sort = FALSE)

sessionTimeTrueTestAggDT <-
    sessionTimeTrueTestDT[, list(true_next_future_item_list_str = paste0("c(", paste0(future_item_id, collapse = ","), paste0(")")),
                                    number_of_next_true = min(number_of_observations)),
                            by = c("session_id", "timestamp_int", "current_item_id")]

sessionTimeRecsTrueTestDT <- merge(sessionTimeRecsTestAggDT, sessionTimeTrueTestAggDT,
                                    by = c("session_id", "timestamp_int", "current_item_id"), all = TRUE, sort = FALSE)

sessionTimeRecsTrueTestDT[is.na(true_next_future_item_list_str), number_of_next_true := 0]

if (!evalUseAllTrueItemsAfterNext) {

    sessionTimeRecsTrueTestDT[, true_future_item_list_str := true_next_future_item_list_str]

    sessionTimeRecsTrueTestDT[, number_of_true := number_of_next_true]

} else {

    uniquePasteCollapse <- function(x_val) {paste0("c(", paste0(unique(x_val), collapse = ","), ")")}

    sortedClickstreamEventsDT <- setorder(clickstreamEventsDT[, list(session_id, timestamp_int, current_item_id, event)],
                                            "session_id", "timestamp_int", "current_item_id")

    remainingItemsPerSessionDT <-
        sortedClickstreamEventsDT[, list(session_id, timestamp_int, timestamp_int_x = timestamp_int, event_x = event,
                                        item_id = current_item_id)][sortedClickstreamEventsDT[,
                                            list(session_id, timestamp_int, timestamp_int_i = timestamp_int, event_i = event, current_item_id)],
                                            , on = .(session_id, timestamp_int > timestamp_int)]

    remainingItemsPerSessionDT <- remainingItemsPerSessionDT[event_x %in% targetEventsVec]


    remainingItemsPerSessionDT <- remainingItemsPerSessionDT[item_id != current_item_id]

    remainingItemsPerSessionDT <- remainingItemsPerSessionDT[!is.na(item_id)]

    remainingItemsPerSessionAggDT <- remainingItemsPerSessionDT[, list(true_future_item_list_str = uniquePasteCollapse(item_id),
                                                                        number_of_true = length(unique(item_id))),
                                                                by = c("session_id", "timestamp_int", "current_item_id")]

    sessionTimeRecsTrueTestDT <- merge(sessionTimeRecsTrueTestDT, remainingItemsPerSessionAggDT,
                                        by = c("session_id", "timestamp_int", "current_item_id"), all.x = TRUE, all.y = FALSE, sort = FALSE)

    sessionTimeRecsTrueTestDT[is.na(true_future_item_list_str), number_of_true := 0]

}

sessionTimeRecsTrueTestFilteredDT <- sessionTimeRecsTrueTestDT[gbm_number_of_recs >= numberOfTrainingItemsPerSession - 10 & number_of_true > 0]

rm(sessionTimeRecsTrueValidDT, sessionTimeTrueValidAggDT, sessionTimeRecsValidAggDT, sessionTimeRecsValidDT, sessionTimeRecsValidFilteredDT,
    sessionTimeRecsTrueValidFilteredDT, sessionTimeRecsTrueTestDT, sessionTimeTrueTestAggDT, sessionTimeRecsTestAggDT, sessionTimeRecsTestDT,
    sessionTimeRecsTestFilteredDT); gc()


rec_gbm_List <- as.list(sessionTimeRecsTrueTestFilteredDT$gbm_rec_future_item_list_str)

true_List <- as.list(sessionTimeRecsTrueTestFilteredDT$true_future_item_list_str)

evalParse <- function(x) {eval(parse(text = x))}

rec_gbm_List <- lapply(rec_gbm_List, evalParse)

true_List <- lapply(true_List, evalParse)


base_s_sknn_DT <- readRDS(paste0(cachePath, "baseRecDT", "_",
                                    ifelse(grepl("s_sknn", baseRecFileSuffix_1), baseRecFileSuffix_1, baseRecFileSuffix_2), ".rds"))

try(setnames(base_s_sknn_DT, "future_item_rank", "predicted_future_item_rank", skip_absent = TRUE))

try(setnames(base_s_sknn_DT, "future_item_score", "predicted_future_item_score", skip_absent = TRUE))

base_s_sknn_DT <- merge(sessionTimeRecsTrueTestFilteredDT[, c("session_id", "timestamp_int", "current_item_id"), with = FALSE], base_s_sknn_DT,
                        by = c("session_id", "timestamp_int", "current_item_id"), all.x = TRUE, all.y = FALSE, sort = TRUE)

base_gru4rec_DT <- readRDS(paste0(cachePath, "baseRecDT", "_",
                                    ifelse(grepl("gru4rec", baseRecFileSuffix_1), baseRecFileSuffix_1, baseRecFileSuffix_2), ".rds"))

try(setnames(base_gru4rec_DT, "future_item_rank", "predicted_future_item_rank", skip_absent = TRUE))

try(setnames(base_gru4rec_DT, "future_item_score", "predicted_future_item_score", skip_absent = TRUE))

base_gru4rec_DT <- merge(sessionTimeRecsTrueTestFilteredDT[, c("session_id", "timestamp_int", "current_item_id"), with = FALSE],
                            base_gru4rec_DT, by = c("session_id", "timestamp_int", "current_item_id"), all.x = TRUE, all.y = FALSE, sort = TRUE)

base_s_sknn_DT <- base_s_sknn_DT[current_item_id != future_item_id]

base_gru4rec_DT <- base_gru4rec_DT[current_item_id != future_item_id]

recs_s_sknn_sessionTimeTrueTestFilteredDT <-
    base_s_sknn_DT[!is.na(predicted_future_item_rank),
                    list(s_sknn_future_item_list_str = paste0("c(", paste0(future_item_id, collapse = ","), paste0(")"))),
                        by = c("session_id", "timestamp_int", "current_item_id")]

recs_gru4rec_sessionTimeTrueTestFilteredDT <-
    base_gru4rec_DT[!is.na(predicted_future_item_rank),
                    list(gru4rec_future_item_list_str = paste0("c(", paste0(future_item_id, collapse = ","), paste0(")"))),
                        by = c("session_id", "timestamp_int", "current_item_id")]

rec_s_sknn_List <- as.list(recs_s_sknn_sessionTimeTrueTestFilteredDT$s_sknn_future_item_list_str)

rec_gru4rec_List <- as.list(recs_gru4rec_sessionTimeTrueTestFilteredDT$gru4rec_future_item_list_str)

rec_s_sknn_List <- lapply(rec_s_sknn_List, evalParse)

rec_gru4rec_List <- lapply(rec_gru4rec_List, evalParse)


rm(base_s_sknn_DT, base_gru4rec_DT, recs_s_sknn_sessionTimeTrueTestFilteredDT, recs_gru4rec_sessionTimeTrueTestFilteredDT, sessionTimeRecsTrueTestFilteredDT); gc()

try(source(paste0(codePath, link_hysar_gbm_evaluate_results_r)))


#______________________________
# josef.b.bauer at gmail.com

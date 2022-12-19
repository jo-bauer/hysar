
# Assumes that results by the GBM model and the baselines are existing and loaded already, therefore the previous modules have to be run first.

targetMetric <- calcMRR

res_gbm_metric_Vec <- rep(-1, length(rec_gbm_List))
res_s_sknn_metric_Vec <- rep(-1, length(rec_gbm_List))
res_gru4rec_metric_Vec <- rep(-1, length(rec_gbm_List))

for (ind in 1:length(rec_gbm_List)) {

    res_gbm_metric_Vec[ind] <- targetMetric(predictedItemsSortedVec = unlist(rec_gbm_List[[ind]]), trueItemsVec = unlist(true_List[[ind]]))
    res_s_sknn_metric_Vec[ind] <- targetMetric(predictedItemsSortedVec = unlist(rec_s_sknn_List[[ind]]), trueItemsVec = unlist(true_List[[ind]]))
    res_gru4rec_metric_Vec[ind] <- targetMetric(predictedItemsSortedVec = unlist(rec_gru4rec_List[[ind]]), trueItemsVec = unlist(true_List[[ind]]))

}

evalResDT <- data.table(
    metric = "mrr",
    gbm_rec_result = mean(res_gbm_metric_Vec),
    s_sknn_result = mean(res_s_sknn_metric_Vec),
    gru4rec_result = mean(res_gru4rec_metric_Vec)
)

if (storeResults) {
    fwrite(evalResDT, paste0(resultsPath, "rec_evaluation_result", "_", extendedFileSuffix, "_", experimentId, ".csv"), sep = ";")
}


rm(list=ls()); gc()


#______________________________
# josef.b.bauer at gmail.com

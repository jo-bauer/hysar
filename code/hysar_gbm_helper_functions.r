
cumSumCond = cppFunction("
NumericVector cumSumCond(NumericVector targetVec, NumericVector condVec) {
    NumericVector runningVec(targetVec.size());
    for (int ind = 0; ind < targetVec.size(); ++ind) {
        runningVec[ind] = 0;
        for (int cond_ind = ind; cond_ind >= 0; --cond_ind) {
            if (condVec[cond_ind] != condVec[ind]) {
                runningVec[ind] += targetVec[cond_ind];
            }
        }
    }
    return runningVec;
}")

cumSumNAIgnore = cppFunction("
NumericVector cumSumNAIgnore(NumericVector targetVec) {
    NumericVector runningVec(targetVec.size());
    if (R_IsNA(targetVec[0])) {
        runningVec[0] = 0;
    } else {
        runningVec[0] = targetVec[0];
    }
    if (targetVec.size() > 0) {
        for (int ind = 1; ind < targetVec.size(); ind++) {
            if (!(R_IsNA(targetVec[ind]))) {
                runningVec[ind] = runningVec[ind-1] + targetVec[ind];
            } else {
                runningVec[ind] = runningVec[ind-1];
            }
        }
    }
    return runningVec;
}")

timeSinceUnequalCond = cppFunction("
NumericVector timeSinceUnequalCond(NumericVector timeVec, NumericVector condVec, double NAval = -1000000) {
    NumericVector timeDiffVec(timeVec.size());
    for (int ind = 0; ind < timeVec.size(); ++ind) {
    timeDiffVec[ind] = NAval;
        for (int cond_ind = ind; cond_ind >= 0; --cond_ind) {
            if (condVec[cond_ind] != condVec[ind]) {
                timeDiffVec[ind] = timeVec[ind] - timeVec[cond_ind];
                break;
            }
        }
    }
    return timeDiffVec;
}")

calcWindowCount = cppFunction("
NumericVector calcWindowCount(NumericVector targetVec, NumericVector currentTimestampVec,
                                NumericVector previousTimestampVec, double timeStampDiffThreshold) {
    NumericVector runningVec(targetVec.size());
    for (int ind = 0; ind < targetVec.size(); ++ind) {
        runningVec[ind] = 0;
        for (int prev_ind = ind; prev_ind >= 0; --prev_ind) {
            if (currentTimestampVec[ind] - previousTimestampVec[prev_ind] <= timeStampDiffThreshold) {
                runningVec[ind] += targetVec[prev_ind];
            }
            else {
                break;
            }
        }
    }
    return runningVec;
}")

calcAdjustedLogLoss <- function (yPred, yTrue, aWeightFalseNegative = 1, bWeightFalsePositive = 1) {

    yPred <- pmin(pmax(yPred, 0.000001), 0.999999)

    weightedLogLossValue <- -mean(aWeightFalseNegative * yTrue * log(yPred) + bWeightFalsePositive * (1 - yTrue) * log(1 - yPred))

    return(weightedLogLossValue)

}

calcMRR <- function(predictedItemsSortedVec, trueItemsVec) {

    mrrVal <- 1/suppressWarnings(min(match(trueItemsVec, predictedItemsSortedVec), na.rm = TRUE))
    mrrVal <- ifelse(is.na(mrrVal), 0, mrrVal)
    mrrVal

}


#______________________________
# josef.b.bauer at gmail.com

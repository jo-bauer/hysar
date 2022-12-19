
clickstreamEventsDT <- readRDS(paste0(cachePath, cacheNameClickstreamEventsDT, "_", extendedFileSuffix, ".rds"))

if (dataset == "retailrocket") {

    itemPropertiesNumericWideDT <- readRDS(paste0(cachePath, "itemPropertiesNumericWideDT", "_", extendedFileSuffix, ".rds"))

    itemPropertiesCategoricalWideDT <- readRDS(paste0(cachePath, "itemPropertiesCategoricalWideDT", "_", extendedFileSuffix, ".rds"))

}

if (dataset == "diginetica") {

    itemPropertiesDT <- readRDS(paste0(cachePath, "itemPropertiesDT", "_", extendedFileSuffix, ".rds"))

}

if (file.exists(paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_1, ".rds"))
    & file.exists(paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_2, ".rds")) & useCachedBaseRec) {

    baseRecDT <- readRDS(paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_1, ".rds"))

    try(setnames(baseRecDT, "from_item_id", "current_item_id", skip_absent = TRUE))

    baseRec2DT <- readRDS(paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_2, ".rds"))

    try(setnames(baseRec2DT, "from_item_id", "current_item_id", skip_absent = TRUE))

} else {

    baseRecDT <- fread(paste0(firstPhaseOutputPath, baseRecFileName, ".csv"), sep = ";")

    setcolorder(baseRecDT, c("time_step", "timestamp_int", "session_id", "current_item_id", "future_item_id"))

    baseRecDT[, future_item_rank := 1:.N, by = c("time_step", "timestamp_int", "session_id", "current_item_id")]

    saveRDS(baseRecDT, paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_1, ".rds"))

    baseRec2DT <- fread(paste0(firstPhaseOutputPath, baseRecFileName_2, ".csv"), sep = ";")

    setcolorder(baseRec2DT, c("time_step", "timestamp_int", "session_id", "current_item_id", "future_item_id"))

    baseRec2DT[, future_item_rank := 1:.N, by = c("time_step", "timestamp_int", "session_id", "current_item_id")]

    saveRDS(baseRec2DT, paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_2, ".rds"))

}


baseRecDT <- baseRecDT[, list(future_item_rank = min(future_item_rank)),
                            by = c("session_id", "timestamp_int", "current_item_id", "future_item_id", "time_step")]
gc()

baseRecDT_o <- copy(baseRecDT)

baseRecDT <- baseRecDT[time_step %in% timestepSubset]

baseRec2DT <- baseRec2DT[, list(future_item_rank = min(future_item_rank)),
                            by = c("session_id", "timestamp_int", "current_item_id", "future_item_id", "time_step")]
gc()

baseRec2DT <- baseRec2DT[time_step %in% timestepSubset]


if (!file.exists(paste0(cachePath, baseRecCombDTFilename)) | !useCachedBaseRec) {

    baseRecDT[, future_item_rank_base_1 := future_item_rank]

    baseRec2DT[, future_item_rank_base_2 := future_item_rank]

    baseRec1DT <- copy(baseRecDT)

    baseRecDT <- merge(baseRecDT, baseRec2DT[, -c("future_item_rank"), with = FALSE],
                        by = c("time_step", "timestamp_int", "session_id", "current_item_id", "future_item_id"), all = TRUE, sort = FALSE)

    baseRecDT[is.na(future_item_rank_base_1), future_item_rank_base_1 := numberOfTrainingItemsPerSession + 1]

    baseRecDT[is.na(future_item_rank_base_2), future_item_rank_base_2 := numberOfTrainingItemsPerSession + 1]

    baseRecDT[, future_item_score := future_item_rank_base_1 + future_item_rank_base_2]

    baseRecDT[, future_item_rank := rank(future_item_score, ties.method = "first"),
                by = c("time_step", "timestamp_int", "session_id", "current_item_id")]

    baseRecDT <- baseRecDT[future_item_rank <= numberOfTrainingItemsPerSession]

    setorder(baseRecDT, session_id, timestamp_int, current_item_id, future_item_rank)

    baseRecDT <- baseRecDT[time_step %in% timestepSubset]

    saveRDS(baseRecDT, paste0(cachePath, baseRecCombDTFilename))

} else {

    baseRecDT <- readRDS(paste0(cachePath, baseRecCombDTFilename))

}

baseRecDT[, future_item_rank_base_1 := NULL]
baseRecDT[, future_item_rank_base_2 := NULL]
baseRecDT[, future_item_score := NULL]


timestamp_timestep_DT <- unique(baseRecDT[, c("timestamp_int", "time_step")])

setcolorder(baseRecDT, c(colnames(baseRecDT_o), setdiff(colnames(baseRecDT), colnames(baseRecDT_o))))

rm(baseRecDT_o, baseRec1DT, baseRec2DT)
gc()

clickstreamEventsDT <-
    clickstreamEventsDT[, c("timestamp", "timestamp_int", "user_id", "item_id", "session_id",
                            "user_session_number", "event", "diff_time"), with = FALSE]

clickstreamEventsDT[, lookahead_observations := as.integer(pmin((.N:1) - 1L, 1)), by = "session_id"]

clickstreamEventsExpandedDT <- clickstreamEventsDT[, list(lookahead_observations = if (lookahead_observations != 0) seq_len(lookahead_observations)
                                                                                   else lookahead_observations),
                                                            by = c(names(clickstreamEventsDT)[-ncol(clickstreamEventsDT)])]


clickstreamEventsExpandedDT[, timestep_number := cumSumNAIgnore(!duplicated(timestamp_int)) - 1, by = "session_id"]

clickstreamEventsExpandedDT[, future_item_ind := ifelse(lookahead_observations > 0, lookahead_observations + timestep_number, 0), by = "session_id"]

futureItemsEventsDT <-
    unique(clickstreamEventsExpandedDT[timestep_number > 0,
                                        list(session_id, future_item_id = item_id, future_event = event, future_item_ind = timestep_number)])

clickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT, futureItemsEventsDT,
                                        by = c("session_id", "future_item_ind"), all.x = TRUE, all.y = FALSE, sort = FALSE)

rm(futureItemsEventsDT)

clickstreamEventsExpandedDT <-
    clickstreamEventsExpandedDT[!duplicated(clickstreamEventsExpandedDT[, list(session_id, timestamp_int, item_id, lookahead_observations,
                                                                                future_item_id, future_event)])]

clickstreamEventsExpandedDT <- clickstreamEventsExpandedDT[!is.na(future_item_id)]

clickstreamEventsExpandedDT[, future_event_rank := ifelse(future_event == "buy", 3, ifelse(future_event == "add_to_cart", 2, 1))]

clickstreamEventsExpandedDT <- clickstreamEventsExpandedDT[clickstreamEventsExpandedDT[, .I[which.max(future_event_rank)],
                                                            by = c("session_id", "timestamp_int", "item_id", "future_item_id")]$V1]

clickstreamEventsExpandedDT[, current_item_id := item_id]


clickstreamEventsExpandedDT[, number_of_items_to_add := numberOfTrainingItemsPerSession, by = c("session_id", "timestamp_int", "current_item_id")]

clickstreamEventsExtendedBaseRecDT <- clickstreamEventsExpandedDT[number_of_items_to_add > 0]

if (sampleBySession) {

    uniqueKeysDT <- unique(clickstreamEventsExtendedBaseRecDT[, c("session_id", "timestamp_int", "current_item_id"), with = FALSE])

    if ((!evalUseAllTrueItemsAfterNext) | targetCase == "views") {

        set.seed(seedVal)

        retainKeysDT <- uniqueKeysDT[uniqueKeysDT[, .I[sample(1:.N, size = min(.N, sampleBySessionSize), replace = FALSE)], by = c("session_id")]$V1]

    } else {

        uniqueKeysDT <- unique(clickstreamEventsExtendedBaseRecDT[, c("session_id", "timestamp_int", "current_item_id", "event"), with = FALSE])

        uniqueKeysDT[, step_number := 1:.N, by = c("session_id")]

        uniqueKeysDT[, max_step_number_by_session := max(step_number), by = c("session_id")]

        uniqueKeysDT[, first_step_number_add_to_cart_buy := min(step_number[event %in% targetEventsVec]), by = c("session_id")]

        uniqueKeysDT[!is.finite(first_step_number_add_to_cart_buy) | is.na(first_step_number_add_to_cart_buy),
                        first_step_number_add_to_cart_buy := max_step_number_by_session]

        retainKeysDT <- uniqueKeysDT[step_number == pmax(first_step_number_add_to_cart_buy - 1, 1)]

    }

    try(retainKeysDT[, max_step_number_by_session := NULL])

    try(retainKeysDT[, first_step_number_add_to_cart_buy := NULL])

    clickstreamEventsExtendedBaseRecDT <- merge(clickstreamEventsExtendedBaseRecDT, retainKeysDT,
                                                by = c("session_id", "timestamp_int", "current_item_id"), all = FALSE)

    rm(uniqueKeysDT, retainKeysDT); gc()

}


set.seed(seedVal)
clickstreamEventsExtendedBaseRecDT <-
    clickstreamEventsExtendedBaseRecDT[clickstreamEventsExtendedBaseRecDT[, .I[sample(1:.N, size = number_of_items_to_add, replace = TRUE)],
                                                by = c("session_id", "timestamp_int", "current_item_id")]$V1]

clickstreamEventsExtendedBaseRecDT[, future_item_id := NULL]

clickstreamEventsExtendedBaseRecDT[, future_event := "non-view"]

clickstreamEventsExtendedBaseRecDT[, future_event_rank := 0]

clickstreamEventsExtendedBaseRecDT[, future_item_rank := 1:.N, by = c("session_id", "timestamp_int", "current_item_id")]

gc()


clickstreamEventsExtendedBaseRecDT <- merge(clickstreamEventsExtendedBaseRecDT, baseRecDT,
                                            by = c("session_id", "timestamp_int", "current_item_id", "future_item_rank"), all.x = FALSE, all.y = FALSE,
                                            sort = FALSE)

setorder(clickstreamEventsExtendedBaseRecDT, user_id, session_id, timestamp_int, item_id, future_item_rank)

setcolorder(clickstreamEventsExtendedBaseRecDT, c(intersect(colnames(clickstreamEventsExpandedDT), colnames(clickstreamEventsExtendedBaseRecDT)),
                                                        setdiff(colnames(clickstreamEventsExtendedBaseRecDT), colnames(clickstreamEventsExpandedDT))))

clickstreamEventsExtendedBaseRecDT <- clickstreamEventsExtendedBaseRecDT[!is.na(future_item_id)]

clickstreamEventsExtendedBaseRecDT <-
    clickstreamEventsExtendedBaseRecDT[!duplicated(clickstreamEventsExtendedBaseRecDT[, list(session_id, timestamp_int, current_item_id, future_item_rank)])]

clickstreamEventsExtendedBaseRecDT[, future_item_rank := NULL]

clickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT, timestamp_timestep_DT, by = c("timestamp_int"), all.x = TRUE, all.y = FALSE)

clickstreamEventsExpandedDT <- rbind(clickstreamEventsExpandedDT, clickstreamEventsExtendedBaseRecDT, fill = TRUE)

clickstreamEventsExpandedDT <-
        clickstreamEventsExpandedDT[clickstreamEventsExpandedDT[, .I[which.max(future_event_rank)],
                                    by = c("session_id", "timestamp_int", "item_id", "future_item_id")]$V1]


clickstreamEventsExpandedDT[, (c("lookahead_observations", "timestep_number", "number_of_items_to_add")) := NULL]

rm(clickstreamEventsExtendedBaseRecDT, baseRecDT, timestamp_timestep_DT)

gc()

clickstreamEventsExpandedDT[is.na(event), event := event.y]


setkeyv(clickstreamEventsDT, c("timestamp_int", "user_id", "item_id"))

setkeyv(clickstreamEventsExpandedDT, c("timestamp_int", "user_id", "item_id"))

eventTypes <- unique(clickstreamEventsDT$event)

clickstreamEventsDT[, session_time_since_start := as.numeric(timestamp_int - min(timestamp_int)), by = "session_id"]

if (dataset == "retailrocket") {

    clickstreamEventsDT[, session_interaction_number := 1:.N, by = "session_id"]

}

if (dataset == "diginetica") {

    clickstreamEventsDT[, session_interaction_number := 1:.N, by = c("user_id", "session_id")]

}


for (eventVal in eventTypes) {

    clickstreamEventsDT[, (paste0("session_number_of_", eventVal, "s")) :=
                            ifelse(rep(.N > 1, .N), c(0, cumSumNAIgnore(ifelse(event == eventVal, 1, 0))[1:(.N - 1)]), 0), by = "session_id"]

}


if (dataset == "retailrocket") {

    clickstreamEventsDT <-
        itemPropertiesNumericWideDT[, c("item_id", "timestamp_int", numericItemPropertiesNames), with = FALSE][clickstreamEventsDT,
                                                                    , on = c("item_id" = "item_id", "timestamp_int" = "timestamp_int"), roll = Inf]

    clickstreamEventsDT <-
        itemPropertiesCategoricalWideDT[, c("item_id", "timestamp_int", categoricalItemPropertiesNames), with = FALSE][clickstreamEventsDT,
                                                                    , on = c("item_id" = "item_id", "timestamp_int" = "timestamp_int"), roll = Inf]

    clickstreamEventsDT[, available_ind := as.numeric(available)]

}

if (dataset == "diginetica") {

    clickstreamEventsDT <- merge(clickstreamEventsDT, itemPropertiesDT, by = c("item_id"), all.x = TRUE, all.y = FALSE, sort = FALSE)

}


addBaseFeatNumericNames <- setdiff(addBaseFeatNames, "category_id")

addBaseFeatCatNames <- c("category_id")

setnames(clickstreamEventsDT, addBaseFeatNames, c(paste0("current_item_", addBaseFeatNumericNames), paste0("current_", addBaseFeatCatNames)))

clickstreamEventsDT[, current_item_id := item_id]


if (dataset == "retailrocket") {

    setnames(itemPropertiesNumericWideDT, "item_id", "future_item_id")

    clickstreamEventsExpandedDT <-
        itemPropertiesNumericWideDT[, c("future_item_id", "timestamp_int", numericItemPropertiesNames), with = FALSE][clickstreamEventsExpandedDT,
                                                        , on = c("future_item_id" = "future_item_id", "timestamp_int" = "timestamp_int"), roll = Inf]

    setnames(itemPropertiesCategoricalWideDT, "item_id", "future_item_id")

    clickstreamEventsExpandedDT <-
        itemPropertiesCategoricalWideDT[, c("future_item_id", "timestamp_int", categoricalItemPropertiesNames),
                                            with = FALSE][clickstreamEventsExpandedDT,
                                            , on = c("future_item_id" = "future_item_id", "timestamp_int" = "timestamp_int"), roll = Inf]

    clickstreamEventsExpandedDT[, available_ind := as.numeric(available)]

}

if (dataset == "diginetica") {

    itemPropertiesDT[, future_item_id := item_id]
    itemPropertiesDT[, item_id := NULL]

    clickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT, itemPropertiesDT, by = c("future_item_id"), all.x = TRUE, all.y = FALSE,
                                            sort = FALSE)

}


setnames(clickstreamEventsExpandedDT, addBaseFeatNames, c(paste0("future_item_current_", addBaseFeatNumericNames),
                                                            paste0("future_", addBaseFeatCatNames)))

clickstreamExpandedBaseFeatures <- c(
c(paste0("future_item_current_", addBaseFeatNumericNames), paste0("future_", addBaseFeatCatNames))
)

baseColOrder <- c("session_id", "timestamp_int", "timestamp", "user_id", "item_id", "future_item_id", "user_session_number")

setcolorder(clickstreamEventsExpandedDT, c(baseColOrder, setdiff(colnames(clickstreamEventsExpandedDT), baseColOrder)))


if (dataset == "retailrocket") {

    clickstreamEventsPastFutureItemsDT <-
        clickstreamEventsDT[, list(future_item_id = item_id, timestamp_int, future_item_current_available_ind = current_item_available_ind,
                                    future_category_id = current_category_id, future_item_current_price = current_item_price,
                                    timestamp, user_id, session_id, user_session_number, event)]

    clickstreamEventsExpandedCombinedDT <- rbind(clickstreamEventsExpandedDT, clickstreamEventsPastFutureItemsDT, fill = TRUE)

    userItemSessionAggDT <-
        clickstreamEventsExpandedCombinedDT[, list(future_item_current_price = min(future_item_current_price, na.rm = TRUE),
                                                    future_item_current_available_ind = min(future_item_current_available_ind, na.rm = TRUE)),
                                                by = c("user_id", "future_item_id", "user_session_number")]

    userItemSessionAggDT[, future_item_session_price_diff :=
                            ifelse(rep(.N > 1, .N), c(NA, future_item_current_price[2:.N] - future_item_current_price[1:(.N - 1)]), as.numeric(NA)),
                                                by = c("user_id", "future_item_id")]

    userItemSessionAggDT[, future_item_session_available_ind_diff :=
                            ifelse(rep(.N > 1, .N), c(NA, future_item_current_available_ind[2:.N] - future_item_current_available_ind[1:(.N - 1)]),
                                                        as.numeric(NA)), by = c("user_id", "future_item_id")]

}

if (dataset == "diginetica") {

    clickstreamEventsPastFutureItemsDT <-
        clickstreamEventsDT[, list(future_item_id = item_id, timestamp_int, future_category_id = current_category_id,
                                    future_item_current_price = current_item_price, timestamp, user_id, session_id, user_session_number, event)]

    clickstreamEventsExpandedCombinedDT <- rbind(clickstreamEventsExpandedDT, clickstreamEventsPastFutureItemsDT, fill = TRUE)

    userItemSessionAggDT <- clickstreamEventsExpandedCombinedDT[, list(future_item_current_price = min(future_item_current_price, na.rm = TRUE)),
                                                                    by = c("user_id", "future_item_id", "user_session_number")]


    userItemSessionAggDT[, future_item_session_price_diff :=
                                ifelse(rep(.N > 1, .N), c(NA, future_item_current_price[2:.N] - future_item_current_price[1:(.N - 1)]), as.numeric(NA)),
                                    by = c("user_id", "future_item_id")]

}


userItemSessionAggDT[, user_future_item_sessions_last_interaction :=
                        ifelse(rep(.N > 1, .N), c(NA, user_session_number[2:.N] - user_session_number[1:(.N - 1)]), as.numeric(NA)),
                            by = c("user_id", "future_item_id")]

userItemSessionAggDT[, user_future_item_sessions_first_interaction :=
                        ifelse(rep(.N > 1, .N), c(NA, user_session_number[2:.N] - user_session_number[1]), as.numeric(NA)),
                            by = c("user_id", "future_item_id")]


clickstreamEventsBaseFeatures <- c(
"user_id", "current_item_id",
"user_session_number", "session_time_since_start", "session_interaction_number",
paste0("session_number_of_", eventNames, "s"),
c(paste0("current_item_", addBaseFeatNumericNames), paste0("current_", addBaseFeatCatNames))
)

tempJoinKeys <- c("user_id", "current_item_id", "session_id", "timestamp_int")

clickstreamEventsExpandedDT <-
    merge(clickstreamEventsExpandedDT,
            clickstreamEventsDT[, c(tempJoinKeys, setdiff(clickstreamEventsBaseFeatures, c(tempJoinKeys, "user_session_number"))),
                    with = FALSE][!duplicated(clickstreamEventsDT[, c(tempJoinKeys), with = FALSE])],
                by = c("user_id", "current_item_id", "session_id", "timestamp_int"), all.x = TRUE, all.y = FALSE, sort = FALSE)

userItemSessionAggFeatures <- c(
paste0("future_item_session_", setdiff(addBaseFeatNames, categoricalItemPropertiesNames), "_diff"),
"user_future_item_sessions_last_interaction",
"user_future_item_sessions_first_interaction"
)

clickstreamEventsExpandedDT <-
    merge(clickstreamEventsExpandedDT, userItemSessionAggDT[, c("user_id", "future_item_id", "user_session_number", userItemSessionAggFeatures),
            with = FALSE], by = c("user_id", "future_item_id", "user_session_number"), all.x = TRUE, all.y = FALSE, sort = FALSE)

rm(userItemSessionAggDT, clickstreamEventsExpandedCombinedDT); gc()


clickstreamEventsExpandedDT[, current_event_type := ifelse(event == "view", 1, ifelse(event == "add_to_cart", 2, 3))]

clickstreamEventsExpandedDT[, same_item := ifelse(current_item_id == future_item_id, 1, 0)]

clickstreamEventsExpandedDT[, same_category := ifelse(current_category_id == future_category_id, 1, 0)]

clickstreamEventsExpandedDT[, price_diff_current_future_item := current_item_price - future_item_current_price]

setkeyv(clickstreamEventsExpandedDT, c("timestamp_int", "user_id", "item_id"))


clickstreamEventsDT[, category_id := current_category_id]

userCurrentItemTimeAggDT <- clickstreamEventsDT[, list(user_session_number = min(user_session_number)),
                                                    by = c("user_id", "item_id", "category_id", "timestamp_int", "event")]

setorder(userCurrentItemTimeAggDT, user_id, timestamp_int)

userCurrentItemTimeAggDT[, min_user_item_timestamp_int := min(timestamp_int), by = c("user_id", "item_id")]

userCurrentItemTimeAggDT[, user_item_time_since_first_interaction := as.numeric(timestamp_int - min_user_item_timestamp_int),
                            by = c("user_id", "item_id", "timestamp_int")]

userCurrentItemTimeAggDT[, user_item_time_since_previous_interaction := ifelse(rep(.N > 1, .N),
                            c(NA, as.numeric(timestamp_int[2:.N] - timestamp_int[1:(.N - 1)])), as.numeric(NA)), by = c("user_id", "item_id")]

userCurrentItemTimeAggDT[, user_item_run_count_interaction := 1:.N, by = c("user_id", "item_id")]

for (eventVal in eventTypes) {

    userCurrentItemTimeAggDT[, (paste0("user_item_run_count_", eventVal)) := cumSumNAIgnore(event == eventVal), by = c("user_id", "item_id")]

    userCurrentItemTimeAggDT[, (paste0("min_user_item_timestamp_int_", eventVal)) := as.numeric(min(timestamp_int[event == eventVal])),
                                by = c("user_id", "item_id")]

    userCurrentItemTimeAggDT[timestamp_int < get(paste0("min_user_item_timestamp_int_", eventVal)),
                                (paste0("min_user_item_timestamp_int_", eventVal)) := as.numeric(NA), by = c("user_id", "item_id")]

    userCurrentItemTimeAggDT[, (paste0("cum_max_user_item_timestamp_int_", eventVal)) :=
                                cummax(ifelse(event == eventVal, as.numeric(timestamp_int), rep(-Inf, length(timestamp_int)))),
                                by = c("user_id", "item_id")]

    userCurrentItemTimeAggDT[, (paste0("user_item_time_since_first_", eventVal)) :=
                                as.numeric(timestamp_int - get(paste0("min_user_item_timestamp_int_", eventVal))), by = c("user_id", "item_id")]

    userCurrentItemTimeAggDT[, (paste0("user_item_time_since_last_", eventVal)) :=
                                    ifelse(rep(.N > 1, .N), c(NA,
                                            as.numeric(timestamp_int[2:.N] - get(paste0("cum_max_user_item_timestamp_int_", eventVal))[1:(.N - 1)])),
                                            as.numeric(NA)), by = c("user_id", "item_id")]

    userCurrentItemTimeAggDT[, (paste0("user_item_time_since_last_", eventVal)) :=
                                    ifelse(event == eventVal, 0, get(paste0("user_item_time_since_last_", eventVal))), by = c("user_id", "item_id")]

    userCurrentItemTimeAggDT[, (paste0("event_is_", eventVal)) := ifelse(event == eventVal, 1L, 0L), by = c("user_id", "item_id")]

    userCurrentItemTimeAggDT[, (paste0("user_category_session_different_item_run_count_", eventVal)) :=
                                    cumSumCond(get(paste0("event_is_", eventVal)), item_id), by = c("user_id", "category_id", "user_session_number")]

    userCurrentItemTimeAggDT[, (paste0("user_category_session_different_item_time_since_last_", eventVal)) :=
                                    timeSinceUnequalCond(ifelse(event == eventVal, timestamp_int, -1000000000000),
                                                            ifelse(event == eventVal, item_id, -1)),
                                by = c("user_id", "category_id", "user_session_number")]

    userCurrentItemTimeAggDT[get(paste0("user_category_session_different_item_time_since_last_", eventVal)) == 0 |
                                abs(get(paste0("user_category_session_different_item_time_since_last_", eventVal))) > 10000000000,
                                        (paste0("user_category_session_different_item_time_since_last_", eventVal)) := -1000000]

}

userCurrentItemTimeAggDT[, user_category_session_different_item_time_since_last_interaction := timeSinceUnequalCond(timestamp_int, item_id),
                            by = c("user_id", "category_id", "user_session_number")]


userItemMaxTimeBySessionDT <- userCurrentItemTimeAggDT[, list(userItemMaxTimeLastSessions = max(timestamp_int)),
                                                            by = c("user_id", "item_id", "user_session_number")]

userItemMaxTimeBySessionDT[, maxUserSessionNumberLastSessions := user_session_number]


userItemTimeAggJoinDT <- userItemMaxTimeBySessionDT[userCurrentItemTimeAggDT, , on = list(user_id, item_id, user_session_number < user_session_number)]

userItemTimeAggNewDT <- userItemTimeAggJoinDT[, list(userItemMaxTimeLastSessions = max(userItemMaxTimeLastSessions),
                                                        maxUserSessionNumberLastSessions = max(maxUserSessionNumberLastSessions)),
                                                    by = setdiff(colnames(userItemTimeAggJoinDT),
                                                            c("userItemMaxTimeLastSessions", "maxUserSessionNumberLastSessions"))]

userCurrentItemTimeAggDT <-
    merge(userCurrentItemTimeAggDT,
            userItemTimeAggNewDT[, c("user_id", "item_id", "category_id", "timestamp_int", "event",
                                        "userItemMaxTimeLastSessions", "maxUserSessionNumberLastSessions"), with = FALSE],
            by = c("user_id", "item_id", "category_id", "timestamp_int", "event"), all.x = TRUE, all.y = FALSE, sort = FALSE)

userCurrentItemTimeAggDT[, user_item_time_since_last_interaction_last_sessions := (timestamp_int - userItemMaxTimeLastSessions)]

userCurrentItemTimeAggDT[, user_item_time_sessions_since_last_interaction := user_session_number - maxUserSessionNumberLastSessions]


userFutureItemDT <- unique(clickstreamEventsExpandedDT[, list(user_id, future_item_id, timestamp_int,
                                                                current_timestamp_int = timestamp_int,
                                                                current_user_session_number = user_session_number)])

userCurrentItemTimeAggDT[, previous_timestamp_int := timestamp_int]

userCurrentItemTimeAggDT[, previous_user_session_number := user_session_number]


userFutureItemDT <- userCurrentItemTimeAggDT[userFutureItemDT,
                                                , on = c("user_id" = "user_id", "item_id" = "future_item_id",
                                                            "timestamp_int" = "timestamp_int"), roll = Inf]

setnames(userFutureItemDT, "item_id", "future_item_id")

userFutureItemDT[, time_delta := current_timestamp_int - previous_timestamp_int]

userFutureItemDT[, session_delta := current_user_session_number - previous_user_session_number]

userFutureItemDT[, user_item_time_since_first_interaction := user_item_time_since_first_interaction + time_delta]

userFutureItemDT[, user_item_time_since_previous_interaction := user_item_time_since_previous_interaction + time_delta]

for (eventVal in eventTypes) {

    userFutureItemDT[, (paste0("user_item_time_since_first_", eventVal)) := get(paste0("user_item_time_since_first_", eventVal)) + time_delta]

}

for (eventVal in eventTypes) {

    userFutureItemDT[, (paste0("user_item_time_since_last_", eventVal)) := get(paste0("user_item_time_since_last_", eventVal)) + time_delta]

}

userFutureItemDT[, user_item_session_interaction_diff := session_delta]


userFutureItemFeatures <- c(
"user_item_time_since_first_interaction", "user_item_time_since_previous_interaction",
"user_item_run_count_interaction",
paste0("user_item_run_count_", eventNames),
paste0("user_item_time_since_first_", eventNames),
paste0("user_item_time_since_last_", eventNames),
"user_item_session_interaction_diff"
)

clickstreamEventsExpandedDT[, row_number := 1:.N]

tempKeys <- c("user_id", "future_item_id", "timestamp_int")

nonDuplicatedUserFutureItemDT <- userFutureItemDT[!(duplicated(userFutureItemDT[, c(tempKeys), with = FALSE])) &
                                                  !(duplicated(userFutureItemDT[, c(tempKeys), with = FALSE], fromLast = TRUE)),
                                                  c(tempKeys, userFutureItemFeatures), with = FALSE]

tempMerge_1 <- merge(clickstreamEventsExpandedDT, nonDuplicatedUserFutureItemDT, by = tempKeys, all.x = FALSE, all.y = FALSE, sort = FALSE)

tempKeys <- c("user_id", "future_item_id", "timestamp_int")

tempKeysExt <- c(tempKeys, "event")

duplicatedUserFutureItemDT <- userFutureItemDT[(duplicated(userFutureItemDT[, c(tempKeys), with = FALSE]) |
                                                duplicated(userFutureItemDT[, c(tempKeys), with = FALSE], fromLast = TRUE)),
                                                c(tempKeysExt, userFutureItemFeatures), with = FALSE]

tempMerge_2 <- merge(clickstreamEventsExpandedDT, duplicatedUserFutureItemDT, by = tempKeysExt, all = FALSE, sort = FALSE)

clickstreamEventsExpandedDT <- rbind(tempMerge_1, tempMerge_2)

setorder(clickstreamEventsExpandedDT, row_number)

setorder(clickstreamEventsExpandedDT, timestamp_int)

rm(tempMerge_1, tempMerge_2, userFutureItemDT, userCurrentItemTimeAggDT, userItemMaxTimeBySessionDT, userItemTimeAggJoinDT,
userItemTimeAggNewDT, nonDuplicatedUserFutureItemDT, duplicatedUserFutureItemDT)

gc()


userTimeAggDT <- clickstreamEventsDT[, list(user_session_number = min(user_session_number)), by = c("user_id", "timestamp_int", "event")]

setkeyv(userTimeAggDT, c("user_id", "timestamp_int"))

userTimeAggDT[, min_user_timestamp_int := min(timestamp_int), by = c("user_id")]

userTimeAggDT[, user_time_since_first_interaction := as.numeric(timestamp_int - min_user_timestamp_int), by = c("user_id", "timestamp_int")]

userTimeAggDT[, user_time_since_previous_interaction :=
                    ifelse(rep(.N > 1, .N), c(NA, as.numeric(timestamp_int[2:.N] - timestamp_int[1:(.N - 1)])), as.numeric(NA)), by = c("user_id")]

userTimeAggDT[, user_run_count_interaction := 1:.N, by = c("user_id")]

for (eventVal in eventTypes) {

    userTimeAggDT[, (paste0("user_run_count_", eventVal)) := cumSumNAIgnore(event == eventVal), by = c("user_id")]

    # By using a custom Rcpp function, these computions can be performed with a considerable speed up.
    userTimeAggDT[, (paste0("user_run_count_", eventVal, "_10")) := runSum(as.numeric(event == eventVal), n = pmin(10, .N)), by = c("user_id")]

    userTimeAggDT[, (paste0("user_run_count_", eventVal, "_100")) := runSum(as.numeric(event == eventVal), n = pmin(100, .N)), by = c("user_id")]

    userTimeAggDT[, (paste0("min_user_timestamp_int_", eventVal)) := as.numeric(min(timestamp_int[event == eventVal])), by = c("user_id")]

    userTimeAggDT[timestamp_int < get(paste0("min_user_timestamp_int_", eventVal)),
                    (paste0("min_user_timestamp_int_", eventVal)) := as.numeric(NA), by = c("user_id")]

    userTimeAggDT[, (paste0("cum_max_user_timestamp_int_", eventVal)) :=
                        cummax(ifelse(event == eventVal, as.numeric(timestamp_int), rep(-Inf, length(timestamp_int)))), by = c("user_id")]

    userTimeAggDT[, (paste0("user_time_since_first_", eventVal)) :=
                        as.numeric(timestamp_int - get(paste0("min_user_timestamp_int_", eventVal))), by = c("user_id")]

    userTimeAggDT[, (paste0("user_time_since_last_", eventVal)) :=
                        ifelse(rep(.N > 1, .N), c(NA,
                                                    as.numeric(timestamp_int[2:.N] - get(paste0("cum_max_user_timestamp_int_", eventVal))[1:(.N - 1)])),
                                                    as.numeric(NA)), by = c("user_id")]

    userTimeAggDT[, (paste0("user_time_since_last_", eventVal)) :=
                        ifelse(event == eventVal, 0, get(paste0("user_time_since_last_", eventVal))), by = c("user_id")]

}

if (length(eventNames) > 1) {

    for (eventValInd_1 in 1:(length(eventNames) - 1)) {

        for (eventValInd_2 in (eventValInd_1 + 1):length(eventNames)) {

            eventVal_1 <- eventNames[eventValInd_1]

            eventVal_2 <- eventNames[eventValInd_2]

            userTimeAggDT[, (paste0("user_frac_run_count_", eventVal_2, "_", eventVal_1)) :=
                                get(paste0("user_run_count_", eventVal_2)) / get(paste0("user_run_count_", eventVal_1))]

            userTimeAggDT[, (paste0("user_frac_run_count_", eventVal_2, "_", eventVal_1, "_", "100")) :=
                                get(paste0("user_run_count_", eventVal_2, "_", "100")) / get(paste0("user_run_count_", eventVal_1, "_", "100"))]

            userTimeAggDT[, (paste0("user_frac_run_count_", eventVal_2, "_", eventVal_1, "_", "10")) :=
                                get(paste0("user_run_count_", eventVal_2, "_", "10")) / get(paste0("user_run_count_", eventVal_1, "_", "10"))]

        }

    }

}


userTimeAggFeatures <- c(
"user_time_since_previous_interaction",
paste0("user_run_count_", eventNames, "_", "10"),
paste0("user_run_count_", eventNames, "_", "100"),
paste0("user_time_since_last_", eventNames)
)


tempKeys <- c("user_id", "timestamp_int")
tempKeysExt <- c(tempKeys, "event")

userTimeAggDT <- userTimeAggDT[, c(tempKeysExt, userTimeAggFeatures), with = FALSE]
gc()

nonDuplicatedUserTimeAggDT <-
    userTimeAggDT[!(duplicated(userTimeAggDT[, c(tempKeys), with = FALSE])) &
                    !(duplicated(userTimeAggDT[, c(tempKeys), with = FALSE], fromLast = TRUE)), c(tempKeys, userTimeAggFeatures), with = FALSE]

tempMerge_1 <- merge(clickstreamEventsExpandedDT, nonDuplicatedUserTimeAggDT, by = tempKeys, all.x = FALSE, all.y = FALSE, sort = FALSE)

duplicatedUserTimeAggDT <- userTimeAggDT[(duplicated(userTimeAggDT[, c(tempKeys), with = FALSE]) |
                                            duplicated(userTimeAggDT[, c(tempKeys), with = FALSE], fromLast = TRUE)),
                                            c(tempKeysExt, userTimeAggFeatures), with = FALSE]

tempMerge_2 <- merge(clickstreamEventsExpandedDT, duplicatedUserTimeAggDT, by = tempKeysExt, all = FALSE, sort = FALSE)

clickstreamEventsExpandedDT <- rbind(tempMerge_1, tempMerge_2)

setorder(clickstreamEventsExpandedDT, timestamp_int)

rm(tempMerge_1, tempMerge_2, userTimeAggDT, nonDuplicatedUserTimeAggDT, duplicatedUserTimeAggDT)
gc()

clickstreamEventsExpandedDT[, session_time_since_start := as.numeric(timestamp_int - min(timestamp_int)), by = "session_id"]


entitiesNamePrefix <- "user_category"

currentByExtVars <- c("user_id", "current_category_id", "timestamp_int", "event")

currentByVars <- c("user_id", "current_category_id")

timeVar <- "timestamp_int"

userSessionVar <- "user_session_number"

userSessionVar <- c("user_id", "future_category_id")

userSessionVarExt <- c(userSessionVar, "event")

entitiesFeatures <- paste0("entities_",
c(
"time_since_first_interaction", "time_since_previous_interaction",
"run_count_interaction",
paste0("run_count_", eventNames),
paste0("time_since_first_", eventNames),
paste0("time_since_last_", eventNames),
"session_interaction_diff"
))

try(source(paste0(codePath, link_hysar_gbm_gen_feat_dynamic_r)))


entitiesNamePrefix <- "item"

currentByExtVars <- c("current_item_id", "timestamp_int", "event")

currentByVars <- c("current_item_id")

timeVar <- "timestamp_int"

userSessionVar <- "user_session_number"

userSessionVar <- c("future_item_id")

userSessionVarExt <- c(userSessionVar, "event")

try(source(paste0(codePath, link_hysar_gbm_gen_feat_item_r)))

try(rm(timeWindowStatsEntitiesDT, nonDuplicatedFutureEntitiesAggDT, futureEntitiesAggDT))

gc()

keys <- c("user_id", "session_id", "timestamp_int", "current_item_id", "future_item_id", "event", "user_session_number")

clickstreamEventsExpandedDT <- clickstreamEventsExpandedDT[!duplicated(clickstreamEventsExpandedDT[, keys, with = FALSE]),]

try(source(paste0(codePath, link_hysar_gbm_gen_add_feat_r)))


clickstreamEventsExpandedDT[, date_value := as.Date(timestamp)]

clickstreamEventsExpandedDT[, target := ifelse(future_event %in% targetEventsVec, 1, 0)]

keys <- c("user_id", "session_id", "timestamp_int", "current_item_id", "future_item_id", "event", "user_session_number")

clickstreamEventsExpandedDT <- clickstreamEventsExpandedDT[!duplicated(clickstreamEventsExpandedDT[, keys, with = FALSE]),]

saveRDS(clickstreamEventsExpandedDT, paste0(cachePath, cacheNameClickstreamEventsExpandedDT, "_", extendedFileSuffix, ".rds"))


futureBaseFeatures <- c("future_item_id", "future_category_id", "future_item_current_price",
                        if ("available_ind" %in% addBaseFeatNames) "future_item_current_available_ind" else character(0),
                        "current_event_type", "same_item", "same_category", "price_diff_current_future_item")

userCategoryFeatures <- c(
    "user_category_time_since_first_interaction", "user_category_time_since_previous_interaction",
    "user_category_run_count_interaction",
    paste0("user_category_run_count_", eventNames),
    paste0("user_category_time_since_first_", eventNames),
    paste0("user_category_time_since_last_", eventNames),
    "user_category_session_interaction_diff"
    )

featureNames <- c(
clickstreamEventsBaseFeatures
, futureBaseFeatures
, userFutureItemFeatures
, userCategoryFeatures
, userItemSessionAggFeatures
, userTimeAggFeatures
, itemTimeAggFeatures
, addNewUserCategoryFeat
, addNewUserFeat
, addNewItemFeat
, addNewCategoryFeat
, addNewUserItemFeat
, addNewUserSessionItemFeat
, addNewSessionFeat
, addNewTimeFeat
)

saveRDS(featureNames, paste0(cachePath, "featureNames", "_", extendedFileSuffix, ".rds"))


#______________________________
# josef.b.bauer at gmail.com

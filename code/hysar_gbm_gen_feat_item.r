
currentEntitiesAggDT <- unique(clickstreamEventsDT[, currentByExtVars, with = FALSE])

currentEntitiesAggDT[, (paste0("min_", entitiesNamePrefix, "_timestamp_int")) := min(timestamp_int), by = currentByVars]

currentEntitiesAggDT[, (paste0(entitiesNamePrefix, "_time_since_first_interaction")) :=
                            as.numeric(timestamp_int - get(paste0("min_", entitiesNamePrefix, "_timestamp_int"))),
                        by = c(currentByVars, "timestamp_int")]

currentEntitiesAggDT[, (paste0(entitiesNamePrefix, "_time_since_previous_interaction")) :=
                            ifelse(rep(.N > 1, .N), c(NA, as.numeric(timestamp_int[2:.N] - timestamp_int[1:(.N - 1)])), as.numeric(NA)),
                        by = currentByVars]

currentEntitiesAggDT[, (paste0(entitiesNamePrefix, "_run_count_interaction")) := 1:.N, by = currentByVars]

for (eventVal in eventTypes) {

    currentEntitiesAggDT[, (paste0(entitiesNamePrefix, "_run_count_", eventVal)) := cumSumNAIgnore(event == eventVal),
                                    by = currentByVars]

    currentEntitiesAggDT[, (paste0("min_", entitiesNamePrefix, "_timestamp_int_", eventVal)) :=
                                as.numeric(min(timestamp_int[event == eventVal])),
                                    by = currentByVars]

    currentEntitiesAggDT[timestamp_int < get(paste0("min_", entitiesNamePrefix, "_timestamp_int_", eventVal)),
                            (paste0("min_", entitiesNamePrefix, "_timestamp_int_", eventVal)) := as.numeric(NA),
                                    by = currentByVars]

    currentEntitiesAggDT[, (paste0("cum_max_", entitiesNamePrefix, "_timestamp_int_", eventVal)) :=
                                cummax(ifelse(event == eventVal, as.numeric(timestamp_int), rep(-Inf, length(timestamp_int)))),
                                    by = currentByVars]

    currentEntitiesAggDT[, (paste0(entitiesNamePrefix, "_time_since_first_", eventVal)) :=
                                as.numeric(timestamp_int - get(paste0("min_", entitiesNamePrefix, "_timestamp_int_", eventVal))),
                                    by = currentByVars]

    currentEntitiesAggDT[, (paste0(entitiesNamePrefix, "_time_since_last_", eventVal)) :=
                                ifelse(rep(.N > 1, .N), c(NA, as.numeric(timestamp_int[2:.N] -
                                        get(paste0("cum_max_", entitiesNamePrefix, "_timestamp_int_", eventVal))[1:(.N - 1)])), as.numeric(NA)),
                                    by = currentByVars]

    currentEntitiesAggDT[, (paste0(entitiesNamePrefix, "_time_since_last_", eventVal)) :=
                                ifelse(event == eventVal, 0, get(paste0(entitiesNamePrefix, "_time_since_last_", eventVal))),
                                    by = currentByVars]

}


futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVar, timeVar), with = FALSE])

futureEntitiesAggDT[, (paste0("current_", timeVar)) := get(timeVar)]

currentEntitiesAggDT[, previous_timestamp_int := timestamp_int]


eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByVars[1:length(currentByVars)], "\"", " = ", "\"", userSessionVar[1:length(userSessionVar)], "\"", collapse = ", "),
", ", "\"", timeVar, "\"", " = ", "\"", timeVar, "\"", "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

futureEntitiesAggDT[, time_delta := current_timestamp_int - previous_timestamp_int]

futureEntitiesAggDT[, (paste0(entitiesNamePrefix, "_", "time_since_first_interaction")) :=
                            get(paste0(entitiesNamePrefix, "_", "time_since_first_interaction")) + time_delta]

futureEntitiesAggDT[, (paste0(entitiesNamePrefix, "_", "time_since_previous_interaction")) :=
                            get(paste0(entitiesNamePrefix, "_", "time_since_previous_interaction")) + time_delta]

for (eventVal in eventTypes) {

    futureEntitiesAggDT[, (paste0(entitiesNamePrefix, "_", "time_since_first_", eventVal)) :=
                                get(paste0(entitiesNamePrefix, "_", "time_since_first_", eventVal)) + time_delta]

    futureEntitiesAggDT[, (paste0(entitiesNamePrefix, "_", "time_since_last_", eventVal)) :=
                                get(paste0(entitiesNamePrefix, "_", "time_since_last_", eventVal)) + time_delta]

}


currentEntitiesAggDT[, entity_id := current_item_id]

futureEntitiesAggDT[, entity_id := future_item_id]


timeWindowStatsEntitiesDT <- rbindlist(list(unique(currentEntitiesAggDT[, c("entity_id", currentByVars, timeVar, "event"), with = FALSE]),
                                            unique(futureEntitiesAggDT[, c("entity_id", userSessionVar, timeVar), with = FALSE])), fill = TRUE)

for (eventVal in eventTypes) {

    timeWindowStatsEntitiesDT[, (paste0("event_is_", eventVal)) := ifelse(event == eventVal, 1L, 0L)]
    timeWindowStatsEntitiesDT[is.na(get(paste0("event_is_", eventVal))), (paste0("event_is_", eventVal)) := 0L]

}

eval(parse(text = paste0("setorder(timeWindowStatsEntitiesDT, entity_id, ", timeVar, ")")))


for (eventVal in eventTypes) {

    timeWindowStatsEntitiesDT[, (paste0(entitiesNamePrefix, "_", "time_window_", eventVal, "s", "_", "last_hour")) :=
                                    calcWindowCount(get(paste0("event_is_", eventVal)), get(timeVar), get(timeVar), 60*60),
                                    by = c("entity_id")]
    timeWindowStatsEntitiesDT[, (paste0(entitiesNamePrefix, "_", "time_window_", eventVal, "s", "_", "last_day")) :=
                                    calcWindowCount(get(paste0("event_is_", eventVal)), get(timeVar), get(timeVar), 60*60*24),
                                    by = c("entity_id")]
    timeWindowStatsEntitiesDT[, (paste0(entitiesNamePrefix, "_", "time_window_", eventVal, "s", "_", "last_week")) :=
                                    calcWindowCount(get(paste0("event_is_", eventVal)), get(timeVar), get(timeVar), 60*60*24*7),
                                    by = c("entity_id")]
    timeWindowStatsEntitiesDT[, (paste0(entitiesNamePrefix, "_", "time_window_", eventVal, "s", "_", "last_4_weeks")) :=
                                    calcWindowCount(get(paste0("event_is_", eventVal)), get(timeVar), get(timeVar), 60*60*24*7*4),
                                    by = c("entity_id")]

}

timeWindowStatsEntitiesDT[, (userSessionVar) := entity_id]


itemTimeAggFeatures <- paste0("item", "_",
c(
"time_since_first_interaction", "time_since_previous_interaction",
"run_count_interaction",
paste0("run_count_", eventNames),
paste0("time_since_first_", eventNames),
paste0("time_since_last_", eventNames),
paste0("time_window_", eventNames, "s", "_", "last_hour"),
paste0("time_window_", eventNames, "s", "_", "last_day"),
paste0("time_window_", eventNames, "s", "_", "last_week"),
paste0("time_window_", eventNames, "s", "_", "last_4_weeks")
))


futureEntitiesAggDT <- merge(futureEntitiesAggDT, unique(timeWindowStatsEntitiesDT[,
                                                            c(userSessionVar, timeVar, intersect(colnames(timeWindowStatsEntitiesDT),
                                                                                                    itemTimeAggFeatures)), with = FALSE]),
                                on = c(userSessionVar, timeVar), all.x = TRUE, all.y = FALSE, sort = FALSE)


nonDuplicatedFutureEntitiesAggDT <-
    futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, c(userSessionVar, timeVar), with = FALSE])) &
                        !(duplicated(futureEntitiesAggDT[, c(userSessionVar, timeVar), with = FALSE], fromLast = TRUE)),
                        c(userSessionVar, timeVar, setdiff(intersect(itemTimeAggFeatures, colnames(futureEntitiesAggDT)),
                            colnames(clickstreamEventsExpandedDT))), with = FALSE]

tempMerge_1 <- merge(clickstreamEventsExpandedDT, nonDuplicatedFutureEntitiesAggDT, by = c(userSessionVar, timeVar),
                        all.x = FALSE, all.y = FALSE, sort = FALSE)

duplicatedFutureEntitiesAggDT <-
    futureEntitiesAggDT[(duplicated(futureEntitiesAggDT[, c(userSessionVar, timeVar), with = FALSE]) |
                         duplicated(futureEntitiesAggDT[, c(userSessionVar, timeVar), with = FALSE], fromLast = TRUE)),
                        c(userSessionVarExt, timeVar, setdiff(intersect(itemTimeAggFeatures, colnames(futureEntitiesAggDT)),
                                                                colnames(clickstreamEventsExpandedDT))), with = FALSE]
tempMerge_2 <- merge(clickstreamEventsExpandedDT, duplicatedFutureEntitiesAggDT, by = c(userSessionVarExt, timeVar), all = FALSE, sort = FALSE)

try(rm(clickstreamEventsExpandedDT)); gc()
tempMerge_1 <- rbindlist(list(tempMerge_1, tempMerge_2), use.names = TRUE)
clickstreamEventsExpandedDT <- tempMerge_1

rm(tempMerge_1, tempMerge_2); gc()

setorder(clickstreamEventsExpandedDT, timestamp_int)


itemTimeAggFeatures <- intersect(itemTimeAggFeatures, colnames(clickstreamEventsExpandedDT))

eval(parse(text = paste0(entitiesNamePrefix, "Features", " <- itemTimeAggFeatures")))

eval(parse(text = paste0(entitiesNamePrefix, "Features", " <- gsub(", "\"", "entities", "\"", ", ", "\"", entitiesNamePrefix,
                            "\"", ", ", "itemTimeAggFeatures", ")")))


#______________________________
# josef.b.bauer at gmail.com

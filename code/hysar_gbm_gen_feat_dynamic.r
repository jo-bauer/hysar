
currentEntitiesAggDT <- eval(parse(text = paste0("clickstreamEventsDT[, list(", userSessionVar,
                                                    " = min(get(userSessionVar))), by = currentByExtVars]")))

currentEntitiesAggDT[, min_entities_timestamp_int := min(timestamp_int), by = currentByVars]

currentEntitiesAggDT[, entities_time_since_first_interaction :=
                            as.numeric(timestamp_int - min_entities_timestamp_int), by = c(currentByVars, "timestamp_int")]

currentEntitiesAggDT[, entities_time_since_previous_interaction :=
                            ifelse(rep(.N > 1, .N), c(NA, as.numeric(timestamp_int[2:.N] - timestamp_int[1:(.N - 1)])), as.numeric(NA)),
                        by = currentByVars]

currentEntitiesAggDT[, entities_run_count_interaction := 1:.N, by = currentByVars]

for (eventVal in eventTypes) {

    currentEntitiesAggDT[, (paste0("entities_run_count_", eventVal)) := cumSumNAIgnore(event == eventVal), by = currentByVars]

    currentEntitiesAggDT[, (paste0("min_entities_timestamp_int_", eventVal)) := as.numeric(min(timestamp_int[event == eventVal])), by = currentByVars]

    currentEntitiesAggDT[timestamp_int < get(paste0("min_entities_timestamp_int_", eventVal)), (paste0("min_entities_timestamp_int_", eventVal)) :=
                            as.numeric(NA), by = currentByVars]

    currentEntitiesAggDT[, (paste0("cum_max_entities_timestamp_int_", eventVal)) :=
                                cummax(ifelse(event == eventVal, as.numeric(timestamp_int), rep(-Inf, length(timestamp_int)))), by = currentByVars]

    currentEntitiesAggDT[, (paste0("entities_time_since_first_", eventVal)) :=
                                as.numeric(timestamp_int - get(paste0("min_entities_timestamp_int_", eventVal))), by = currentByVars]

    currentEntitiesAggDT[, (paste0("entities_time_since_last_", eventVal)) :=
                                ifelse(rep(.N > 1, .N), c(NA, as.numeric(timestamp_int[2:.N] - get(paste0("cum_max_entities_timestamp_int_", eventVal))[1:(.N - 1)])),
                                as.numeric(NA)), by = currentByVars]

    currentEntitiesAggDT[, (paste0("entities_time_since_last_", eventVal)) :=
                                ifelse(event == eventVal, 0, get(paste0("entities_time_since_last_", eventVal))), by = currentByVars]

}


entitiesMaxTimeBySessionDT <- currentEntitiesAggDT[, list(entities_max_time_last_sessions = max(timestamp_int)), by = c(currentByVars, userSessionVar)]

entitiesMaxTimeBySessionDT[, (paste0("max_", userSessionVar, "_last_sessions")) := get(userSessionVar)]

entitiesMaxTimeBySessionDT[, (userSessionVar) := get(userSessionVar) + 1]


eval(parse(text = paste0(
"entitiesTimeAggJoinDT <- entitiesMaxTimeBySessionDT[currentEntitiesAggDT, , on = list(", paste0(currentByVars, collapse = ", "), ", ",
userSessionVar, "), roll = Inf]"
)))

entitiesTimeAggNewDT <-
    entitiesTimeAggJoinDT[, list(entities_max_time_last_sessions = max(entities_max_time_last_sessions),
                                 maxUserSessionNumberLastSessions = max(get(paste0("max_", userSessionVar, "_last_sessions")))),
                            by = setdiff(colnames(entitiesTimeAggJoinDT), c("entities_max_time_last_sessions", "maxUserSessionNumberLastSessions"))]

currentEntitiesAggDT <- merge(currentEntitiesAggDT,
                                entitiesTimeAggNewDT[, c(currentByExtVars, "entities_max_time_last_sessions", "maxUserSessionNumberLastSessions"),
                                with = FALSE], by = currentByExtVars, all.x = TRUE, all.y = FALSE, sort = FALSE)

currentEntitiesAggDT[, entities_time_since_last_interaction_last_sessions := (timestamp_int - entities_max_time_last_sessions)/(60*60*24)]

currentEntitiesAggDT[, entities_time_sessions_since_last_interaction := get(userSessionVar) - maxUserSessionNumberLastSessions]


futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVar, timeVar, userSessionVar), with = FALSE])

futureEntitiesAggDT[, (paste0("current_", timeVar)) := get(timeVar)]

setnames(futureEntitiesAggDT, userSessionVar, paste0("current_", userSessionVar))

currentEntitiesAggDT[, previous_timestamp_int := timestamp_int]

currentEntitiesAggDT[, previous_user_session_number := get(userSessionVar)]


eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByVars[1:length(currentByVars)], "\"", " = ", "\"", userSessionVar[1:length(userSessionVar)], "\"", collapse = ", "),
", ", "\"", timeVar, "\"", " = ", "\"", timeVar, "\"", "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

futureEntitiesAggDT[, time_delta := current_timestamp_int - previous_timestamp_int]

futureEntitiesAggDT[, session_delta := get(paste0("current_", userSessionVar)) - previous_user_session_number]

futureEntitiesAggDT[, entities_time_since_first_interaction := entities_time_since_first_interaction + time_delta]

futureEntitiesAggDT[, entities_time_since_previous_interaction := entities_time_since_previous_interaction + time_delta]

for (eventVal in eventTypes) {

    futureEntitiesAggDT[, (paste0("entities_time_since_first_", eventVal)) := get(paste0("entities_time_since_first_", eventVal)) + time_delta]

    futureEntitiesAggDT[, (paste0("entities_time_since_last_", eventVal)) := get(paste0("entities_time_since_last_", eventVal)) + time_delta]

}

futureEntitiesAggDT[, entities_session_interaction_diff := session_delta]

nonDuplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, c(userSessionVar, timeVar), with = FALSE])) &
                                                        !(duplicated(futureEntitiesAggDT[, c(userSessionVar, timeVar), with = FALSE], fromLast = TRUE)),
                                                        c(userSessionVar, timeVar, entitiesFeatures), with = FALSE]

tempMerge_1 <- merge(clickstreamEventsExpandedDT, nonDuplicatedFutureEntitiesAggDT, by = c(userSessionVar, timeVar),
                        all.x = FALSE, all.y = FALSE, sort = FALSE)

duplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[(duplicated(futureEntitiesAggDT[, c(userSessionVar, timeVar), with = FALSE]) |
                                                      duplicated(futureEntitiesAggDT[, c(userSessionVar, timeVar), with = FALSE], fromLast = TRUE)),
                                                      c(userSessionVarExt, timeVar, entitiesFeatures), with = FALSE]

tempMerge_2 <- merge(clickstreamEventsExpandedDT, duplicatedFutureEntitiesAggDT, by = c(userSessionVarExt, timeVar), all = FALSE, sort = FALSE)

try(rm(clickstreamEventsExpandedDT)); gc()

clickstreamEventsExpandedDT <- rbindlist(list(tempMerge_1, tempMerge_2), use.names = TRUE)

rm(tempMerge_1, tempMerge_2); gc()

setorder(clickstreamEventsExpandedDT, timestamp_int)


eval(parse(text = paste0(entitiesNamePrefix, "Features", " <- entitiesFeatures")))

eval(parse(text = paste0(entitiesNamePrefix, "Features", " <- gsub(", "\"", "entities", "\"", ", ", "\"", entitiesNamePrefix,
                            "\"", ", ", "entitiesFeatures", ")")))

eval(parse(text = paste0("setnames(clickstreamEventsExpandedDT, entitiesFeatures, ", entitiesNamePrefix, "Features", ")")))

eval(parse(text = paste0("rm(", entitiesNamePrefix, "Features", ")")))

rm(entitiesTimeAggNewDT, entitiesTimeAggJoinDT)


#______________________________
# josef.b.bauer at gmail.com

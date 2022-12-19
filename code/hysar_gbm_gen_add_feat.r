
clickstreamEventsDT[event %in% c("add_to_cart", "buy"), mean_price_of_items_user_selected_from_category :=
                                                            cummean(current_item_price), by = c("user_id", "current_category_id")]

clickstreamEventsDT[event %in% c("add_to_cart", "buy"), mean_price_of_items_all_users_selected_from_category :=
                                                            cummean(current_item_price), by = c("current_category_id")]

clickstreamEventsDT[event %in% c("view"), mean_price_of_items_user_viewed_from_category :=
                                                            cummean(current_item_price), by = c("user_id", "current_category_id")]

clickstreamEventsDT[event %in% c("view"), mean_price_of_items_all_users_viewed_from_category :=
                                                            cummean(current_item_price), by = c("current_category_id")]

clickstreamEventsDT[, user_category_relative_price_sensitivity :=
                        mean_price_of_items_all_users_selected_from_category - mean_price_of_items_user_selected_from_category]

clickstreamEventsDT[, user_category_relative_price_sensitivity :=
                            na.locf(user_category_relative_price_sensitivity, fromLast = FALSE, na.rm = FALSE),
                    by = c("user_id", "current_category_id")]

clickstreamEventsDT[, user_category_relative_price_sensitivity_views :=
                        mean_price_of_items_all_users_viewed_from_category - mean_price_of_items_user_viewed_from_category]

clickstreamEventsDT[, user_category_relative_price_sensitivity_views :=
                        na.locf(user_category_relative_price_sensitivity_views, fromLast = FALSE, na.rm = FALSE),
                    by = c("user_id", "current_category_id")]


clickstreamEventsDT[event %in% c("add_to_cart", "buy"), mean_price_of_items_user_selected := cummean(current_item_price), by = c("user_id")]

clickstreamEventsDT[event %in% c("add_to_cart", "buy"), mean_price_of_items_all_users_selected := cummean(current_item_price)]

clickstreamEventsDT[, user_relative_price_sensitivity := mean_price_of_items_all_users_selected - mean_price_of_items_user_selected]

clickstreamEventsDT[, user_relative_price_sensitivity := na.locf(user_relative_price_sensitivity, fromLast = FALSE, na.rm = FALSE), by = c("user_id")]

clickstreamEventsDT[event %in% c("add_to_cart", "buy"), mean_price_of_selected_items_in_category := cummean(current_item_price),
                    by = c("current_category_id")]

clickstreamEventsDT[, mean_price_of_selected_items_in_category := na.locf(mean_price_of_selected_items_in_category, fromLast = FALSE, na.rm = FALSE),
                    by = c("current_category_id")]

clickstreamEventsDT[event %in% c("add_to_cart", "buy"), diff_item_price_mean_price_of_selected_items_in_category :=
                                                            current_item_price - cummean(current_item_price),
                    by = c("current_category_id")]

clickstreamEventsDT[, diff_item_price_mean_price_of_selected_items_in_category :=
                        na.locf(diff_item_price_mean_price_of_selected_items_in_category, fromLast = FALSE, na.rm = FALSE),
                            by = c("current_category_id")]

clickstreamEventsDT[, mean_price_of_items_in_category := cummean(current_item_price), by = c("current_category_id")]

clickstreamEventsDT[, mean_price_of_items_in_category := na.locf(mean_price_of_items_in_category, fromLast = FALSE, na.rm = FALSE),
                        by = c("current_category_id")]

clickstreamEventsDT[, diff_item_price_mean_price_of_items_in_category := current_item_price - cummean(current_item_price),
                        by = c("current_category_id")]

clickstreamEventsDT[, diff_item_price_mean_price_of_items_in_category := na.locf(diff_item_price_mean_price_of_items_in_category,
                        fromLast = FALSE, na.rm = FALSE), by = c("current_category_id")]


calcPriceQuantile <- function(quant, current_category_id_val, timestamp_int_val) {

    quantile(clickstreamEventsDT[timestamp_int <= timestamp_int_val & current_category_id == current_category_id_val]$current_item_price,
                probs = quant)

}

clickstreamEventsDT[, lead_1_diff_time := shift(diff_time, n = 1, type = "lead"), by = "user_id"]

clickstreamEventsDT[, lead_1_diff_time := ifelse(lead_1_diff_time > 30, 30, lead_1_diff_time)]

clickstreamEventsDT[, observation_time_of_item_by_user := cumSumNAIgnore(abs(lead_1_diff_time)), by = c("user_id", "current_item_id")]

clickstreamEventsDT[, prior_observation_time_of_item_by_user := shift(observation_time_of_item_by_user, n = 1, type = "lag"),
                        by = c("user_id", "current_item_id")]

clickstreamEventsDT[, lead_1_diff_time := NULL]

clickstreamEventsDT[, observation_time_of_item_by_user := NULL]

clickstreamEventsDT[, lead_1_diff_time_in_current_session := shift(diff_time, n = 1, type = "lead"), by = c("user_id", "session_id")]

clickstreamEventsDT[, lead_1_diff_time_in_current_session := ifelse(lead_1_diff_time_in_current_session > 30, 30, lead_1_diff_time_in_current_session)]

clickstreamEventsDT[, observation_time_of_item_by_user_in_current_session := cumSumNAIgnore(abs(lead_1_diff_time_in_current_session)),
                        by = c("user_id", "session_id", "current_item_id")]

clickstreamEventsDT[, prior_observation_time_of_item_by_user_in_current_session :=
                        shift(observation_time_of_item_by_user_in_current_session, n = 1, type = "lag"),
                            by = c("user_id", "session_id", "current_item_id")]

clickstreamEventsDT[, lead_1_diff_time_in_current_session := NULL]

clickstreamEventsDT[, observation_time_of_item_by_user_in_current_session := NULL]

timeVar <- "timestamp_int"

clickstreamEventsDT[, run_number_of_observations_by_user_last_3_months := calcWindowCount(rep(1, .N), get(timeVar), get(timeVar), 60*60*24*7*13),
                        by = c("user_id")]

clickstreamEventsDT[, run_number_of_observations_by_session_last_3_months := calcWindowCount(rep(1, .N), get(timeVar), get(timeVar), 60*60*24*7*13),
                        by = c("session_id")]

clickstreamEventsDT[, run_number_of_observations_by_item_last_3_months := calcWindowCount(rep(1, .N), get(timeVar), get(timeVar), 60*60*24*7*13),
                        by = c("current_item_id")]

clickstreamEventsDT[, run_number_of_observations_by_category_last_3_months := calcWindowCount(rep(1, .N), get(timeVar), get(timeVar), 60*60*24*7*13),
                        by = c("current_category_id")]


clickstreamEventsDT[, day_of_week := weekdays(timestamp)]

if (dataset != "diginetica") {

    try(clickstreamEventsDT[, hour_of_day := lubridate::hour(timestamp)])

    try(clickstreamEventsDT[, user_hour_of_day_quantile_025 := runquantile(hour_of_day, k = .N, probs = 0.25, align="right"), by = "user_id"])

    try(clickstreamEventsDT[, user_hour_of_day_quantile_05 := runquantile(hour_of_day, k = .N, probs = 0.5, align="right"), by = "user_id"])

    try(clickstreamEventsDT[, user_hour_of_day_quantile_075 := runquantile(hour_of_day, k = .N, probs = 0.75, align="right"), by = "user_id"])

    if (!grepl("view", targetCase)) {

        try(clickstreamEventsDT[event %in% c("add_to_cart", "buy"), user_purchase_hour_of_day_quantile_025 :=
                                                                        runquantile(hour_of_day, k = .N, probs = 0.25, align="right"), by = "user_id"])

        try(clickstreamEventsDT[event %in% c("add_to_cart", "buy"), user_purchase_hour_of_day_quantile_05 :=
                                                                        runquantile(hour_of_day, k = .N, probs = 0.5, align="right"), by = "user_id"])

        try(clickstreamEventsDT[event %in% c("add_to_cart", "buy"), user_purchase_hour_of_day_quantile_075 :=
                                                                        runquantile(hour_of_day, k = .N, probs = 0.75, align="right"), by = "user_id"])

        try(clickstreamEventsDT[, user_purchase_hour_of_day_quantile_025 :=
                                    na.locf(user_purchase_hour_of_day_quantile_025, fromLast = FALSE, na.rm = FALSE), by = "user_id"])

        try(clickstreamEventsDT[, user_purchase_hour_of_day_quantile_05 := na.locf(user_purchase_hour_of_day_quantile_05, fromLast = FALSE,
                                    na.rm = FALSE), by = "user_id"])

        try(clickstreamEventsDT[, user_purchase_hour_of_day_quantile_075 := na.locf(user_purchase_hour_of_day_quantile_075, fromLast = FALSE,
                                    na.rm = FALSE), by = "user_id"])

    }

}


if (!grepl("view", targetCase)) {

    clickstreamEventsDT[, (paste0("user_run_count_add_to_cart_buy_on_current_weekday")) := 0]

}

clickstreamEventsDT[, (paste0("user_run_count_view_on_current_weekday")) := 0]

for (dayOfWeekVal in unique(clickstreamEventsDT$day_of_week)) {

    if (!grepl("view", targetCase)) {

        clickstreamEventsDT[, (paste0("user_run_count_add_to_cart_buy_weekday_", dayOfWeekVal)) :=
                                cumSumNAIgnore(as.numeric(event %in% c("add_to_cart", "buy") & day_of_week == dayOfWeekVal)), by = c("user_id")]

        clickstreamEventsDT[, (paste0("user_run_count_add_to_cart_buy_on_current_weekday")) :=
                                    ifelse(day_of_week == dayOfWeekVal, get(paste0("user_run_count_add_to_cart_buy_weekday_", dayOfWeekVal)),
                                                                        get(paste0("user_run_count_add_to_cart_buy_on_current_weekday")))]

    }

    clickstreamEventsDT[, (paste0("user_run_count_view_weekday_", dayOfWeekVal)) :=
                            cumSumNAIgnore(as.numeric(event %in% c("view") & day_of_week == dayOfWeekVal)), by = c("user_id")]

    clickstreamEventsDT[, (paste0("user_run_count_view_on_current_weekday")) := ifelse(day_of_week == dayOfWeekVal,
                            get(paste0("user_run_count_view_weekday_", dayOfWeekVal)), get(paste0("user_run_count_view_on_current_weekday")))]

}


clickstreamEventsDT[, user_new_item_interaction := as.numeric(!duplicated(current_item_id)), by = c("user_id")]

clickstreamEventsDT[, user_cum_number_of_unique_items := cumSumNAIgnore(user_new_item_interaction), by = c("user_id")]

clickstreamEventsDT[, user_new_item_interaction := NULL]

clickstreamEventsDT[, user_new_category_interaction := as.numeric(!duplicated(current_category_id)), by = c("user_id")]

clickstreamEventsDT[, user_cum_number_of_unique_categories := cumSumNAIgnore(user_new_category_interaction), by = c("user_id")]

clickstreamEventsDT[, user_new_category_interaction := NULL]

clickstreamEventsDT[, user_session_new_category_interaction := as.numeric(!duplicated(current_category_id)), by = c("user_id", "session_id")]

clickstreamEventsDT[, user_session_cum_number_of_unique_categories := cumSumNAIgnore(user_session_new_category_interaction),
                        by = c("user_id", "session_id")]

clickstreamEventsDT[, user_session_new_category_interaction := NULL]

clickstreamEventsDT[, item_new_user_interaction := as.numeric(!duplicated(user_id)), by = c("current_item_id")]

clickstreamEventsDT[, item_cum_number_of_unique_users := cumSumNAIgnore(item_new_user_interaction), by = c("current_item_id")]

clickstreamEventsDT[, item_new_user_interaction := NULL]

clickstreamEventsDT[, category_new_user_interaction := as.numeric(!duplicated(user_id)), by = c("current_category_id")]

clickstreamEventsDT[, category_cum_number_of_unique_users := cumSumNAIgnore(category_new_user_interaction), by = c("current_category_id")]

clickstreamEventsDT[, category_new_user_interaction := NULL]


clickstreamEventsExpandedDT[, rel_price_diff_current_future_item := log(current_item_price + 1) / log(future_item_current_price + 1)]

clickstreamEventsExpandedDT[, day_of_week := weekdays(timestamp)]

if (dataset != "diginetica") {

    try(clickstreamEventsExpandedDT[, hour_of_day := lubridate::hour(timestamp)])

}


if (dataset == "diginetica") {

    addNewTimeFeat <- c("day_of_week")

} else {

    addNewTimeFeat <- c("day_of_week")

    addNewTimeFeat <- c(addNewTimeFeat, "user_hour_of_day_quantile_025", "user_hour_of_day_quantile_05", "user_hour_of_day_quantile_075")

}

if (grepl("view", targetCase)) {

    addNewUserCategoryFeat <- c("user_category_relative_price_sensitivity_views", "mean_price_of_items_user_viewed_from_category",
                                "mean_price_of_items_all_users_viewed_from_category")

    if (dataset != "diginetica") {

        addNewUserFeat <- c("user_hour_of_day_quantile_025", "user_hour_of_day_quantile_05", "user_hour_of_day_quantile_075",
                                        "user_run_count_view_on_current_weekday",
                                        "user_cum_number_of_unique_items", "user_cum_number_of_unique_categories",
                                        "run_number_of_observations_by_user_last_3_months")

    } else {

        addNewUserFeat <- c(
                                        "user_run_count_view_on_current_weekday",
                                        "user_cum_number_of_unique_items", "user_cum_number_of_unique_categories",
                                        "run_number_of_observations_by_user_last_3_months")

    }

    addNewItemFeat <- c("item_cum_number_of_unique_users", "run_number_of_observations_by_item_last_3_months")

    addNewCategoryFeat <- c("category_cum_number_of_unique_users", "mean_price_of_items_in_category",
                            "diff_item_price_mean_price_of_items_in_category", "run_number_of_observations_by_category_last_3_months")

    addNewUserItemFeat <- c("prior_observation_time_of_item_by_user")

    addNewUserSessionItemFeat <- c("prior_observation_time_of_item_by_user_in_current_session")

    addNewSessionFeat <- c("run_number_of_observations_by_session_last_3_months")

} else {

    addNewUserCategoryFeat <- c("user_category_relative_price_sensitivity", "user_category_relative_price_sensitivity_views",
                                    "mean_price_of_items_user_selected_from_category", "mean_price_of_items_all_users_selected_from_category",
                                    "mean_price_of_items_user_viewed_from_category", "mean_price_of_items_all_users_viewed_from_category")

    if (dataset != "diginetica") {

        addNewUserFeat <- c("user_relative_price_sensitivity", "user_hour_of_day_quantile_025", "user_hour_of_day_quantile_05",
                            "user_hour_of_day_quantile_075", "user_purchase_hour_of_day_quantile_025", "user_purchase_hour_of_day_quantile_05",
                            "user_purchase_hour_of_day_quantile_075", "user_run_count_add_to_cart_buy_on_current_weekday",
                            "user_run_count_view_on_current_weekday", "user_cum_number_of_unique_items", "user_cum_number_of_unique_categories",
                            "run_number_of_observations_by_user_last_3_months")

    } else {

        addNewUserFeat <- c("user_relative_price_sensitivity",
                                        "user_run_count_add_to_cart_buy_on_current_weekday", "user_run_count_view_on_current_weekday",
                                        "user_cum_number_of_unique_items", "user_cum_number_of_unique_categories",
                                        "run_number_of_observations_by_user_last_3_months")

    }

    addNewItemFeat <- c("item_cum_number_of_unique_users", "run_number_of_observations_by_item_last_3_months")

    addNewCategoryFeat <- c("category_cum_number_of_unique_users", "mean_price_of_items_in_category",
                            "mean_price_of_selected_items_in_category", "diff_item_price_mean_price_of_items_in_category",
                            "diff_item_price_mean_price_of_selected_items_in_category", "run_number_of_observations_by_category_last_3_months")

    addNewUserItemFeat <- c("prior_observation_time_of_item_by_user")

    addNewUserSessionItemFeat <- c("prior_observation_time_of_item_by_user_in_current_session")

    addNewSessionFeat <- c("run_number_of_observations_by_session_last_3_months")

}


timeVar <- "timestamp_int"

userSessionVar <- "user_session_number"

currentByVars <- c("user_id", "current_category_id")

currentByExtVars <- c("user_id", "current_category_id", "timestamp_int", "event")

currentByExtVarsWithTime <- c("user_id", "current_category_id", "event", "timestamp_int")

userSessionVar <- c("user_id", "future_category_id")

userSessionVarExt <- c(userSessionVar, "event")

userSessionVarExtWithTime <- c(userSessionVar, "event", "timestamp_int")

entitiesFeatures <- addNewUserCategoryFeat

currentEntitiesAggDT <- eval(parse(text = paste0("clickstreamEventsDT[, list(
", paste0(paste0(entitiesFeatures, " = min(", entitiesFeatures, ")"), collapse = ", "),
"
), by = currentByExtVars]")))

futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVarExt, timeVar, userSessionVar), with = FALSE])

eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByExtVarsWithTime[1:length(currentByExtVarsWithTime)], "\"", " = ", "\"",
userSessionVarExtWithTime[1:length(userSessionVarExtWithTime)], "\"", collapse = ", "), "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

nonDuplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, userSessionVarExtWithTime, with = FALSE])),
                                                        c(userSessionVarExtWithTime, entitiesFeatures), with = FALSE]

tempAddClickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE],
                                                nonDuplicatedFutureEntitiesAggDT, by = userSessionVarExtWithTime,
                                                all.x = TRUE, all.y = FALSE, sort = FALSE)

clickstreamEventsExpandedDT[, (colnames(tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE])) :=
                                tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE]]


currentByVars <- c("user_id")

currentByExtVars <- c("user_id", "timestamp_int", "event")

currentByExtVarsWithTime <- c("user_id", "event", "timestamp_int")

userSessionVar <- c("user_id")

userSessionVarExt <- c(userSessionVar, "event")

userSessionVarExtWithTime <- c(userSessionVar, "event", "timestamp_int")

entitiesFeatures <- addNewUserFeat

currentEntitiesAggDT <- eval(parse(text = paste0("clickstreamEventsDT[, list(
", paste0(paste0(entitiesFeatures, " = min(", entitiesFeatures, ")"), collapse = ", "),
"
), by = currentByExtVars]")))

futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVarExt, timeVar, userSessionVar), with = FALSE])

eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByExtVarsWithTime[1:length(currentByExtVarsWithTime)], "\"", " = ", "\"",
userSessionVarExtWithTime[1:length(userSessionVarExtWithTime)], "\"", collapse = ", "), "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

nonDuplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, userSessionVarExtWithTime, with = FALSE])),
                                                        c(userSessionVarExtWithTime, entitiesFeatures), with = FALSE]

tempAddClickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE],
                                                nonDuplicatedFutureEntitiesAggDT, by = userSessionVarExtWithTime,
                                                all.x = TRUE, all.y = FALSE, sort = FALSE)

clickstreamEventsExpandedDT[, (colnames(tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE])) :=
                                tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE]]


currentByVars <- c("current_item_id")

currentByExtVars <- c("current_item_id", "timestamp_int", "event")

currentByExtVarsWithTime <- c("current_item_id", "event", "timestamp_int")

userSessionVar <- c("future_item_id")

userSessionVarExt <- c(userSessionVar, "event")

userSessionVarExtWithTime <- c(userSessionVar, "event", "timestamp_int")

entitiesFeatures <- addNewItemFeat

currentEntitiesAggDT <- eval(parse(text = paste0("clickstreamEventsDT[, list(
", paste0(paste0(entitiesFeatures, " = min(", entitiesFeatures, ")"), collapse = ", "),
"
), by = currentByExtVars]")))

futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVarExt, timeVar, userSessionVar), with = FALSE])

eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByExtVarsWithTime[1:length(currentByExtVarsWithTime)], "\"", " = ", "\"",
userSessionVarExtWithTime[1:length(userSessionVarExtWithTime)], "\"", collapse = ", "), "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

nonDuplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, userSessionVarExtWithTime, with = FALSE])),
                                                        c(userSessionVarExtWithTime, entitiesFeatures), with = FALSE]

tempAddClickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE],
                                                nonDuplicatedFutureEntitiesAggDT, by = userSessionVarExtWithTime,
                                                all.x = TRUE, all.y = FALSE, sort = FALSE)

clickstreamEventsExpandedDT[, (colnames(tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE])) :=
                                tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE]]


currentByVars <- c("current_category_id")

currentByExtVars <- c("current_category_id", "timestamp_int", "event")

currentByExtVarsWithTime <- c("current_category_id", "event", "timestamp_int")

userSessionVar <- c("future_category_id")

userSessionVarExt <- c(userSessionVar, "event")

userSessionVarExtWithTime <- c(userSessionVar, "event", "timestamp_int")

entitiesFeatures <- addNewCategoryFeat

currentEntitiesAggDT <- eval(parse(text = paste0("clickstreamEventsDT[, list(
", paste0(paste0(entitiesFeatures, " = min(", entitiesFeatures, ")"), collapse = ", "),
"
), by = currentByExtVars]")))

futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVarExt, timeVar, userSessionVar), with = FALSE])

eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByExtVarsWithTime[1:length(currentByExtVarsWithTime)], "\"", " = ", "\"",
userSessionVarExtWithTime[1:length(userSessionVarExtWithTime)], "\"", collapse = ", "), "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

nonDuplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, userSessionVarExtWithTime, with = FALSE])),
                                                        c(userSessionVarExtWithTime, entitiesFeatures), with = FALSE]

tempAddClickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE],
                                                nonDuplicatedFutureEntitiesAggDT, by = userSessionVarExtWithTime,
                                                all.x = TRUE, all.y = FALSE, sort = FALSE)

clickstreamEventsExpandedDT[, (colnames(tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE])) :=
                                tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE]]


currentByVars <- c("user_id", "current_item_id")

currentByExtVars <- c("user_id", "current_item_id", "timestamp_int", "event")

currentByExtVarsWithTime <- c("user_id", "current_item_id", "event", "timestamp_int")

userSessionVar <- c("user_id", "future_item_id")

userSessionVarExt <- c(userSessionVar, "event")

userSessionVarExtWithTime <- c(userSessionVar, "event", "timestamp_int")

entitiesFeatures <- addNewUserItemFeat

currentEntitiesAggDT <- eval(parse(text = paste0("clickstreamEventsDT[, list(
", paste0(paste0(entitiesFeatures, " = min(", entitiesFeatures, ")"), collapse = ", "),
"
), by = currentByExtVars]")))

futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVarExt, timeVar, userSessionVar), with = FALSE])

eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByExtVarsWithTime[1:length(currentByExtVarsWithTime)], "\"", " = ", "\"",
userSessionVarExtWithTime[1:length(userSessionVarExtWithTime)], "\"", collapse = ", "), "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

nonDuplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, userSessionVarExtWithTime, with = FALSE])),
                                                        c(userSessionVarExtWithTime, entitiesFeatures), with = FALSE]

tempAddClickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE],
                                                nonDuplicatedFutureEntitiesAggDT, by = userSessionVarExtWithTime,
                                                all.x = TRUE, all.y = FALSE, sort = FALSE)

all.equal(tempAddClickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE],
                                clickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE])

clickstreamEventsExpandedDT[, (colnames(tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE])) :=
                                tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE]]


currentByVars <- c("user_id", "session_id", "current_item_id")

currentByExtVars <- c("user_id", "session_id", "current_item_id", "timestamp_int", "event")

currentByExtVarsWithTime <- c("user_id", "session_id", "current_item_id", "event", "timestamp_int")

userSessionVar <- c("user_id",  "session_id", "future_item_id")

userSessionVarExt <- c(userSessionVar, "event")

userSessionVarExtWithTime <- c(userSessionVar, "event", "timestamp_int")

entitiesFeatures <- addNewUserSessionItemFeat

currentEntitiesAggDT <- eval(parse(text = paste0("clickstreamEventsDT[, list(
", paste0(paste0(entitiesFeatures, " = min(", entitiesFeatures, ")"), collapse = ", "),
"
), by = currentByExtVars]")))

futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVarExt, timeVar, userSessionVar), with = FALSE])

eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByExtVarsWithTime[1:length(currentByExtVarsWithTime)], "\"", " = ", "\"",
userSessionVarExtWithTime[1:length(userSessionVarExtWithTime)], "\"", collapse = ", "), "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

nonDuplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, userSessionVarExtWithTime, with = FALSE])),
                                                        c(userSessionVarExtWithTime, entitiesFeatures), with = FALSE]

tempAddClickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE],
                                                nonDuplicatedFutureEntitiesAggDT, by = userSessionVarExtWithTime,
                                                all.x = TRUE, all.y = FALSE, sort = FALSE)

clickstreamEventsExpandedDT[, (colnames(tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE])) :=
                                tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE]]


currentByVars <- c("session_id")

currentByExtVars <- c("session_id", "timestamp_int", "event")

currentByExtVarsWithTime <- c("session_id", "event", "timestamp_int")

userSessionVar <- c("session_id")

userSessionVarExt <- c(userSessionVar, "event")

userSessionVarExtWithTime <- c(userSessionVar, "event", "timestamp_int")

entitiesFeatures <- addNewSessionFeat

currentEntitiesAggDT <- eval(parse(text = paste0("clickstreamEventsDT[, list(
", paste0(paste0(entitiesFeatures, " = min(", entitiesFeatures, ")"), collapse = ", "),
"), by = currentByExtVars]")))

futureEntitiesAggDT <- unique(clickstreamEventsExpandedDT[, c(userSessionVarExt, timeVar, userSessionVar), with = FALSE])

eval(parse(text = paste0(
"futureEntitiesAggDT <- currentEntitiesAggDT[futureEntitiesAggDT, , on = c(",
paste0("\"", currentByExtVarsWithTime[1:length(currentByExtVarsWithTime)], "\"", " = ", "\"",
userSessionVarExtWithTime[1:length(userSessionVarExtWithTime)], "\"", collapse = ", "), "), roll = Inf]"
)))

setnames(futureEntitiesAggDT, currentByVars, userSessionVar)

nonDuplicatedFutureEntitiesAggDT <- futureEntitiesAggDT[!(duplicated(futureEntitiesAggDT[, userSessionVarExtWithTime, with = FALSE])),
                                                        c(userSessionVarExtWithTime, entitiesFeatures), with = FALSE]

tempAddClickstreamEventsExpandedDT <- merge(clickstreamEventsExpandedDT[, userSessionVarExtWithTime, with = FALSE],
                                                nonDuplicatedFutureEntitiesAggDT, by = userSessionVarExtWithTime,
                                                all.x = TRUE, all.y = FALSE, sort = FALSE)

clickstreamEventsExpandedDT[, (colnames(tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE]))
                                := tempAddClickstreamEventsExpandedDT[, -userSessionVarExtWithTime, with = FALSE]]

rm(currentEntitiesAggDT, futureEntitiesAggDT)


#______________________________
# josef.b.bauer at gmail.com

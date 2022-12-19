
# Path for the raw data before preprocessing.
dataPath <- paste0(basePath, "data", "/", dataset, "/", "raw", "/")

# Path to the preprocessed data serving as the input to the first phase session-based recommenders.
pathToPreparedData <- paste0(basePath, "data", "/", dataset, "/", "prepared", "/")

# Path where intermediate results are stored.
cachePath <- paste0(basePath, "output", "/", "cache", "/")

# Path where the first phase recommender predictions are stored.
firstPhaseOutputPath <- cachePath

resultsPath <- paste0(basePath, "output", "/", "results", "/")

if (!exists("codePath")) {
    codePath <- paste0(basePath, "code", "/", "py", "/")
}

# The dataset to be used, can also be specified in the main file
if (!exists("dataset")) {
    dataset <- "diginetica"
}

# The prediction target, either "buys" or "views", can also be specified in the main file.
# The case of "buys" may include add-to-cart events, based on the definition of targetEventsVec below.
if (!exists("targetCase")) {
    targetCase <- "buys"
}

# Whether all remaining items after the current time in a session should be considered in the evaluation
# or only the next items (see the paper for more details).
evalUseAllTrueItemsAfterNext <- FALSE

# Options for whether to use the cache files of features and existing predictions by the first phase recommenders
# (avoiding recomputation).
# These settings are better specified in the main file used for running the individual code blocks.
# useFeatureCache <- FALSE
# useCachedBaseRec <- TRUE

# Names for generated cache files.
cacheNameClickstreamEventsExpandedDT <- "clickstreamEventsExpandedDT"
cacheNameClickstreamEventsDT <- "clickstreamEventsDT"

# Names of the first phase recommenders, should be consistent with the names in the Python code.
# Reticulate and rpy2 can be used for exchanging variables between R and Python
# (this is currently also simplified in this repo for clarity).
if (!exists("baseRecModel")) {
    baseRecModel <- "s_sknn"
}
if (!exists("baseRecModel_2")) {
    baseRecModel_2 <- ifelse(baseRecModel == "s_sknn", "gru4rec", "s_sknn")
}

# Settings for namings of the output files.
fileSuffix <- paste0("_", targetCase)
addSuffix <- paste0("_", experimentId)
# If the preprocessing is performed in R, it serves as an identifier of the file for the Python code.
preparedDataPrefix <- paste0("events", fileSuffix, "_", "prepared")

if (!exists("mainSettingsDescr")) {
    mainSettingsDescr <- paste0(dataset, "_", targetCase, "_", baseRecModel, "_", baseRecModel_2)
}
if (!exists("extendedFileSuffix")) {
    extendedFileSuffix <- mainSettingsDescr
}
if (!exists("baseRecModel_1")) {
    baseRecModel_1 <- baseRecModel
}
if (!exists("baseRecFileSuffix")) {
    baseRecFileSuffix <- paste0(dataset, "_", targetCase, "_", baseRecModel)
}
if (!exists("baseRecFileSuffix_1")) {
    baseRecFileSuffix_1 <- paste0(dataset, "_", targetCase, "_", baseRecModel_1)
}
if (!exists("baseRecFileSuffix_2")) {
    baseRecFileSuffix_2 <- paste0(dataset, "_", targetCase, "_", baseRecModel_2)
}
baseRecFileName <- paste0("first_phase_recommender_prediction_results_", dataset, "_", baseRecModel, fileSuffix)
baseRecFileName_2 <- paste0("first_phase_recommender_prediction_results_", dataset, "_", baseRecModel_2, fileSuffix)
baseRecCombDTFilename <- paste0("base_rec_1_2_comb_DT", "_", mainSettingsDescr, ".rds")

storeResults <- TRUE

# Filters for the preprocessing.
useFilter <- TRUE
filterMinSessionLength <- 2
filterMinItemSupport <- 5

# If no session IDs are present, the threshold in minutes for defining new sessions.
thresholdMinutesNewSession <- 30

# Whether to use samples of observations for each session and how many.
sampleBySession <- TRUE
sampleBySessionSize <- 1
seedVal <- 1

# The number of candidate items from the first phase recommender predictions to be scored by the second phase GBM method.
numberOfTrainingItemsPerSession <- 200

# Whether to train several GBM models for different seeds in order to reduce variance and improve the predictions,
# as well as a specification of those seed values. See the GBM code for more options.
useGbmSeedAvg <- TRUE
gbmSeedVec <- 1:5

# Below are the dataset specific settings regarding the event names and targets,
# the training, validation and test subsets and features.

if (dataset == "retailrocket") {

    eventNames <- c("view", "add_to_cart", "buy")

    filterEventExpression <- expression(event %in% c("view", "add_to_cart", "buy"))

    targetEventsVec <- c("add_to_cart", "buy")

    if (grepl("view", targetCase)) {
        eventNames <- c("view")
        filterEventExpression <- expression(event %in% c("view"))
        targetEventsVec <- c("view")
    }

    timestepSubset <- 12:18
    validTimesteps <- 16
    testTimesteps <- 17:18

    numericItemPropertiesNames <- c("price")

    categoricalItemPropertiesNames <- c("available", "category_id")

    addBaseFeatNames <- c("price", "available_ind", "category_id")

    if (targetCase %in% c("views")) {

        includeFeatureNames <- c("predicted_future_item_rank_base_1", "predicted_future_item_rank_base_2",
                                 "category_cum_number_of_unique_users", "current_category_id", "current_item_available_ind",
                                 "current_item_id", "current_item_price", "day_of_week",
                                 "diff_item_price_mean_price_of_items_in_category", "future_category_id",
                                 "future_item_current_available_ind", "future_item_current_price", "future_item_id",
                                 "future_item_session_available_ind_diff", "future_item_session_price_diff",
                                 "item_cum_number_of_unique_users", "item_run_count_interaction", "item_run_count_view",
                                 "item_time_since_first_interaction", "item_time_since_first_view", "item_time_since_last_view",
                                 "item_time_since_previous_interaction", "item_time_window_views_last_4_weeks",
                                 "item_time_window_views_last_day", "item_time_window_views_last_hour",
                                 "item_time_window_views_last_week", "mean_price_of_items_all_users_viewed_from_category",
                                 "mean_price_of_items_in_category", "mean_price_of_items_user_viewed_from_category",
                                 "price_diff_current_future_item", "prior_observation_time_of_item_by_user",
                                 "run_number_of_observations_by_category_last_3_months",
                                 "run_number_of_observations_by_item_last_3_months",
                                 "run_number_of_observations_by_session_last_3_months",
                                 "run_number_of_observations_by_user_last_3_months", "same_category", "same_item",
                                 "session_interaction_number", "session_number_of_views", "session_time_since_start",
                                 "user_category_relative_price_sensitivity_views", "user_category_run_count_view",
                                 "user_category_session_interaction_diff", "user_category_time_since_first_interaction",
                                 "user_category_time_since_first_view", "user_category_time_since_previous_interaction",
                                 "user_cum_number_of_unique_categories", "user_cum_number_of_unique_items",
                                 "user_future_item_sessions_first_interaction", "user_future_item_sessions_last_interaction",
                                 "user_hour_of_day_quantile_025", "user_hour_of_day_quantile_05", "user_id",
                                 "user_item_run_count_view", "user_item_session_interaction_diff",
                                 "user_item_time_since_first_interaction", "user_item_time_since_first_view",
                                 "user_item_time_since_last_view", "user_item_time_since_previous_interaction",
                                 "user_run_count_view_10", "user_run_count_view_100", "user_run_count_view_on_current_weekday",
                                 "user_session_number", "user_time_since_last_view", "user_time_since_previous_interaction")

    }

    if (targetCase %in% c("buys")) {

        includeFeatureNames <- c("user_id", "current_item_id", "user_session_number", "session_time_since_start",
                                 "session_interaction_number", "session_number_of_views", "session_number_of_add_to_carts",
                                 "session_number_of_buys", "current_item_price", "current_item_available_ind", "current_category_id",
                                 "future_item_id", "future_category_id", "future_item_current_available_ind", "current_event_type",
                                 "same_category", "price_diff_current_future_item", "user_item_time_since_previous_interaction",
                                 "user_item_run_count_interaction", "user_item_run_count_view", "user_item_run_count_add_to_cart",
                                 "user_item_run_count_buy", "user_item_time_since_first_view", "user_item_time_since_first_buy",
                                 "user_item_time_since_last_view", "user_item_time_since_last_buy",
                                 "user_item_session_interaction_diff", "user_category_time_since_first_interaction",
                                 "user_category_time_since_previous_interaction", "user_category_run_count_interaction",
                                 "user_category_run_count_view", "user_category_run_count_add_to_cart",
                                 "user_category_run_count_buy", "user_category_time_since_first_view",
                                 "user_category_time_since_first_add_to_cart", "user_category_time_since_first_buy",
                                 "user_category_time_since_last_view", "user_category_time_since_last_add_to_cart",
                                 "user_category_time_since_last_buy", "user_category_session_interaction_diff",
                                 "future_item_session_price_diff", "future_item_session_available_ind_diff",
                                 "user_future_item_sessions_last_interaction", "user_future_item_sessions_first_interaction",
                                 "user_time_since_previous_interaction", "user_run_count_view_10", "user_run_count_add_to_cart_10",
                                 "user_run_count_buy_10", "user_run_count_view_100", "user_run_count_add_to_cart_100",
                                 "user_run_count_buy_100", "user_time_since_last_view", "user_time_since_last_add_to_cart",
                                 "user_time_since_last_buy", "item_time_since_first_interaction",
                                 "item_time_since_previous_interaction", "item_run_count_interaction", "item_run_count_view",
                                 "item_run_count_add_to_cart", "item_run_count_buy", "item_time_since_first_view",
                                 "item_time_since_first_add_to_cart", "item_time_since_last_view",
                                 "item_time_since_last_add_to_cart", "item_time_since_last_buy", "item_time_window_views_last_hour",
                                 "item_time_window_add_to_carts_last_hour", "item_time_window_buys_last_hour",
                                 "item_time_window_views_last_day", "item_time_window_add_to_carts_last_day",
                                 "item_time_window_buys_last_day", "item_time_window_views_last_week",
                                 "item_time_window_add_to_carts_last_week", "item_time_window_buys_last_week",
                                 "item_time_window_views_last_4_weeks", "item_time_window_add_to_carts_last_4_weeks",
                                 "item_time_window_buys_last_4_weeks", "user_category_relative_price_sensitivity",
                                 "user_category_relative_price_sensitivity_views", "mean_price_of_items_user_selected_from_category",
                                 "mean_price_of_items_all_users_selected_from_category", "mean_price_of_items_user_viewed_from_category",
                                 "user_relative_price_sensitivity", "user_hour_of_day_quantile_025", "user_hour_of_day_quantile_05",
                                 "user_hour_of_day_quantile_075", "user_purchase_hour_of_day_quantile_025",
                                 "user_purchase_hour_of_day_quantile_05", "user_purchase_hour_of_day_quantile_075",
                                 "user_run_count_add_to_cart_buy_on_current_weekday", "user_run_count_view_on_current_weekday",
                                 "user_cum_number_of_unique_items", "user_cum_number_of_unique_categories",
                                 "run_number_of_observations_by_user_last_3_months", "item_cum_number_of_unique_users",
                                 "run_number_of_observations_by_item_last_3_months", "category_cum_number_of_unique_users",
                                 "mean_price_of_items_in_category", "mean_price_of_selected_items_in_category",
                                 "diff_item_price_mean_price_of_items_in_category",
                                 "diff_item_price_mean_price_of_selected_items_in_category",
                                 "run_number_of_observations_by_category_last_3_months",
                                 "prior_observation_time_of_item_by_user", "prior_observation_time_of_item_by_user_in_current_session",
                                 "run_number_of_observations_by_session_last_3_months", "day_of_week",
                                 "predicted_future_item_rank_base_1", "predicted_future_item_rank_base_2")

    }

    # Default settings can be overwritten.
    useGbmSeedAvg <- FALSE

    excludeFeatureNames <- ""

    categoricalVariables <- character(0)

}

if (dataset == "diginetica") {

    eventNames <- c("view", "buy")

    filterEventExpression <- expression(event %in% c("view", "buy"))

    targetEventsVec <- c("buy")
    if (grepl("view", targetCase)) {
        eventNames <- c("view")
        filterEventExpression <- expression(event %in% c("view"))
        targetEventsVec <- c("view")
    }

    timestepSubset <- 0:18
    validTimesteps <- 13:14
    testTimesteps <- 15:18

    numericItemPropertiesNames <- c("price")

    categoricalItemPropertiesNames <- c("category_id")

    addBaseFeatNames <- c("price", "category_id")

    if (targetCase %in% c("views")) {

        includeFeatureNames <- c("user_id", "current_item_id", "user_session_number", "session_interaction_number",
                                 "session_number_of_views", "current_item_price", "current_category_id", "future_item_id",
                                 "future_category_id", "future_item_current_price", "current_event_type",
                                 "price_diff_current_future_item", "user_item_time_since_first_interaction",
                                 "user_item_time_since_previous_interaction", "user_item_run_count_interaction",
                                 "user_item_run_count_view", "user_item_time_since_last_view", "user_item_session_interaction_diff",
                                 "user_category_time_since_first_interaction", "user_category_time_since_previous_interaction",
                                 "user_category_run_count_interaction", "user_category_time_since_last_view",
                                 "user_category_session_interaction_diff", "user_future_item_sessions_last_interaction",
                                 "user_future_item_sessions_first_interaction", "user_time_since_previous_interaction",
                                 "user_run_count_view_10", "user_run_count_view_100", "user_time_since_last_view",
                                 "item_time_since_first_interaction", "item_time_since_previous_interaction",
                                 "item_run_count_interaction", "item_run_count_view", "item_time_since_first_view",
                                 "item_time_since_last_view", "item_time_window_views_last_hour", "item_time_window_views_last_day",
                                 "item_time_window_views_last_week", "item_time_window_views_last_4_weeks",
                                 "user_category_relative_price_sensitivity_views", "mean_price_of_items_user_viewed_from_category",
                                 "mean_price_of_items_all_users_viewed_from_category", "user_run_count_view_on_current_weekday",
                                 "user_cum_number_of_unique_items", "user_cum_number_of_unique_categories",
                                 "run_number_of_observations_by_user_last_3_months", "item_cum_number_of_unique_users",
                                 "run_number_of_observations_by_item_last_3_months", "category_cum_number_of_unique_users",
                                 "mean_price_of_items_in_category", "diff_item_price_mean_price_of_items_in_category",
                                 "run_number_of_observations_by_category_last_3_months", "prior_observation_time_of_item_by_user",
                                 "prior_observation_time_of_item_by_user_in_current_session",
                                 "run_number_of_observations_by_session_last_3_months", "day_of_week",
                                 "predicted_future_item_rank_base_1", "predicted_future_item_rank_base_2")

    }

    if (targetCase %in% c("buys")) {

        includeFeatureNames <- c("user_id", "current_item_id", "user_session_number", "session_time_since_start",
                                 "session_interaction_number", "session_number_of_views", "session_number_of_buys",
                                 "current_item_price", "current_category_id", "future_item_id", "future_category_id",
                                 "future_item_current_price", "current_event_type", "same_category",
                                 "price_diff_current_future_item", "user_item_time_since_first_interaction",
                                 "user_item_time_since_previous_interaction", "user_item_run_count_interaction",
                                 "user_item_run_count_buy", "user_item_time_since_first_view", "user_item_time_since_first_buy",
                                 "user_item_time_since_last_view", "user_item_time_since_last_buy",
                                 "user_item_session_interaction_diff", "user_category_time_since_previous_interaction",
                                 "user_category_run_count_interaction", "user_category_run_count_buy",
                                 "user_category_time_since_first_view", "user_category_time_since_first_buy",
                                 "user_category_time_since_last_view", "user_category_time_since_last_buy",
                                 "user_category_session_interaction_diff", "future_item_session_price_diff",
                                 "user_future_item_sessions_last_interaction", "user_future_item_sessions_first_interaction",
                                 "user_time_since_previous_interaction", "user_run_count_view_10", "user_run_count_buy_10",
                                 "user_run_count_view_100", "user_run_count_buy_100", "user_time_since_last_view",
                                 "user_time_since_last_buy", "item_time_since_first_interaction",
                                 "item_time_since_previous_interaction", "item_run_count_interaction",
                                 "item_run_count_view", "item_run_count_buy", "item_time_since_first_view",
                                 "item_time_since_first_buy", "item_time_since_last_view", "item_time_since_last_buy",
                                 "item_time_window_views_last_hour", "item_time_window_buys_last_hour",
                                 "item_time_window_views_last_day", "item_time_window_buys_last_day",
                                 "item_time_window_views_last_week", "item_time_window_buys_last_week",
                                 "item_time_window_views_last_4_weeks", "item_time_window_buys_last_4_weeks",
                                 "user_category_relative_price_sensitivity", "user_category_relative_price_sensitivity_views",
                                 "mean_price_of_items_user_selected_from_category", "mean_price_of_items_user_viewed_from_category",
                                 "mean_price_of_items_all_users_viewed_from_category", "user_relative_price_sensitivity",
                                 "user_run_count_add_to_cart_buy_on_current_weekday", "user_run_count_view_on_current_weekday",
                                 "user_cum_number_of_unique_items", "user_cum_number_of_unique_categories",
                                 "run_number_of_observations_by_user_last_3_months", "item_cum_number_of_unique_users",
                                 "run_number_of_observations_by_item_last_3_months", "category_cum_number_of_unique_users",
                                 "mean_price_of_items_in_category", "mean_price_of_selected_items_in_category",
                                 "diff_item_price_mean_price_of_items_in_category",
                                 "diff_item_price_mean_price_of_selected_items_in_category",
                                 "run_number_of_observations_by_category_last_3_months", "prior_observation_time_of_item_by_user",
                                 "prior_observation_time_of_item_by_user_in_current_session",
                                 "run_number_of_observations_by_session_last_3_months",
                                 "predicted_future_item_rank_base_1", "predicted_future_item_rank_base_2")

    }

    excludeFeatureNames <- ""

    categoricalVariables <- character(0)

}

# The default filter settings before the evaluation that only the items also included in the first phase recommenders are considered
# and that cases are excluded when the recommendations coincide with the currently interacted item.
sessionTimeRecsFilterExpression <- expression(is_in_base_rec_recs == 1 & current_item_id != future_item_id)
filterSameItem <- TRUE


# Different experiments can be configured with associated IDs. Mlflow is useful, but removed for simplicity.
if (exists("experimentId")) {

    if (experimentId == 1) {

        evalUseAllTrueItemsAfterNext <- FALSE

    }

    if (experimentId == 2) {

        evalUseAllTrueItemsAfterNext <- TRUE

        constraintSampleBySessionType <- "last_view_before_first_add_to_cart_buy"

        extendedFileSuffix <- mainSettingsDescr
        extendedFileSuffix <- paste0(extendedFileSuffix, "_", "evalUseAllTrueItemsAfterNext", "_", evalUseAllTrueItemsAfterNext)

    }

}


#______________________________
# josef.b.bauer at gmail.com

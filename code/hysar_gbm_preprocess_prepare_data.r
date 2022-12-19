
if (dataset == "retailrocket") {

    events <- fread(paste0(dataPath, "events.csv"))

    categoryTree <- fread(paste0(dataPath, "category_tree.csv"))

    itemPropertiesPart1 <- fread(paste0(dataPath, "item_properties_part1.csv"))
    itemPropertiesPart2 <- fread(paste0(dataPath, "item_properties_part2.csv"))
    itemProperties <- rbind(itemPropertiesPart1, itemPropertiesPart2)
    rm(itemPropertiesPart1, itemPropertiesPart2); gc()

    itemProperties[, is_text := 0]
    itemProperties[grep(" ", value), is_text := 1]

    itemProperties$timestamp <- as.POSIXct(itemProperties$timestamp/1000, origin = '1970-01-01')

    itemPropertiesDT <- itemProperties

    setnames(itemPropertiesDT, c("timestamp", "item_id", "property", "value", "is_text"))

    itemPropertiesDT[, timestamp_int := as.integer(timestamp)]
    itemPropertiesDT[, timestamp := NULL]

    setorder(itemPropertiesDT, "item_id", "timestamp_int")


    itemPropertiesNumericDT <- itemPropertiesDT[is_text == 0 & substr(value, 1, 1) == "n"]

    itemPropertiesNumericDT[, value := sub("n", "", value)]

    itemPropertiesNumericDT[value == "Infinity", value := "Inf"]

    itemPropertiesNumericDT[, value := as.numeric(value)]

    itemPropertiesNumericDT[property == "790", property := "price"]

    itemPropertiesNumericDT[property == "price", value := value/100]

    itemPropertiesNumericWideDT <- dcast(itemPropertiesNumericDT, item_id + timestamp_int ~ property, value.var = c("value"))

    for (column in numericItemPropertiesNames) {

        itemPropertiesNumericWideDT[, (column) := na.locf(get(column), na.rm = FALSE), by = "item_id"]

    }

    itemPropertiesCategoricalDT <- itemPropertiesDT[is_text == 0 & substr(value, 1, 1) != "n"]

    itemPropertiesCategoricalDT[property == "categoryid", property := "category_id"]

    itemPropertiesCategoricalWideDT <- dcast(itemPropertiesCategoricalDT[property %in% categoricalItemPropertiesNames],
                                                item_id + timestamp_int ~ property, value.var = c("value"))

    for (column in categoricalItemPropertiesNames) {

        itemPropertiesCategoricalWideDT[, (column) := na.locf(get(column), na.rm = FALSE), by = "item_id"]

    }

    itemPropertiesCategoricalWideDT[, category_id := na.locf(category_id, na.rm = FALSE, fromLast = TRUE), by = "item_id"]

    events$timestamp <- as.POSIXct(events$timestamp/1000, origin = '1970-01-01')

    clickstreamEventsDT <- events

    setnames(clickstreamEventsDT, c("timestamp", "user_id", "event", "item_id", "transaction_id"))

    setorder(clickstreamEventsDT, "user_id", "timestamp")

    clickstreamEventsDT[event == "addtocart", event := "add_to_cart"]
    clickstreamEventsDT[event == "transaction", event := "buy"]

    clickstreamEventsDT[, timestamp_int := as.integer(timestamp)]

    clickstreamEventsDT[, diff_time := ifelse(rep(length(timestamp_int) > 1, length(timestamp_int)),
                                                c(NA, (timestamp_int[2:.N] - timestamp_int[1:(.N-1)])/60), as.numeric(NA)), by = c("user_id")]

    clickstreamEventsDT[, flag_time_gap := ifelse(diff_time > thresholdMinutesNewSession & !is.na(diff_time), 1, 0)]

    clickstreamEventsDT[, cumsum_flag_time_gap := cumSumNAIgnore(flag_time_gap), by = c("user_id")]

    clickstreamEventsDT[, user_session_number := cumsum_flag_time_gap + 1, by = c("user_id")]

    clickstreamEventsDT[, session_id := cumSumNAIgnore(!duplicated(clickstreamEventsDT[, list(user_id, user_session_number)])) - 1]

    clickstreamEventsDT <- clickstreamEventsDT[!duplicated(clickstreamEventsDT[, c("session_id", "timestamp_int", "item_id", "event"), with = FALSE])]

}


if (dataset == "diginetica") {

    itemViews <- fread(paste0(dataPath, "train-item-views.csv"))

    setnames(itemViews, c("session_id", "user_id", "item_id", "timeframe", "date_value"))

    itemPurchases <- fread(paste0(dataPath, "train-purchases.csv"))

    setnames(itemPurchases, c("session_id", "user_id", "timeframe", "date_value", "order_number", "item_id"))

    setcolorder(itemPurchases, c("session_id", "user_id", "item_id", "timeframe", "date_value", "order_number"))

    itemViews[, event := "view"]
    itemPurchases[, event := "buy"]

    clickstreamEventsDT <- rbind(itemViews, itemPurchases[, -c("order_number"), with = FALSE])

    clickstreamEventsDT[, original_session_id := session_id]
    clickstreamEventsDT[, original_user_id := user_id]

    setcolorder(clickstreamEventsDT, c("timeframe", "session_id", "original_session_id", "user_id",
                                        "original_user_id", "event", "item_id", "date_value"))

    setorder(clickstreamEventsDT, "user_id", "date_value", "timeframe")

    clickstreamEventsDT[, date_value := as.Date(date_value)]

    clickstreamEventsDT[, timestamp := date_value]

    clickstreamEventsDT[, timeframe_int := round(timeframe/1000)]

    clickstreamEventsDT[, timestamp_int := as.numeric(date_value)*24*60*60 + timeframe_int]

    clickstreamEventsDT$timeframe <- as.POSIXct(clickstreamEventsDT$timeframe/1000, origin = clickstreamEventsDT$date_value)

    setorder(clickstreamEventsDT, user_id, session_id, timestamp_int)

    clickstreamEventsDT[, diff_time := ifelse(rep(length(timestamp_int) > 1, length(timestamp_int)),
                                                c(NA, (timestamp_int[2:.N] - timestamp_int[1:(.N-1)])/(60)), as.numeric(NA)), by = c("user_id")]

    clickstreamEventsDT[, diff_session_id := ifelse(rep(length(session_id) > 1, length(session_id)),
                                                        c(NA, (session_id[2:.N] - session_id[1:(.N-1)])), as.numeric(NA)), by = c("user_id")]

    clickstreamEventsDT[, flag_new_session := ifelse(diff_session_id != 0 & !is.na(diff_session_id), 1, 0)]

    clickstreamEventsDT[, cumsum_flag_new_session := cumSumNAIgnore(flag_new_session), by = c("user_id")]

    clickstreamEventsDT[, user_session_number := cumsum_flag_new_session + 1, by = c("user_id")]

    clickstreamEventsDT[, orig_row_id := 1:.N]

    clickstreamEventsDT[, min_time_by_session := min(timestamp_int), by = "session_id"]

    setorder(clickstreamEventsDT, user_id, min_time_by_session, session_id, timestamp_int)

    clickstreamEventsDT[, diff_session_id := ifelse(rep(length(session_id) > 1, length(session_id)),
                                                    c(NA, (session_id[2:.N] - session_id[1:(.N-1)])), as.numeric(NA)), by = c("user_id")]

    clickstreamEventsDT[, flag_new_session := ifelse(diff_session_id != 0 & !is.na(diff_session_id), 1, 0)]

    clickstreamEventsDT[, cumsum_flag_new_session := cumSumNAIgnore(flag_new_session), by = c("user_id")]

    clickstreamEventsDT[, user_session_number := cumsum_flag_new_session + 1, by = c("user_id")]

    setorder(clickstreamEventsDT, orig_row_id)

    clickstreamEventsDT <- clickstreamEventsDT[!duplicated(clickstreamEventsDT[, c("session_id", "timestamp_int", "item_id", "event"), with = FALSE])]

    clickstreamEventsDT <- clickstreamEventsDT[!is.na(user_id)]

    itemPropertiesDT <- fread(paste0(dataPath, "products.csv"), select = c("itemId", "pricelog2"))

    itemPropertiesDT[, price := 2^pricelog2]

    itemPropertiesDT[, pricelog2 := NULL]

    setnames(itemPropertiesDT, c("item_id", "price"))

    itemCatDT <- fread(paste0(dataPath, "product-categories.csv"))

    setnames(itemCatDT, c("item_id", "category_id"))

    itemPropertiesDT <- merge(itemPropertiesDT, itemCatDT, by = "item_id", all.x = TRUE, all.y = TRUE, sort = FALSE)

    saveRDS(itemPropertiesDT, paste0(cachePath, "itemPropertiesDT", "_", extendedFileSuffix, ".rds"))

}


if (useFilter) {

    clickstreamEventsDT <- clickstreamEventsDT[eval(filterEventExpression)]

    sessionsLengthsDT <- clickstreamEventsDT[, list(session_length = .N), by = "session_id"]

    itemsSupportDT <- clickstreamEventsDT[, list(item_support = .N), by = "item_id"]

    includedItems <- itemsSupportDT[item_support >= filterMinItemSupport]$item_id

    clickstreamEventsDT <- clickstreamEventsDT[item_id %in% includedItems]

    sessionsLengthsDT <- clickstreamEventsDT[, list(session_length = .N), by = "session_id"]

    included_sessions <- sessionsLengthsDT[session_length >= filterMinSessionLength]$session_id

    clickstreamEventsDT <- clickstreamEventsDT[session_id %in% included_sessions]

}


saveRDS(clickstreamEventsDT, paste0(cachePath, cacheNameClickstreamEventsDT, "_", extendedFileSuffix, ".rds"))

allDataExportDT <- clickstreamEventsDT[, list(Time = timestamp_int, UserId = user_id, ItemId = item_id, SessionId = session_id)]

fwrite(allDataExportDT, paste0(pathToPreparedData, preparedDataPrefix, "_", "all_data.txt"), sep = "\t")


#______________________________
# josef.b.bauer at gmail.com

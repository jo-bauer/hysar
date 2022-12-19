
# Main code for running the Hybrid Session-Aware Recommendation method.
# See the explanations here and also in the individual source code files for the meanings and options.

# Path settings.
basePath <- "~/hysar_gbm/"
codePath <- paste0(basePath, "code", "/")
firstPhaseCodePath <- paste0(basePath, "code", "/", "first_phase_algorithms", "/")
configPath <- paste0(basePath, "code", "/", "config", "/")

# The dataset to be used. All datasets use the same code base including the logic for the creation of many features.
# Specifics regarding data preprocessing and additional available features can be realized with case distinctions.
dataset <- "diginetica"

# Definition of the prediction target. Either "views" or "buys", the later being an abbreviation and also including add-to-cart events.
# Similar targets can be defined in an analogous way.
targetCase <- "buys"

if (!dir.exists(basePath)) {
    try(dir.create(basePath))
}

if (!dir.exists(codePath)) {
    try(dir.create(codePath))
}

if (!dir.exists(firstPhaseCodePath)) {
    try(dir.create(firstPhaseCodePath))
}

if (!dir.exists(configPath)) {
    try(dir.create(configPath))
}

# Loop over all experiments/cases as defined in the configuration file.

for (experimentId in c(1)) {

    objectsToKeep <- c("basePath", "codePath", "firstPhaseCodePath", "configPath", "logsPath", "savelogfile", "experimentId", "dataset", "targetCase")

    print(paste0("Start run with experimentId = ", experimentId))

    rm(list = setdiff(ls(), objectsToKeep))
    gc()

    # Get the individual file names with path.
    try(source(paste0(codePath, "hysar_gbm_get_links.r")))

    # Load the required libraries or try to install them if they are missing.
    try(source(paste0(codePath, link_hysar_gbm_load_libraries_r)))

    # Run the file with most of the experiment settings.
    try(source(paste0(configPath, link_hysar_gbm_config_r)))

    # Helper functions (e.g., for the feature engineering).
    try(source(paste0(codePath, link_hysar_gbm_helper_functions_r)))

    # If first phase recommender result and feature cache files are already available for the given setting,
    # these options can be set to TRUE to avoid recomputing them.
    useCachedBaseRec <- FALSE
    useFeatureCache <- FALSE

    if (!useFeatureCache) {

        # Run the preprocessing of the raw data for the use by the first phase recommender methods. It usually depends of the dataset
        # in question and templates for this purpose can be found in the source file.
        try(source(paste0(codePath, link_hysar_gbm_preprocess_prepare_data_r)))

        if (!file.exists(paste0(firstPhaseOutputPath, baseRecFileName, ".csv")) | !file.exists(paste0(firstPhaseOutputPath, baseRecFileName_2, ".csv")) |
            !useCachedBaseRec) {

            # Running the first and afterwards the second first phase session-based recommender in Python. It can also be done directly
            # from Python with the corresponding settings.

            system(paste0("python ", codePath, "hysar_gbm_run_first_phase_recommender.py", " --base_path ", basePath,
                            " --dataset ", dataset, " --method ", baseRecModel, " --target_case ", targetCase))

            system(paste0("python ", codePath, "hysar_gbm_run_first_phase_recommender.py", " --base_path ", basePath,
                            " --dataset ", dataset, " --method ", baseRecModel_2, " --target_case ", targetCase))

        }

        # Main file for generating a huge set of features based on different principles and entities. This code also calls other
        # submodules performing other feature calculations.
        # See these source files for templates and suggestions of how to add further features.
        try(source(paste0(codePath, link_hysar_gbm_preprocess_generate_features_r)))

        baseRecDT <- readRDS(paste0(cachePath, baseRecCombDTFilename))

        if (!exists("clickstreamEventsExpandedDT")) {

            clickstreamEventsExpandedDT <- readRDS(paste0(cachePath, cacheNameClickstreamEventsExpandedDT, "_", extendedFileSuffix, ".rds"))

        }

    } else {

        clickstreamEventsDT <- readRDS(paste0(cachePath, cacheNameClickstreamEventsDT, "_", extendedFileSuffix, ".rds"))

        if (!file.exists(paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_1, ".rds")) |
            !file.exists(paste0(cachePath, "baseRecDT", "_", baseRecFileSuffix_2, ".rds"))) {

            try(source(paste0(codePath, link_hysar_gbm_preprocess_generate_features_r)))

        }

        baseRecDT <- readRDS(paste0(cachePath, baseRecCombDTFilename))

        try(setnames(baseRecDT, "from_item_id", "current_item_id", skip_absent = TRUE))

        eventTypes <- unique(clickstreamEventsDT$event)

        clickstreamEventsExpandedDT <- readRDS(paste0(cachePath, cacheNameClickstreamEventsExpandedDT, "_", extendedFileSuffix, ".rds"))

    }

    # Code for running the second phase GBM method. For more details and possible adaptions see the source for it.
    try(source(paste0(codePath, link_hysar_gbm_train_model_r)))

}


#______________________________
# josef.b.bauer at gmail.com

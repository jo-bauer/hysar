
allLibrariesVec <- c(
"data.table",
"Rcpp",
"Matrix",
"cumstats",
"reticulate",
"readr",
"TTR",
"boot",
"zoo",
"RSQLite",
"jsonlite",
"purrr",
"readxl",
"stringr",
"lightgbm",
"dplyr",
"parallel",
"mlflow",
"caTools",
"bit64"
)

newLibrariesVec <- allLibrariesVec[!(allLibrariesVec %in% installed.packages()[,"Package"])]

if (length(newLibrariesVec) > 0) {

    try(install.packages(newLibrariesVec, dependencies = TRUE))

}

tryRequire <- function (x) {try(require(x, character.only = TRUE))}

lapply(allLibrariesVec, tryRequire)


#______________________________
# josef.b.bauer at gmail.com

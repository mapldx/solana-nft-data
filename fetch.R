library(tidyverse)
library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(lubridate)

base_url <- "https://api-mainnet.magiceden.dev/v2/collections"

offsets <- seq(from = 0, to = 20000, by = 20)
limit <- 500

responses <- list()

for (offset in offsets) {
  url <- modify_url(base_url, query = list(offset = offset, limit = limit))
  response <- GET(url)
  
  if (status_code(response) == 200) {
    responses[[length(responses) + 1]] <- content(response, "parsed")
    cat("Success: Retrieved data for offset", offset, "\n")
  } else {
    cat("Request failed with status code:", status_code(response), "\n")
    Sys.sleep(1)
  }
}

responses <- unlist(responses, recursive = FALSE)
response_df <- bind_rows(lapply(responses, function(x) {
  if (length(x) > 0) {
    as.data.frame(t(x), stringsAsFactors = FALSE)
  }
}))

with_twitter <- subset(response_df, select = c(symbol, name, twitter, categories, isBadged, isFlagged))

with_twitter$symbol <- unlist(with_twitter$symbol)
with_twitter$name <- unlist(with_twitter$name)
with_twitter$twitter <- sapply(with_twitter$twitter, function(x) ifelse(is.null(x), "", x))
with_twitter$twitter <- unlist(with_twitter$twitter)
with_twitter$categories <- sapply(with_twitter$categories, paste, collapse = ",")
with_twitter$isFlagged <- sapply(with_twitter$isFlagged, function(x) ifelse(is.null(x), FALSE, x))
with_twitter$isFlagged <- unlist(with_twitter$isFlagged)
with_twitter$isFlagged <- unlist(with_twitter$isFlagged)

with_twitter <- with_twitter[!is.na(with_twitter$twitter) & with_twitter$twitter != "",]

with_twitter <- with_twitter %>% 
  mutate(username = str_extract(twitter, "[^/]+$"))

with_twitter$username <- gsub("\\?.*", "", with_twitter$username)

fetch_data <- function(symbol) {
  message(sprintf("Processing symbol: %s", symbol))
  
  # Query the first endpoint
  query1 <- sprintf('{"query":"query CollectionStats($slug: String!) {\\n  instrumentTV2(slug: $slug) {\\n    slugMe\\n    statsOverall {\\n      floorPrice\\n    }\\n    firstListDate\\n  }\\n}","variables":{"slug":"%s"}}', symbol)
  response1 <- POST(
    url = "https://api.tensor.so/graphql",
    add_headers(
      "content-type" = "application/json",
      "X-TENSOR-API-KEY" = ""
    ),
    body = query1
  )
  result1 <- content(response1, "parsed")
  
  # Query the second endpoint
  query2 <- sprintf('symbol=%s/SOL&resolution=1D&from=1&to=1681358400&countback=2880', symbol)
  response2 <- GET(
    url = "https://api-tradingview.tensor.so/tv/history",
    query = query2,
    add_headers(
      "X-TENSOR-API-KEY" = ""
    )
  )
  result2 <- content(response2, "text")
  
  # Combine the results into a single list
  list(stats = result1$data$instrumentTV2$statsOverall$floorPrice,
       first_list_date = result1$data$instrumentTV2$firstListDate,
       trading_view_history = result2)
}

fetch_data_safe <- function(symbol) {
  tryCatch(
    fetch_data(symbol),
    error = function(e) {
      message(sprintf("Error processing symbol %s: %s", symbol, e$message))
      return(NULL)
    }
  )
}

with_twitter$stats <- lapply(with_twitter$symbol, fetch_data_safe)
library(tidyverse)
library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(lubridate)

valid_twitter <- read.csv("./followers.csv")
valid_twitter$followers_count <- as.numeric(valid_twitter$followers_count)

match_df <- with_twitter %>%
  right_join(valid_twitter, by = "username", relationship = "many-to-many")

match_df <- inner_join(valid_twitter, with_twitter, by = "username", relationship = "many-to-many")
match_df <- unique(match_df)

data_df <- match_df %>%
  mutate(stats = map(stats, ~{
    list(
      stats = if(is.null(.x$stats)) NA else .x$stats,
      first_list_date = if(is.null(.x$first_list_date)) NA else .x$first_list_date,
      trading_view_history = if(is.null(.x$trading_view_history)) NA else .x$trading_view_history
    )
  })) %>%
  unnest_wider(stats)

data_df <- data_df %>% filter(!is.na(stats))

data_df <- data_df %>%
  mutate(floor_price = round(as.numeric(stats) / 1e9, 2)) %>%
  select(-stats)

process_trade_history <- function(trade_history) {
  # Parse the JSON string
  trade_data <- fromJSON(trade_history)
  
  # Check if there's more than one "t" value
  more_than_1m <- length(trade_data$t) >= 2
  
  # Check if the "t" array is empty
  if (length(trade_data$t) == 0) {
    return(list(NA, NA, more_than_1m, NA))
  }
  
  # Find the index of the highest "h" value
  high_index <- which.max(trade_data$h)
  
  # Extract the corresponding "t" value
  high_time <- trade_data$t[high_index]
  
  # Extract the highest "h" value
  high_price <- trade_data$h[high_index]
  
  # Extract the oldest "o" value
  oldest_open <- trade_data$o[1]
  
  # Return the result as a list
  return(list(high_price, high_time, more_than_1m, oldest_open))
}

data_df <- data_df %>%
  rowwise() %>%
  mutate(trade_result = list(process_trade_history(trading_view_history))) %>%
  ungroup() %>%
  mutate(trade_high_price = map_dbl(trade_result, 1),
         trade_high_time = map_dbl(trade_result, 2),
         more_than_1m = map_lgl(trade_result, 3),
         first_open = map_dbl(trade_result, 4)) %>%
  select(-trade_result)

more_than_1m <- data_df[data_df$more_than_1m == TRUE, c("symbol", "categories", "twitter", 
                                               "followers_count", "first_list_date", 
                                               "floor_price", "trade_high_price", 
                                               "trade_high_time", "first_open")]

more_than_1m <- more_than_1m %>% filter(!is.na(trade_high_time) & !is.na(trade_high_price))

more_than_1m$first_list_date <- more_than_1m$first_list_date / 1000
more_than_1m$trade_high_price <- round(more_than_1m$trade_high_price, 2)
more_than_1m$first_open <- round(more_than_1m$first_open, 2)

more_than_1m <- more_than_1m %>%
  mutate(
    first_list_date = as.POSIXct(first_list_date, origin = "1970-01-01"),
    trade_high_time = as.POSIXct(trade_high_time, origin = "1970-01-01"),
    duration_to = seconds_to_period(difftime(trade_high_time, first_list_date, units = "secs"))
  )

more_than_1m <- more_than_1m %>%
  filter(!is.na(duration_to) & as.numeric(duration_to) > 0)
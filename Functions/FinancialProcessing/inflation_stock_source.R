require(quantmod)
require(dplyr)
require(data.table)
require(stringi)


# Desc: 
#   This function takes in a vector of stock tickers to merge into a data frame 
#   that contains the daily adjusted prices and trading volume
# Inpt:
#   - tickers [vector]: stock ticker values to create a data frame out of
#   - sc [Spark]: spark context object to assign resulting data frame to
# Oupt:
#   - mergeDf [df]: data frame of stock tickers and adjust price and volume values
createTickerDf <- function(tickers, sc){
  
  # Set up environment to create df
  stockEnv <- new.env()
  getSymbols(tickers, env = stockEnv)
  
  # Create adjusted price
  stockList <- eapply(stockEnv, Ad) #Ad pulls in the adjusted price of the stock
  stockDf <- do.call(merge, stockList)
  
  # Create volume
  stockListVol <- eapply(stockEnv, Vo)
  stockVol <- do.call(merge, stockListVol)
  
  # Merge data frames
  mergeDf <- cbind(stockDf, stockVol)
  
  # Get period return for each stock
  # Amazon, Boeing, Microsoft, Alaska Airlines, Starbucks, Expedia, Nordstrom (JWN)
  yearlyAmznDelta <- as.numeric(periodReturn(mergeDf$AMZN.Adjusted, period = "yearly"))
  yearlyBoeingDelta <- as.numeric(periodReturn(mergeDf$BA.Adjusted, period = "yearly"))
  yearlyMsftDelta <- as.numeric(periodReturn(mergeDf$MSFT.Adjusted, period = "yearly"))
  yearlyAlkDelta <- as.numeric(periodReturn(mergeDf$ALK.Adjusted, period = "yearly"))
  yearlySbuxDelta <- as.numeric(periodReturn(mergeDf$SBUX.Adjusted, period = "yearly"))
  yearlyJwnDelta <- as.numeric(periodReturn(mergeDf$JWN.Adjusted, period = "yearly"))
  yearlyExpeDelta <- as.numeric(periodReturn(mergeDf$EXPE.Adjusted, period = "yearly"))
  
  # Bind these back into a data frame
  stockDfYrReturn <- as.data.frame(cbind("Years" = seq(2007, format(Sys.Date(), "%Y"), 1),
                                         "AMZN" = yearlyAmznDelta,
                                         "BA" = yearlyBoeingDelta,
                                         "MSFT" = yearlyMsftDelta,
                                         "ALK" = yearlyAlkDelta,
                                         "SBUX" = yearlySbuxDelta,
                                         "JWN" = yearlyJwnDelta,
                                         "EXPE" = yearlyExpeDelta),
                                   stringsAsFactors = FALSE)
  
  # Convert to Spark
  mergeDf_spark <- copy_to(sc, stockDfYrReturn)
  
  
  return(mergeDf_spark)
  
}


# Desc:
#   Given data frame of prices and corresponding year of price, convert
#   to a user input year dollars.
# Inpt:
#   - startYear [int]: earliest year for prices
#   - dataFrame [df]: data frame of prices and years
#   - columnYearName [str]: name of df column that corresponds to year
#   - targetColumn [str]: name of df column that corresponds to price
#   - inflationYear [int]: user specifed year to convert prices to
# Oupt:
#   - dataFrame [df]: updated data frame with prices adjusted to user
#                     defined year dollars
yearlyInflationRef <- function(startYear, spark_dataFrame, columnYearName, targetColumn, inflationYear, sc){
  
  require(quantmod)
  
  # Get CPI to calculate inflation rates
  getSymbols("CPIAUCSL", src='FRED')
  
  # Apply a yearly function to get the yearly inflation
  avgCPI <- apply.yearly(CPIAUCSL, mean)

  # Filter paste
  filter <- paste0(startYear, "::", inflationYear)
  
  # Subset to startYear based on real estate data
  avgCPI <- avgCPI[filter]
  
  # Calcualte conversion factor
  conversion <- as.vector(as.numeric(avgCPI[as.character(inflationYear)])/avgCPI)
  years <- seq(startYear, inflationYear, 1)
  
  # Inflation data frame
  inflDf <- as.data.frame(cbind(years,
                                "InflConversionFactor" = conversion),
                          stringsAsFactors = FALSE)
  colnames(inflDf)[1] <- columnYearName
  
  # Convert to Spark df
  inflDf_spark <- copy_to(sc, inflDf)
  
  # Inner join the Spark dataframes
  spark_dataFrame <- spark_dataFrame %>%
    inner_join(inflDf_spark, by = columnYearName)
  
  # Create name for new target column
  newTargetColName <- paste0(targetColumn, paste0(inflationYear, "_Dollars"))
  
  # Create new target column
  spark_dataFrame <- spark_dataFrame %>%
    mutate(!!as.name(newTargetColName) := (SalePrice * InflConversionFactor))
  
  return (spark_dataFrame)
  
}

# Desc:
  # Function that melts a given dataframe
# Inpt:
  # tbl [df]: spark dataframe
  # gather_cols [list]: list of columns to gather from data frame
sdf_gather <- function(tbl, gather_cols){
  
  other_cols <- colnames(tbl)[!colnames(tbl) %in% gather_cols]
  
  lapply(gather_cols, function(col_nm){
    tbl %>% 
      select(c(other_cols, col_nm)) %>% 
      mutate(key = col_nm) %>%
      rename(value = col_nm)  
  }) %>% 
    sdf_bind_rows() %>% 
    select(c(other_cols, 'key', 'value'))
}
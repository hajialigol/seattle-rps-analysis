
require(dplyr)

# Desc:
#   Given seattle real estate data sets and user preferences, analyze the data and output data and 
#   charts of the analysis.  This is a compiler script of all the source functions and other 
#   necessary processes.
# Inpt:
#   - rpsDf [df]: Seattle RPS data set
#   - rbDf [df]: Seattle Residential Building data set
#   - sc [spark]: Spark session object
#   - latest_date [str]: string format of latest date to run analysis through in YYYY-mm-dd format
# Oupt:
#   - finalDf [df]: final data set from analysis
seattle_rps_compiler <- function(rps_spark, rb_spark, sc, latest_date){

  # Initial Processing
  script_endYear <- as.numeric(format(as.Date(latest_date), '%Y'))
  
  # Filter data frame by date
  rps_spark <- rps_spark %>% 
    mutate(DocumentDate = to_date(DocumentDate, "MM/dd/yyyy")) %>%
    filter(DocumentDate > '2007-01-01', DocumentDate < '2019-12-31') %>%
    mutate(Years = as.numeric(year(DocumentDate)))

  # Extract yearly inflation
  rps_spark <- yearlyInflationRef(startYear = 2007, spark_dataFrame = rps_spark, columnYearName = "Years",
                                  targetColumn = "SalePrice", inflationYear = 2019, sc = sc)
  
  # Create flags to remove rows with
  corpEntityFlag <- c(" corp.", " co.", " corp ", " co ", "corporation",
                      " llc.", "llc. ", " llc ", "llc.", "llc", "l l c", "l.l.c",
                      " inc.", " inc. ", " inc ", " inc", "incorporated",
                      " holdings ", " holdings",
                      " trust ", " trust", "trustee",
                      " ltd.", " ltd", " ltd.", "limited",
                      " bank", " city", " seattle")
  
  
  # Remove transactions with buyers who have any of the above corporate flags
  rps_spark <- removePartialStrings(dataFrame = rps_spark, stringVector = corpEntityFlag,
                                    targetColumn = "BuyerName")
  
  # Properties to exclude:
  #   - Luxury real estate: >$1m in current dollars
  #   - We will only look at sale vehicles of statutory warranty deeds: id = 3
  #   - To account for any other special warranties, we will remove transactions with $0 value
  filtering_string <- paste0("SalePrice", script_endYear, "_Dollars")
  rps_spark <- rps_spark %>%
    filter(!!as.name(filtering_string) < 1000000,
           SaleInstrument == 3, !!as.name(filtering_string) > 0)
  
  # Prep arguments for subsetMerge function
  rbVec <- c("Major", "Minor", "Address", "ZipCode", "SqFtTotLiving", "Bedrooms", "BathFullCount", "YrBuilt")
  joinVec <- c("Major", "Minor")
  
  # Run subsetMerge function
  people_rb_spark <- subsetMerge(dataFrame1 = rb_spark, dataFrame2 = rps_spark,
                                 keepColumns = rbVec, joinColumns = joinVec)
  
  # Create a new column for bedroom bathroom
  people_rb_spark <- people_rb_spark %>%
    mutate("BedBath" = paste0(Bedrooms, "-", BathFullCount))
  
  # Run standardizeZipcode function on people_rbDf 
  people_rb_spark <- standardizeZipcode(dataFrame = people_rb_spark, targetColumn = "ZipCode", removeStatus = "Y")
  
  # Geocode all zip codes
  people_rb_spark_geo <- geocodeZipcode(dataFrame = people_rb_spark, "ZipCode",
                                        sc = sc)
  
  # Calculate bins of total sq ftge
  people_rb_spark_geo <- calculateBins(people_rb_spark_geo, "SqFtTotLiving")
  people_rb_spark_geo <- people_rb_spark_geo %>%
    mutate(PPSqFt = !!as.name(filtering_string) / SqFtTotLiving)
  
  # Create a grouped data frame for these parameters:
  #   - SqFtTotLiving_Bins within each Zipcode
  #   - Broken out by years
  #   ex: 2007-2019 SqFt Bins by each zipcode
  #       - Note: Summarize the total transaction volume and average 
  
  # First group by zipcode -> sqftbins -> years
  groupDf <- people_rb_spark_geo %>%
    group_by(ZipCode, Bedrooms, Years) %>%
    summarize(AvgSale = mean(!!as.name(filtering_string)),
              TotalVolume = n(),
              AvgPPSqFt = mean(PPSqFt))
  
  # Convert back into R because sparklyr can't count number of unique items in a column
  groupDf_r <- as.data.frame(groupDf)
  
  # Calculate the difference columns within each group 
  groupDf_r <- groupDf_r %>% arrange(Years) %>% group_by(ZipCode, Bedrooms) %>% 
    mutate(DiffVolume = (TotalVolume - lag(TotalVolume))/lag(TotalVolume),
           DiffAvgSale = (AvgSale - lag(AvgSale))/(lag(AvgSale)),
           CountYears = length(unique(Years)),
           TotalVolumeGrouping = sum(TotalVolume))
  
  # Convert R data frame back into Spark
  groupDf_spark <- copy_to(sc, groupDf_r)
  
  # Now order the data set
  groupDf_spark <- groupDf_spark %>%
    arrange(ZipCode, Bedrooms, Years)
  
  # Remove groups where there is less than n# for the years count
  groupDf_spark <- groupDf_spark %>%
    filter(TotalVolumeGrouping > 100, Years != 2007) #, CountYears == script_endYear-2007+1)
  
  # Create data frame of stock tickers
  tickers <- c("ALK", "AMZN", 'BA', 'EXPE', 'JWN', 'MSFT', 'SBUX')
  spark_stock <- createTickerDf(tickers, sc)
  
  # Merge the the stock prices into our groupDf
  # This will be our final data frame 
  analysisDf <- groupDf_spark %>% inner_join(spark_stock, by = "Years")
  
  #----------------#
  # Begin analysis #
  #----------------#
  
  # Keep only groupings of transactions (zipcode + beds) that has at least 10 transactions per
  # year for all years 2008 - current year
  keep <- colnames(analysisDf)[1:10]
  stocks_list <- colnames(analysisDf)[11:17]
  
  # Melt the analysis data frame
  analysisDf_melt <- analysisDf %>%
    sdf_gather(stocks_list)
  
  # Rename the key and value columns
  analysisDf_melt <- analysisDf_melt %>%
    select(colnames(analysisDf_melt)[1:11], Stocks = key, DiffAvgPrice = value)
  
  # Alternate analysis, calculate integral of curves to get area between them, lower the area the better the score
  analysisDf_melt <- analysisDf_melt %>%
    mutate(UID = paste0(!!as.name("ZipCode"), "-", !!as.name("Bedrooms"), "-", !!as.name("Stocks")))
  
  # Scale the two differences so sd of 1 and mean of 0
  analysisDf_melt <- analysisDf_melt %>%
    group_by(UID) %>% 
    mutate(DiffAvgSale_S = (DiffAvgSale - mean(DiffAvgSale, na.rm = T)),
           DiffAvgPrice_S = (DiffAvgPrice - mean(DiffAvgPrice, na.rm = T)),
           Correlation = cor(DiffAvgSale_S, DiffAvgPrice_S))
  
  # Return df for testing purposes
  return (analysisDf_melt)
}


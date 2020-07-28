import pyspark
import re
import os
import pandas_datareader as pdf
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_unixtime, lower as _lower, first, unix_timestamp, \
year, mean as _mean, concat, corr, regexp_replace, trim, lit, when, length, substring, sum as _sum, \
countDistinct, lag, stddev as _stddev, count
from pyspark.sql.types import DateType
from pyspark.ml.feature import Bucketizer


def seattle_rps_compiler(rps_spark, rb_spark, zipcodes, latest_date,
                         save_wd, save_df, spark):
    """
    Desc:
      Given seattle real estate data sets and user preferences, analyze the data and output data and 
      charts of the analysis.  This is a compiler script of all the source functions and other 
      necessary processes.
    Inpt:
      rps_spark [df]: Seattle RPS data set
      rb_spark [df]: Seattle Residential Building data set
      zipcode [df]: Data set of all zipcodes
      latest_date [str]: string format of latest date to run analysis through in YYYY-mm-dd format
      save_wd [str]: working directory to save output data frame
      save_df [str]: string stating whether to save data frame or not ('y' or 'n')
      spark [spark]: Spark session object
    Oupt:
      finalDf [df]: final data set from analysis
    """

    # Import analysis and preprocessing functions
    from FinancialProcessing import inflation_stock_source
    from Geocoding import zipcode_geocode_source
    from RPSProcessing import seattle_rps_source
    from Analytics import price_integral_source
    
    # Convert columns into appropriate type
    string_columns = {"DocumentDate", "PlatType", "SellerName", "BuyerName", "AFCurrentUseLand",
                      "AFNonProfitUse", "AFHistoricProperty"}
    for column in rps_spark.columns:
        if column not in string_columns:
            rps_spark = rps_spark.withColumn(column, rps_spark[column].cast("double"))

    # Find year of latest date
    script_end_year = re.findall("(\d{4})", latest_date)[0]

    # Make date column
    rps_spark = rps_spark.withColumn("DocumentDate", from_unixtime(unix_timestamp("DocumentDate", "MM/dd/yyy")).cast(DateType()))

    # Filter dataframe based on dates
    rps_spark = rps_spark.filter((col("DocumentDate") > '2007-01-01') & (col("DocumentDate") < latest_date))

    # Create year column
    rps_spark = rps_spark.withColumn("Years", year("DocumentDate"))

    # Get yearly inflation rate
    rps_spark = inflation_stock_source.yearlyInflationRef(spark_dataFrame=rps_spark, 
                                              spark=spark)

    # Create set of strings to filter from buyer's name
    corpEntityFlag = {" corp.", " co.", " corp ", " co ", "corporation",
                          " llc.", "llc. ", " llc ", "llc.", "llc", "l l c", "l.l.c",
                          " inc.", " inc. ", " inc ", " inc", "incorporated",
                          " holdings ", " holdings",
                          " trust ", " trust", "trustee",
                          " ltd.", " ltd", " ltd.", "limited",
                          " bank", " city", " seattle"}

    # Remove transactions with buyers who have any of the above corporate flags
    rps_spark = seattle_rps_source.removePartialStrings(spark_dataFrame = rps_spark, stringVector = corpEntityFlag)

    # To account for any other special warranties, we will remove transactions with $0 value
    # Properties to exclude:
    #   - Luxury real estate: >$1m in current dollars
    filter_string = "SalePrice" + script_end_year + "_Dollars" 
    rps_spark = rps_spark.filter((col(filter_string) < 1000000) & (col(filter_string) > 0)) # Non luxury data set

    # We will only look at sale vehicles of statutory warranty deeds: id = 3
    rps_spark = rps_spark.filter(col("SaleInstrument") == 3)

    # Prep arguments for subsetMerge function
    rbVec = ["Major", "Minor", "Address", "ZipCode", "SqFtTotLiving",
              "Bedrooms", "BathFullCount", "YrBuilt"]
    joinVec = ["Major", "Minor"]

    # Change rpsDf major minor to integer
    rps_spark = rps_spark.withColumn("Major", col("Major").cast('int'))
    rps_spark = rps_spark.withColumn("Minor", col("Minor").cast('int'))

    # Run subsetMerge function
    people_rb_spark = seattle_rps_source.subsetMerge(spark_dataFrame1 = rb_spark,
                                                    spark_dataFrame2 = rps_spark,
                                                    keepColumns = rbVec, joinColumns = joinVec)

    # Create a new column for bedroom bathroom
    people_rb_spark = people_rb_spark.withColumn("BedBath", \
                                                 concat(col("Bedrooms"), \
                                                 lit("-"), col("BathFullCount")))

    # Run standardizeZipcode function on people_rbDf 
    people_rb_spark = zipcode_geocode_source.standardizeZipcode(spark_dataFrame = people_rb_spark)

    # Geocode all zip codes
    people_rb_spark = zipcode_geocode_source.geocodeZipcode(people_dataFrame = people_rb_spark,
                                                           zipcode_dataFrame = zipcodes)

    # Calculate bins of total sq ft
    people_geo_spark = seattle_rps_source.calculateBins(merged_dataFrame = people_rb_spark)

    # Create PPSqFt column
    people_geo_spark = people_geo_spark.withColumn("PPSqFt", col(filter_string) / col("SqFtTotLiving"))

    # Create a grouped data frame for these parameters:
    #       - SqFtTotLiving_Bins within each Zipcode
    #       - Broken out by years
    #       ex: 2007-2019 SqFt Bins by each zipcode
    #               - Note: Summarize the total transaction volume and average 
      
    # First group by zipcode -> sqftbins -> years
    # First group by zipcode -> sqftbins -> years
    # address_window = window.partitionBy('Address')
    spark_group = people_geo_spark.groupby("ZipCode", "Bedrooms", "Years").agg(count("Address"). \
                                                                               alias("Total Volume"),
                                                                               _mean(filter_string).alias("Average Sale"),
                                                                               _mean("PPSqFt").alias("AvgPPSqFt"))

    # Calculate the difference columns within each group 
    window = Window.partitionBy('ZipCode', 'Bedrooms').orderBy('Years')
    spark_group = spark_group.withColumn("DiffVol", (col("Total Volume") \
                                          - lag(col("Total Volume")).over(window)) \
                                          / lag(col("Total Volume")).over(window))

    # Compute average difference in sales
    spark_group = spark_group.withColumn("DiffAvgSale", (col("Average Sale") \
                                         - lag(col("Average Sale")).over(window)) \
                                         / lag(col("Average Sale")).over(window))

    # Create total volume groupping and count years columns
    temp_group = spark_group.orderBy(col("Years")) \
                                     .groupby("ZipCode", "Bedrooms") \
                                     .agg(_sum(col("Total Volume")) \
                                     .alias("TotalVolumeGrouping"), \
                                     countDistinct(col("Years")) \
                                     .alias("CountYears"))

    # Inner-join data frames based on zipcode and bedrooms
    spark_group_combined = spark_group.join(temp_group, on = ['ZipCode', 'Bedrooms'], how = 'inner')

    # Now order the data set
    spark_group_combined = spark_group_combined.orderBy("ZipCode", "Bedrooms", 'Years') 

    # Remove groups where there is less than n# for the years count
    spark_group_combined = spark_group_combined.filter((col("CountYears") == int(script_end_year) - 2007 + 1) \
                                     & (col("TotalVolumeGrouping") > 100) \
                                     & (col("Years") != 2007))

    # Create data frame of stock tickers
    tickers = ["ALK", "AMZN", 'BA', 'EXPE', 'JWN', 'MSFT', 'SBUX']

    # Fetch stock data frame
    stockDf = inflation_stock_source.createTickerDf(tickers = tickers)

    # Extract first year's returns
    first_year_returns = (stockDf.groupby("Years") \
                       .apply(inflation_stock_source.yearly_returns))['Adj Close'].iloc[0]

    # Get every other year's returns
    yearly_stock_returns = stockDf['Adj Close'].resample('Y').ffill().pct_change()

    # Assign first year's returns to newly generated dataframe
    yearly_stock_returns.iloc[0] = first_year_returns

    # Create year column
    yearly_stock_returns['Years'] = yearly_stock_returns.index.year

    # Convert stock data frame to Spark
    spark_stock = spark.createDataFrame(yearly_stock_returns)

    # Join two Spark data frames
    analysis_spark = spark_group_combined.join(other = spark_stock, on = "Years", how = "inner")

    # Remove groups where there is less than n# for the years count
    analysis_spark = analysis_spark.filter((col("CountYears") == int(script_end_year) - 2007 + 1) \
                                     & (col("TotalVolumeGrouping") > 100) \
                                     & (col("Years") != 2007))

    # Keep only groupings of transactions (zipcode + beds) that has at least 10 transactions per
    # year for all years 2008 - current year
    keep = analysis_spark.columns[:10]

    # Melt the analysis data frame
    analysis_melt = price_integral_source.melt(df = analysis_spark, id_vars = keep, var_name = "Stocks",
                         value_name = "DiffAvgPrice", value_vars = tickers)

    # Update "UID" column to include zipcode, bedroom, and stock information
    analysis_melt = analysis_melt.withColumn("UID", concat(col("ZipCode"), lit("-"), col("Bedrooms"),
                                     lit("-"), col("Stocks")))

    # Create temporary data frame to compute mean difference in average sales and price
    temp_analysis_melt = analysis_melt.groupby("UID").agg(_mean(col("DiffAvgPrice")). \
                                                          alias("mean_DiffAvgPrice"),
                                                           _mean(col("DiffAvgSale")). \
                                                          alias("mean_DiffAvgSale"))

    # Join the analysis_melt data frame and the temporary data frame
    analysis_melt = analysis_melt.join(temp_analysis_melt, on = "UID", how = 'inner')

    # Scale difference in average price
    analysis_melt = analysis_melt.withColumn("DiffAvgPrice_S", (col("DiffAvgPrice") - \
                                                                col("mean_DiffAvgPrice")))

    # Scale difference in average sales
    analysis_melt = analysis_melt.withColumn("DiffAvgSale_S", (col("DiffAvgSale") - \
                                                               col("mean_DiffAvgSale"))) 
    # Drop intermediate columns
    analysis_melt = analysis_melt.drop('mean_DiffAvgSale').drop('mean_DiffAvgPrice')
    
    # Correlation
    correlation_df = analysis_melt.groupby('UID').agg(corr("DiffAvgSale_S", "DiffAvgPrice_S") \
                                                  .alias("Correlation"))

    # Join data frames to get correlation values
    analysis = analysis_melt.join(other = correlation_df, on = 'UID', how = 'inner')

    # Save data frame if desired
    if (str.lower(save_df) == 'y'):
      analysis.toPandas().to_csv("final_dataframe.csv")        
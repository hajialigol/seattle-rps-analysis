import pyspark
import re
import os
import pandas_datareader as pdf
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_unixtime, lower as _lower, first, unix_timestamp, \
year, mean as _mean, concat, regexp_replace, trim, lit, when, length, substring, sum as _sum, \
countDistinct, lag, stddev as _stddev
from pyspark.sql.types import DateType, DoubleType
from pyspark.ml.feature import Bucketizer

# Global variables
latest_date = "" # Enter date here
save_wd = ""
data_wd = r"C:\Users\15712\Documents\GitHub Projects\seattle-rps-analysis\Data"
os.chdir(data_wd)

# Set up Spark session
spark = SparkSession.builder.getOrCreate()

# Read in csv files
rps_spark = spark.read.csv("EXTR_RPSale.csv", header=True)
rb_spark = spark.read.csv("EXTR_ResBldg.csv", header=True)
zipcodes = spark.read.csv("Zipcode.csv", header=True)

# Convert columns into appropriate type
string_columns = {"DocumentDate", "PlatType", "SellerName", "BuyerName", "AFCurrentUseLand",
                  "AFNonProfitUse", "AFHistoricProperty"}
for column in rps_spark.columns:
    if column not in string_columns:
        rps_spark = rps_spark.withColumn(column, rps_spark[column].cast("double"))
        
latest_date = '2019-12-31'
# Find year of latest date
script_end_year = re.findall("(\d{4})", latest_date)[0]

# Make date column
rps_spark = rps_spark.withColumn("DocumentDate", from_unixtime(unix_timestamp("DocumentDate", "MM/dd/yyy")).cast(DateType()))

# Filter dataframe based on dates
rps_spark = rps_spark.filter((col("DocumentDate") > '2007-01-01') & (col("DocumentDate") < latest_date))

# Create year column
rps_spark = rps_spark.withColumn("Years", year("DocumentDate"))

def yearlyInflationRef(startYear = 2007, spark_dataFrame = rps_spark, columnYearName = "Years",
                       targetColumn = "SalePrice", inflationYear = 2019, spark = spark):
    '''
    Desc:
      Given data frame of prices and corresponding year of price, convert
      to a user input year dollars.
    Inpt:
      - startYear [int]: earliest year for prices
      - spark_dataFrame [df]: pyspark data frame of prices and years
      - columnYearName [str]: name of df column that corresponds to year
      - targetColumn [str]: name of df column that corresponds to price
      - inflationYear [int]: user specifed year to convert prices to
      - spark [SparkSession]: sparksession variable
    Oupt:
      - spark_dataFrame [df]: updated pyspark data frame with prices adjusted to user
                        defined year dollars
    '''
    CPIAUCSL = pdf.get_data_fred('CPIAUCSL', start = str(startYear) + "-01-01",
                                end = str(inflationYear) + '-12-01')
    avg_cpi = (CPIAUCSL.resample('Y').mean())['CPIAUCSL']
    inflation_year_cpi = avg_cpi[str(inflationYear)][0]
    conversion = (inflation_year_cpi / avg_cpi).to_list()
    years = [year for year in range(startYear, inflationYear + 1)]
    conversion_df = pd.DataFrame(data = {columnYearName: years, "InflConversionFactor": conversion})
    spark_conversion_df = spark.createDataFrame(conversion_df)
    spark_dataFrame = spark_dataFrame.join(other = spark_conversion_df, on = columnYearName,
                                          how = "inner")
    newTargetColName = targetColumn + str(inflationYear) + "_Dollars"
    spark_dataFrame = spark_dataFrame.withColumn(newTargetColName,
                                                 col(targetColumn) * col("InflConversionFactor"))
    return spark_dataFrame

# Add adjusted sales price
rps_spark = yearlyInflationRef()

# Create flags to remove rows with
corpEntityFlag = {" corp.", " co.", " corp ", " co ", "corporation",
                  " llc.", "llc. ", " llc ", "llc.", "llc", "l l c", "l.l.c",
                  " inc.", " inc. ", " inc ", " inc", "incorporated",
                  " holdings ", " holdings",
                  " trust ", " trust", "trustee",
                  " ltd.", " ltd", " ltd.", "limited",
                  " bank", " city", " seattle"}

def removePartialStrings(spark_dataFrame = rps_spark, stringVector = corpEntityFlag,
                         target_name = "BuyerName"):
    '''
    Desc:
      Given a pysaprk data frame and a column of string values, remove rows that contain
      any sequence of string values.
    Inpt:
      - dataFrame [df]: input pyspark data frame
      - stringVector [vec]: vector of string values to find and remove rows on
      - targetColumn [str]: string format of column name to search through
    Oupt:
      - dataFrame [df]: subsetted pyspark data frame
    '''
    spark_dataFrame = spark_dataFrame.withColumn(target_name, trim(_lower(col(target_name))))
    for unwanted_value in stringVector:
        spark_dataFrame = spark_dataFrame.withColumn(target_name,
                                                     regexp_replace(col(target_name),
                                                     unwanted_value, ""))
    return spark_dataFrame    

# Remove transactions with buyers who have any of the above corporate flags
rps_spark = removePartialStrings()

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

def subsetMerge(spark_dataFrame1 = rb_spark, spark_dataFrame2 = rps_spark,
                keepColumns = rbVec, joinColumns = joinVec):
    '''
    Desc:
      Given two data frames and multiple columns to inner join on,
      perform the merge 
    Inpt:
      - spark_dataFrame1 [df]: first pyspark data frame
      - spark_dataFrame2 [df]: second pyspark data frame
      - keepColumns [vec]: vector of columns to subset spark_dataFrame1 to
      - joinColumns [vec]: vector of columns to inner join on
    Oupt:
      - merged_spark_dataframe [df]: merged and potentially subsetted pyspark df
    '''
    subset_dataFrame1 = spark_dataFrame1.select(rbVec)
    merged_spark_dataframe = subset_dataFrame1.join(other = spark_dataFrame2,
                                                    on = joinColumns, how = "inner")
    return merged_spark_dataframe


# Run subsetMerge function
people_rb_spark = subsetMerge()

# Create a new column for bedroom bathroom
people_rb_spark = people_rb_spark.withColumn("BedBath", \
                                             concat(col("Bedrooms"), \
                                             lit("-"), col("BathFullCount")))

def standardizeZipcode(spark_dataFrame = people_rb_spark, targetColumn = "ZipCode", removeStatus = "Y"):
    '''
    Desc:
      Given a data frame with a zip code column, standardize zipcodes
      down to 5 digit format
    Inpt:
      - dataFrame [df]: inpt data frame
      - targetColumn [str]: name of column with zipcodes
      - removeStatus [str ('y', 'n')]: 'y' will remove no-match zip codes, 'n' will keep
    Oupt:
      - dataFrame [df]: data frame with standardized zipcodes
    '''

    '''
    Checks character length, if longer than 5 zip code needs to be
    trimmed down to first 5 characters: ex 29210-4209 -> 29210.
    If less than 5 characters, unable to determine zip code, replace
    it with an empty string that will be removed afterwards.
    '''
    column_length = length(col(targetColumn))
    spark_dataFrame = spark_dataFrame.withColumn(targetColumn, when(column_length < 5, "").
                                 when(column_length > 5, substring(col(targetColumn), 1, 5)).
                                 when(column_length == 5, col(targetColumn)))
    
    # If statement for user specification if no-match zip codes
    # should be removed from data frame. If specified as Y, remove them
    if str.lower(removeStatus) == 'y':
        spark_dataFrame = spark_dataFrame.filter(col(targetColumn) != "")
    return spark_dataFrame


# Run standardizeZipcode function on people_rbDf 
people_rb_spark = standardizeZipcode()


def geocodeZipcode(people_dataFrame = people_rb_spark, zipcode_dataFrame = zipcodes, 
                   targetColumn = "ZipCode"):
    '''
    Desc:
      Given data frame and zipcodes, geocode each zipcode to a lat/long
    Inpt:
      - people_dataFrame [df]: input pyspark people data frame
      - zipcode_dataFrame [df]: input pyspark zipcode data frame
      - targetColumn [str]: column with standradized zipcodes
    Oupt:
      - zipcode_dataFrame [df]: pyspark data frame with two newn columns (lat/long) 
    '''
    
    # Change the column name to match with the Zipcode column in the dataFrame
    # so you can merge the two this will bring in the lat/longs needed to plot
    zipcode_dataFrame = zipcode_dataFrame.withColumnRenamed("zip", targetColumn)
    
    # Merge the data frames together
    zipcode_dataFrame = people_dataFrame.join(other = zipcode_dataFrame, on = targetColumn,
                                            how = "inner") 
    # Return the data frame back to the user
    return zipcode_dataFrame

# Geocode all zip codes
people_rb_spark = geocodeZipcode()

def calculateBins(merged_dataFrame = people_rb_spark, targetColumn = "SqFtTotLiving"):
    '''
    Desc:
        Given data frame and square footage column of real estate properties,
        create sq footage bins of properties in 1000 sq ft increments
        up until 5000 sq ft, at which point >5000 sq ft is the last bin
    Inpt:
        - merged_dataFrame [df]: input pyspark data frame
        - targetColumn [str]: string format of real estate sq footage column
    oupt:
        - merged_dataFrame [df]: updated pyspark data frame
    '''    
    newColName = targetColumn + "_Bins"
    merged_dataFrame = merged_dataFrame.withColumn(targetColumn, col(targetColumn).cast('double'))
    buckets = Bucketizer(splits = [0, 1000, 2000, 3000, 4000, 5000, float('Inf')],
                        inputCol = targetColumn, outputCol = newColName)
    merged_dataFrame = buckets.setHandleInvalid("keep").transform(merged_dataFrame)
    return merged_dataFrame


# Calculate bins of total sq ft
people_geo_spark = calculateBins()
people_geo_spark = people_geo_spark.withColumn("PPSqFt", col(filter_string) / col("SqFtTotLiving"))

# Create a grouped data frame for these parameters:
#		- SqFtTotLiving_Bins within each Zipcode
#		- Broken out by years
#		ex: 2007-2019 SqFt Bins by each zipcode
#				- Note: Summarize the total transaction volume and average 
  
# First group by zipcode -> sqftbins -> years

spark_group = people_geo_spark.groupby("ZipCode", "Bedrooms", "Years").agg(length(first("Address")),
                                                            _mean(filter_string).alias("Average Sale"),
                                                            _mean("PPSqFt").alias("AvgPPSqFt"))
spark_group = spark_group.withColumnRenamed("length(first(Address, false))", "Total Volume")


# from pyspark.sql.functions import lag
# from pyspark.sql import Window

 # Calculate the difference columns within each group 
window = Window.partitionBy('ZipCode', 'Bedrooms').orderBy('Years')
spark_group = spark_group.withColumn("DiffVol", (col("Total Volume") \
                                      - lag(col("Total Volume")).over(window)) \
                                      / lag(col("Total Volume")).over(window))

spark_group = spark_group.withColumn("DiffAvgSale", (col("Average Sale") \
                                     - lag(col("Average Sale")).over(window)) \
                                     / lag(col("Average Sale")).over(window))

temp_group = spark_group.orderBy(col("Years")) \
                                 .groupby("ZipCode", "Bedrooms") \
                                 .agg(_sum(col("Total Volume")) \
                                 .alias("TotalVolumeGrouping"), \
                                 countDistinct(col("Years")) \
                                 .alias("CountYears"))


spark_group_combined = spark_group.join(temp_group, on = ['ZipCode', 'Bedrooms'], how = 'inner')

# Now order the data set
spark_group_combined = spark_group_combined.orderBy("ZipCode", "Bedrooms", 'Years') 

# Remove groups where there is less than n# for the years count
spark_group_combined = spark_group_combined.filter((col("CountYears") == int(script_end_year) - 2007 + 1) \
                                 & (col("TotalVolumeGrouping") > 100) \
                                 & (col("Years") != 2007))

# Create data frame of stock tickers
tickers = ["ALK", "AMZN", 'BA', 'EXPE', 'JWN', 'MSFT', 'SBUX']

def createTickerDf(tickers = tickers, startYear = '2007-01-01', endYear = '2019-12-31'):
    '''
    Desc: 
      This function takes in a vector of stock tickers to merge into a data frame 
      that contains the daily adjusted prices and trading volume
    Inpt:
      - tickers [vector]: stock ticker values to create a data frame out of
    Oupt:
      - mergeDf [df]: data frame of stock tickers and adjust price and volume values
    '''
    dataFrame = pdf.get_data_yahoo(tickers, start = startYear, end = endYear)
    stock_dataFrame = dataFrame[['Adj Close', 'Volume']]
    stock_dataFrame['Years'] = stock_dataFrame.index.year
    return stock_dataFrame

spark_stockDf = createTickerDf()

def yearly_returns(df):    
    return (df.iloc[-1] / df.iloc[0]) - 1

stockDfYrReturn = (spark_stockDf.groupby("Years").apply(yearly_returns))['Adj Close']
stockDfYrReturn['Years'] = stockDfYrReturn.index
spark_stock = spark.createDataFrame(stockDfYrReturn)

analysis_spark = spark_group_combined.join(other = spark_stock, on = "Years", how = "inner")

# Keep only groupings of transactions (zipcode + beds) that has at least 10 transactions per
# year for all years 2008 - current year
keep = analysis_spark.columns[:10]

from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable 

def melt(
        df: DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

analysis_melt = melt(df = analysis_spark, id_vars = keep, var_name = "Stocks",
                     value_name = "DiffAvgPrice", value_vars = tickers)

analysis_melt = analysis_melt.withColumn("UID", concat(col("ZipCode"), lit("-"), col("Bedrooms"),
                                 lit("-"), col("Stocks")))

# Scale the two differences so sd of 1 and mean of 0
analysis_test = (analysis_melt
  .select(_mean("DiffAvgSale").alias("mean_DiffAvgSale"),
          _stddev("DiffAvgSale").alias("stddev_DiffAvgSale"),
          _mean("DiffAvgPrice").alias("mean_DiffAvgPrice"),
          _stddev("DiffAvgPrice").alias("stddev_DiffAvgPrice"))
  .crossJoin(analysis_melt)
  .withColumn("DiffAvgSale_Scaled" , (col("DiffAvgSale") - col("mean_DiffAvgSale")) \
              / col("stddev_DiffAvgSale"))) \
  .withColumn("DiffAvgPrice_Scaled" , (col("DiffAvgPrice") - col("mean_DiffAvgPrice")) \
              / col("stddev_DiffAvgPrice"))


def integral_analysis(df = analysis_test, startYear = 2008, 
                      endYear = script_end_year):
    
    # Create columns for integral score and correlation value between values
    df = df.withColumn('IntegralScore', lit(None).cast(DoubleType()))
    df = df.withColumn('Correlation', lit(None).cast(DoubleType()))
    df.groupby("UID").agg(interp1d(col("Years"),
                                   (col("DiffAvgPrice_Scaled") - col("DiffAvgSale_Scaled"))))
integral_analysis()
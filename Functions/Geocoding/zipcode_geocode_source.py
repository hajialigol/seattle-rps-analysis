import pyspark
import re
import os
import pandas_datareader as pdf
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_unixtime, lower as _lower, first, unix_timestamp, \
year, mean as _mean, concat, regexp_replace, trim, lit, when, length, substring, sum as _sum, \
countDistinct, lag
from pyspark.sql.types import DateType
from pyspark.ml.feature import Bucketizer

# spark_dataFrame = people_rb_spark,
def standardizeZipcode(spark_dataFrame, targetColumn = "ZipCode", removeStatus = "Y"):
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

     # Return data frame back to user    
    return spark_dataFrame


# people_dataFrame = people_rb_spark, zipcode_dataFrame = zipcodes
def geocodeZipcode(people_dataFrame , zipcode_dataFrame, 
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
    zipcode_dataFrame = zipcode_dataFrame.withColumnRenamed("zip", targetColumn
                                                            
    # Merge the data frames together
    zipcode_dataFrame = people_dataFrame.join(other = zipcode_dataFrame, on = targetColumn,
                                            how = "inner") 
                                                            
    # Return the data frame back to the user
    return zipcode_dataFrame 
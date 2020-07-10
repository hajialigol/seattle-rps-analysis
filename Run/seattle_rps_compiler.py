import pyspark
import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime, year
from pyspark.sql.types import DateType

# Global variables
latest_date = "" # Enter date here
save_wd = ""
data_wd = r""
os.chdir(data_wd)

# Set up Spark session
spark = SparkSession.builder.getOrCreate()

# Read in csv files
rps_spark = spark.read.csv("EXTR_RPSale.csv", header=True)
rb_spark = spark.read.csv("EXTR_ResBldg.csv", header=True)

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
rps_spark = rps_spark.withColumn("year", year("DocumentDate"))        
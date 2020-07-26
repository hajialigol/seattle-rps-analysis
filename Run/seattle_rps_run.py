import pyspark
import os
import seattle_rps_compiler_test
from pyspark.sql import SparkSession

# Enter latest date to perform analysis in format YYYY-MM-DD
latest_date = "2019-12-31"

# Initialize directories to save and extract data and get functions from
save_wd = r""
data_wd = r""
function_wd = r""

# Change to data directory
os.chdir(data_wd)

# Create Spark object
# Note: this project is ran locally
spark = SparkSession.builder.getOrCreate()

# Read in csv files
rps_spark = spark.read.csv("EXTR_RPSale.csv", header = True)
rb_spark = spark.read.csv("EXTR_ResBldg.csv", header = True)
zipcodes = spark.read.csv("Zipcode.csv", header = True)

# set working directory to directory of functions
os.chdir(function_wd)

# Call compiler function
seattle_rps_compiler.seattle_rps_compiler(rps_spark = rps_spark, rb_spark = rb_spark, zipcodes = zipcodes, latest_date = latest_date,
										  save_wd = save_wd, save_df = 'n', spark = spark)
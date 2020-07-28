import pyspark
import os
import seattle_rps_compiler
import time
from pyspark.sql import SparkSession

# Enter latest date to perform analysis in format YYYY-MM-DD
latest_date = "2019-12-31"

# Initialize directories to save and extract data and get functions from
save_wd = r""
data_wd = r""
function_wd = r""
# Boolean indication if user would like to time function
time_bool = True

# Amount of times to time function
n = 10

# timing lists
start_time_list = []
end_time_list = []

# Change to data directory
os.chdir(data_wd)

# Create Spark object
spark = SparkSession.builder.getOrCreate()

# Read in csv files
rps_spark = spark.read.csv("EXTR_RPSale.csv", header = True)
rb_spark = spark.read.csv("EXTR_ResBldg.csv", header = True)
zipcodes = spark.read.csv("Zipcode.csv", header = True)

# set working directory to directory of functions
os.chdir(function_wd)

# Call compiler function
if (time_bool == True):
    for i in range(1, n):
        start_time = time.clock()
        seattle_rps_compiler.seattle_rps_compiler(rps_spark = rps_spark, rb_spark = rb_spark, zipcodes = zipcodes, latest_date = latest_date,
                                                  save_wd = save_wd, save_df = 'n', spark = spark)
        end_time = time.clock()
        start_time_list.append(start_time)
        end_time_list.append(end_time)
else:
    seattle_rps_compiler.seattle_rps_compiler(rps_spark = rps_spark, rb_spark = rb_spark, zipcodes = zipcodes, latest_date = latest_date,
                                                  save_wd = save_wd, save_df = 'n', spark = spark)

# Compute timing results
time_results = [end_time - start_time for start_time, end_time in zip(start_time_list, end_time_list)]                                                  
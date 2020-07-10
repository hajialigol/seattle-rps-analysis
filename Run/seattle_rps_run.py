import pyspark
import os
from pyspark.sql import SparkSession

# Global variables
latest_date = "" # Enter date here
save_wd = ""
data_wd = r"C:\Users\15712\Documents\GitHub Projects\seattle-rps-analysis\Data"
os.chdir(data_wd)

# Set up Spark session
spark = SparkSession.builder.getOrCreate()

# Read in csv files
rps_spark = spark.read.csv("EXTR_RPSale.csv")
rb_spark = spark.read.csv("EXTR_ResBldg.csv")
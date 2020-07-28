
# Remove all variables
rm(list=ls())

# Load libraries
require(sparklyr)

# Preset Variables for run script
latest_date = "2019/12/31" # Enter date here
save_wd = ""
data_wd = ""
connection_type = ""

# Load all source script functions
source("~/GitHub Projects/seattle-rps-analysis/Functions/FinancialProcessing/inflation_stock_source.R")
source("~/GitHub Projects/seattle-rps-analysis/Functions/Geocoding/zipcode_geocode_source.R")
source("~/GitHub Projects/seattle-rps-analysis/Functions/RPSProcessing/seattle_rps_source.R")
source("~/GitHub Projects/seattle-rps-analysis/Functions/Analytics/price_integral_source.R")
source("~/GitHub Projects/seattle-rps-analysis/Run/seattle_rps_compiler.R")

# Set Spark connection
sc <- spark_connect(master = connection_type)

# Set working directory to data directory
setwd(data_wd)

# Initial Processing
rpsDf <- read.csv("EXTR_RPSale.csv", stringsAsFactors = FALSE, nrows = 200000)
rbDf <- read.csv("EXTR_ResBldg.csv", stringsAsFactors = FALSE, nrows = 200000)

# Convert to Spark
rps_spark <- copy_to(sc, rpsDf)
rb_spark <- copy_to(sc, rbDf)

# Run program
seattle_rps_compiler(rps_spark = rps_spark, rb_spark = rb_spark, sc = sc,
                     latest_date = latest_date)
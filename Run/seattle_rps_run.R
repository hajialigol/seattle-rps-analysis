
# Remove all variables
rm(list=ls())

library(sparklyr)


# Preset Variables for run script
latest_date = "" # Enter date here
save_wd = ""
data_wd = ""
connection_type = ""

# Load all source script functions
# source("~/GitHub/seattle-rps-analysis/Functions/FinancialProcessing/inflation_stock_source.R")
# source("~/GitHub/seattle-rps-analysis/Functions/Geocoding/zipcode_geocode_source.R")
# source("~/GitHub/seattle-rps-analysis/Functions/RPSProcessing/seattle_rps_source.R")
# source("~/GitHub/seattle-rps-analysis/Functions/Analytics/price_integral_source.R")
# source("~/GitHub/seattle-rps-analysis/Functions/Visualizations/price_comparison_source.R")
# source("~/GitHub/seattle-rps-analysis/Run/seattle_rps_compiler.R")

# Load in data to start for full-run
setwd(data_wd)


# Set Spark connection
sc <- spark_connect(master = connection_type)

# Initial Processing
rpsDf <- spark_read_csv(sc = sc, path = "EXTR_RPSale.csv", stringsAsFactors = FALSE)
rbDf <- spark_read_csv(sc = sc, path = "EXTR_ResBldg.csv", stringsAsFactors = FALSE)

rps_spark <- copy_to(sc, rpsDf)
rb_spark <- copy_to(sc, rbDf)

seattle_rps_compiler(rpsDf = rpsDf, rbDf = rbDf, 
                     latest_date = latest_date, topN = 10, 
                     save_wd = save_wd, data_wd = data_wd, 
                     visuals = 'y', save_df = 'y')

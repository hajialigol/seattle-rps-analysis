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


def yearly_returns(df):    
    '''
    Desc: 
      function that outputs yearly returns
    Inpt:
      df [df]: input dataframe
    Oupt:
      [df]: dataframe of yearly returns 
    '''
    return (df.iloc[-1] / df.iloc[0]) - 1
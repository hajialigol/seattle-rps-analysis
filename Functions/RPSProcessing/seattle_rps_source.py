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
    spark_dataFrame = spark_dataFrame.withColumn(target_name, trim(lower(col(target_name))))
    for unwanted_value in stringVector:
        spark_dataFrame = spark_dataFrame.withColumn(target_name,
                                                     regexp_replace(col(target_name),
                                                     unwanted_value, ""))
    return spark_dataFrame    
rps_spark = removePartialStrings()
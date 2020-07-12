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
    if lower(removeStatus) == 'y':
        spark_dataFrame.withColumn(targetColumn, col(targetColumn) != "")
    return spark_dataFrame
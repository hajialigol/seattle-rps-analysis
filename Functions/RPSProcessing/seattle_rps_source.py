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




def subsetMerge(spark_dataFrame1 = rb_spark, spark_dataFrame2 = rps_spark,
                keepColumns = rbVec, joinColumns = joinVec):
    '''
    Desc:
      Given two data frames and multiple columns to inner join on,
      perform the merge 
    Inpt:
      - spark_dataFrame1 [df]: first pyspark data frame
      - spark_dataFrame2 [df]: second pyspark data frame
      - keepColumns [vec]: vector of columns to subset spark_dataFrame1 to
      - joinColumns [vec]: vector of columns to inner join on
    Oupt:
      - merged_spark_dataframe [df]: merged and potentially subsetted pyspark df
    '''
    subset_dataFrame1 = spark_dataFrame1.select(rbVec)
    merged_spark_dataframe = subset_dataFrame1.join(other = spark_dataFrame2,
                                                    on = joinColumns, how = "inner")
    return merged_spark_dataframe



def calculateBins(merged_dataFrame = people_rb_spark, targetColumn = "SqFtTotLiving"):
    '''
    Desc:
        Given data frame and square footage column of real estate properties,
        create sq footage bins of properties in 1000 sq ft increments
        up until 5000 sq ft, at which point >5000 sq ft is the last bin
    Inpt:
        - merged_dataFrame [df]: input pyspark data frame
        - targetColumn [str]: string format of real estate sq footage column
    oupt:
        - merged_dataFrame [df]: updated pyspark data frame
    '''    
    newColName = targetColumn + "_Bins"
    merged_dataFrame = merged_dataFrame.withColumn(targetColumn, col(targetColumn).cast('double'))
    buckets = Bucketizer(splits = [0, 1000, 2000, 3000, 4000, 5000, float('Inf')],
                        inputCol = targetColumn, outputCol = newColName)
    merged_dataFrame = buckets.setHandleInvalid("keep").transform(merged_dataFrame)
    return merged_dataFrame

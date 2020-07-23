require(qdapRegex)
require(dplyr)
require(data.table)

# Desc:
#   Given a data frame and a column of string values, remove rows that contain
#   any sequence of string values.
# Inpt:
#   - dataFrame [df]: input data frame
#   - stringVector [vec]: vector of string values to find and remove rows on
#   - targetColumn [str]: string format of column name to search through
# Oupt:
#   - dataFrame [df]: subsetted data frame
removePartialStrings <- function(dataFrame, stringVector, targetColumn){
  
  require(qdapRegex)
  require(stringr)
  
  # Standardize string column to lowercase with no extra white space
  dataFrame <- dataFrame %>%
    mutate("BuyerName" = trimws(tolower(BuyerName)))
  stringVector <- tolower(stringVector)
  
  # Loop through vector of unwanted values and filter data set each iteration
  for (unwanted_word in stringVector){
    
    # Remove transactions for seller name  
    dataFrame <- dataFrame %>%
      filter(!rlike(!!as.name(targetColumn), unwanted_word))
  
  }  
  
  return (dataFrame)
  
}


# Desc:
#   Given two data frames and multiple columns to inner join on,
#   perform the merge 
# Inpt:
#   - dataFrame1 [df]: first data frame
#   - dataFrame2 [df]: second data frame
#   - keepColumns [vec]: vector of columns to subset df1 to
#   - joinColumns [vec]: vector of columns to inner join on
# Oupt:
#   - mergeDataFrame [df]: merged and potentially subsetted df
subsetMerge <- function(dataFrame1, dataFrame2, keepColumns, joinColumns){
  
  require(dplyr)
  
  # Subset the columns down to what the user specifies in 'keepColumns'
  subDataFrame1 <- dataFrame1 %>% select(keepColumns)
  
  # inner_join data sets together
  mergeDataFrame <- subDataFrame1 %>% inner_join(dataFrame2, by = joinColumns)
  
  # return back to user
  return(mergeDataFrame)
  
}



# Desc:
#   Given data frame and square footage column of real estate properties,
#   create sq footage bins of properties in 1000 sq ft increments
#   up until 5000 sq ft, at which point >5000 sq ft is the last bin
# Inpt:
#   - dataFrame [df]: input data frame
#   - targetColumn [str]: string format of real estate sq footage column
calculateBins <- function(dataFrame, targetColumn){
  
  newColName <- paste0(targetColumn, "_Bins")
  
  dataFrame <- dataFrame %>%
    mutate(!!as.name(newColName) := case_when(!!as.name(targetColumn) <= 1000 ~ 1,
                                             (!!as.name(targetColumn) > 1000 && !!as.name(targetColumn) <= 2000) ~ 2,
                                             (!!as.name(targetColumn) > 2000 && !!as.name(targetColumn) <= 3000) ~ 3,
                                             (!!as.name(targetColumn) > 3000 && !!as.name(targetColumn) <= 4000) ~ 4,
                                             (!!as.name(targetColumn) > 4000 && !!as.name(targetColumn) <= 5000) ~ 5,
                                             (!!as.name(targetColumn) > 5000) ~ 6))
  
  return (dataFrame)
}
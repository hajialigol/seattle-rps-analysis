require(zipcode)
require(dplyr)

# Desc:
#   Given a data frame with a zip code column, standardize zipcodes
#   down to 5 digit format
# Inpt:
#   - dataFrame [df]: inpt data frame
#   - targetColumn [str]: name of column with zipcodes
#   - removeStatus [str ('y', 'n')]: 'y' will remove no-match zip codes, 'n' will keep
# Oupt:
#   - dataFrame [df]: data frame with standardized zipcodes
standardizeZipcode <- function(dataFrame, targetColumn, removeStatus){
  
  require(dplyr)
  
  # Checks character length, if longer than 5 zip code needs to be
  # trimmed down to first 5 characters: ex 29210-4209 -> 29210.
  # If less than 5 characters, unable to determine zip code, replace
  # it with an empty string that will be removed afterwards.
  # For zips that we are unable to determine, change them to empty
  # For zips longer than 5 characters, trim down to first 5 characters
  dataFrame <- dataFrame %>%
    group_by(!!as.name(targetColumn)) %>%
    mutate(!!as.name(targetColumn) := case_when(length(!!as.name(targetColumn)) == 5 ~ !!as.name(targetColumn),
                            length(!!as.name(targetColumn)) < 5 ~ '',
                            length(!!as.name(targetColumn)) > 5 ~ substring(!!as.name(targetColumn), 1, 5)))

  # If statement for user specification if no-match zip codes
  # should be removed from data frame. If specified as Y, remove them
  if (tolower(removeStatus) == 'y'){
    dataFrame <- dataFrame %>%
      filter(!!as.name(targetColumn) != '')
  }
             
  # Return data frame back to user
  return (dataFrame)
}


# Desc:
#   Given data frame and zipcodes, geocode each zipcode to a lat/long
# Inpt:
#   - dataFrame [df]: input data frame
#   - targetColumn [str]: column with standradized zipcodes
# Oupt:
#   - dataFrame [df]: data frame with two newn columns (lat/long) 
geocodeZipcode <- function(dataFrame, targetColumn, sc){
  
  # Require the zipcode package
  require(zipcode)
  
  # Load the internal data set from the package
  data(zipcode)
  
  # Change the column name to match with the Zipcode column in the dataFrame
  # so you can merge the two this will bring in the lat/longs needed to plot
  colnames(zipcode)[1] <- targetColumn
  
  zipcode_spark <- copy_to(sc, zipcode, overwrite = TRUE)
  # Merge the data frames together
  dataFrame <- dataFrame %>% inner_join(zipcode_spark, by = targetColumn)
  
  # Return the data frame back to the user
  return(dataFrame)
  
}
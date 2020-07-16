from pyspark.sql.functions import array, col, corr, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable 

def melt(
        df: DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def integral_analysis(df, startYear, endYear):

    '''
    Desc:
        Given a real estate data set with UID set for granularity of analytics,
        calculate integral of time-series values and correlation for real estate
        prices and local stock prices
     Inpt:
        - df [df]: data frame of real estate values
                - it is assumed that this is pre-processed with the following columns:
                        [UID]: Unique identifier of zipcode-bedroom combos
                        [DiffAvgSale_S]: % change difference of real estate sale prices yr-over-yr
                        [DiffAvgPrice_S]: % change difference of stock prices yr-over-yr 
        - startYear [int]: value for earliest year of data
        - endYear [int]: value for latest year of data
    Oupt:
        - df [df]: data frame with corresponding scaled integrals and correlation values
    '''

    correlation_df = df.groupby("UID").agg(corr('DiffAvgSale_S', 'DiffAvgPrice_S').alias("Correlation"))
    df = df.join(other = correlation, on = "UID", how = "inner")

    # Loop through each unique identifier of real estate properties and calculate
    # integral and correlation for each UID
    # for (i in unique(df$UID)){
      # cursub <- df[df$UID == i,]

      # Get years where UID's properties are sold
      # x <- cursub$Years
      # y1 <- cursub$DiffAvgSale_S
      # y2 <- cursub$DiffAvgPrice_S

      # Linear interpolation of values
      # f1 <- approxfun(x, y1-y2)

      # Quick function for getting absolute value
      # f2 <- function(x) abs(f1(x))   

      # Scaled integral 
      # integral_scaled <- integrate(f2, startYear, endYear, subdivisions = 2000)

      # Get correlation value
      # correlation_value <- cor(y1, y2)

      # Populate data frame with respective values
      # df$IntegralScore[df$UID == i] <- abs(integral_scaled$value)
      # df$Correlation[df$UID == i] <- correlation_value
      
    # }

    return(df)

# }    
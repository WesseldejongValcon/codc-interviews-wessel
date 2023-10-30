from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

def read_csv_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """
    Read data from a CSV file using Apache Spark.
    
    Parameters:
    csv_path (str): The path to the CSV file.

    Returns:
    DataFrame: A Spark DataFrame containing the CSV data.
    """
    return spark.read.format("csv").option("header", "True").load(f"{csv_path}")


def filter_countries(spark: SparkSession, df: DataFrame, column_name: str, country_list: List[str]) -> DataFrame:
    """
    Filter a DataFrame to include only rows where a specified column's values match any of the given countries.

    Parameters:
    spark (SparkSession): The Spark session used for DataFrame operations.
    df (DataFrame): The DataFrame to be filtered.
    column_name (str): The name of the column to filter on.
    country_list (List[str]): A list of country codes to filter for.

    Returns:
    DataFrame: A new DataFrame containing only the rows where the specified column's values match any of the given countries.

    Example:
    >>> spark = SparkSession.builder.appName("CountryFilter").getOrCreate()
    >>> data = [("United Kingdom", "London"), ("United States", "New York"), ("Netherlands", "Amsterdam")]
    >>> columns = ["Country", "City"]
    >>> df = spark.createDataFrame(data, columns)
    >>> filtered_df = filter_countries(spark, df, "Country", ["United Kingdom", "Netherlands"])
    >>> filtered_df.show()
    +--------------+---------+
    |       Country|     City|
    +--------------+---------+
    |United Kingdom|   London|
    |   Netherlands|Amsterdam|
    +--------------+---------+
    """
    country_string = ', '.join(f"'{country}'" for country in country_list)
    
    filter_statement = f"{column_name} IN ({country_string})"
    
    return df.where(f"{filter_statement}")


def rename_column(spark: SparkSession, df: DataFrame, old_column_name: str, new_column_name: str) -> DataFrame:
    return df.withColumnRenamed(old_column_name, new_column_name)    


def rename_columns_from_dict(spark: SparkSession, df: DataFrame, dict_name_changes: dict) -> DataFrame:
    print(f"dict_name_changes: {dict_name_changes}")
    for old_column_name, new_column_name in dict_name_changes.items():
        df = df.withColumnRenamed(old_column_name, new_column_name)
    return df


def drop_column(spark: SparkSession, df: DataFrame, column_name_to_drop: str) -> DataFrame:
    return df.drop(column_name_to_drop)
          
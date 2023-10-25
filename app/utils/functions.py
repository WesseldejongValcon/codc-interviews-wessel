from pyspark.sql import DataFrame

def read_csv_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """
    Read data from a CSV file using Apache Spark.
    
    Parameters:
    csv_path (str): The path to the CSV file.

    Returns:
    DataFrame: A Spark DataFrame containing the CSV data.
    """
    df = spark.read.format("csv").option("header", "True").load(f"{csv_path}")

    return df
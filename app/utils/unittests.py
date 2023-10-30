import functions as codc_interview_functions
from pyspark.sql import SparkSession
from chispa import assert_df_equality, schema_comparer
from pyspark.sql.types import StructField, StructType, StringType
import pytest

def test_filter_countries(spark: SparkSession):
    data = [
        ("United Kingdom", "London"),
        ("United States", "New York"),
        ("Netherlands", "Amsterdam"),
        (None, None)
    ]
    df = (spark.createDataFrame(data, ["Country", "City"]))
    
    actual_df = codc_interview_functions.filter_countries(spark, df, "Country", ["United Kingdom", "Netherlands"])
    print(actual_df)

    expected_data = [
        ("United Kingdom", "London"),
        ("Netherlands", "Amsterdam")
    ]
    expected_df = spark.createDataFrame(expected_data, ["Country", "City"])
    print(expected_df)
    
    assert_df_equality(actual_df, expected_df)
    print("Reached here")


def test_rename_column(spark: SparkSession):
    data = [
        ("United Kingdom", "London"),
        ("United States", "New York"),
        ("Netherlands", "Amsterdam"),
        (None, None)
    ]
    df = (spark.createDataFrame(data, ["Country", "City"]))

    actual_df = codc_interview_functions.rename_column(spark, df, "City", "Town")
    actual_schema = actual_df.schema

    expected_df = df.withColumnRenamed("City", "Town")
    expected_schema = StructType([
        StructField("City", StringType(), True),
        StructField("Town", StringType(), True)
    ])

    schema_comparer.assert_schema_equality(actual_schema, expected_schema, ignore_nullable=True)


def main():
    spark = (SparkSession.builder.master("local").appName("unittests_codc_interviews").getOrCreate())
    test_filter_countries(spark)
    test_rename_column(spark)
    print("Reached end")


if __name__ == '__main__':
    main()
import sys
import os

# Set PySpark and Python variables to same version
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Set source directory to root folder of project
source_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, source_directory)

from app.utils import functions as codc_interview_functions
from pyspark.sql import SparkSession
from chispa import assert_df_equality, schema_comparer
from pyspark.sql.types import StructField, StructType, StringType
import pytest

@pytest.fixture(scope='class')
def spark_session(request):
    spark = SparkSession.builder.master("local[*]").appName("unittests_codc_interviews").getOrCreate()

    request.cls.spark = spark

@pytest.mark.usefixtures("spark_session")
class TestDataFrameFunctions():
    def test_filter_countries(self):
        data = [
            ("United Kingdom", "London"),
            ("United States", "New York"),
            ("Netherlands", "Amsterdam"),
            (None, None)
        ]
        df = (self.spark.createDataFrame(data, ["Country", "City"]))
        
        actual_df = codc_interview_functions.filter_countries(self.spark, df, "Country", ["United Kingdom", "Netherlands"])

        expected_data = [
            ("United Kingdom", "London"),
            ("Netherlands", "Amsterdam")
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["Country", "City"])
        
        assert_df_equality(actual_df, expected_df)


    def test_rename_column(self):
        data = [
            ("United Kingdom", "London"),
            ("United States", "New York"),
            ("Netherlands", "Amsterdam"),
            (None, None)
        ]
        df = (self.spark.createDataFrame(data, ["Country", "City"]))

        actual_df = codc_interview_functions.rename_column(self.spark, df, "City", "Town")
        actual_schema = actual_df.schema

        expected_df = df.withColumnRenamed("City", "Town")
        expected_schema = StructType([
            StructField("Country", StringType(), True),
            StructField("Town", StringType(), True)
        ])

        schema_comparer.assert_schema_equality(actual_schema, expected_schema, ignore_nullable=True)
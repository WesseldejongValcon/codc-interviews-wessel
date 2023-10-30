import pytest

from chispa.column_comparer import assert_column_equality
import pyspark.sql.functions as F

from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())

def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")

def test_remove_non_word_characters_short():
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    df = (spark.createDataFrame(data, ["name", "expected_name"])
        .withColumn("clean_name", remove_non_word_characters(F.col("name"))))
    assert_column_equality(df, "clean_name", "expected_name")

test_remove_non_word_characters_short()


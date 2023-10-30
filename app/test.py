from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local").appName("test_result_size").getOrCreate())

print(spark.conf.get("spark.driver.maxResultSize"))
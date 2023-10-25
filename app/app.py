from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local").appName("WesselTest").getOrCreate())

import pyspark.sql.functions as F
import utils.functions

df_customer = spark.read.format("csv").option("header", "True").load("C:/Users/Wessel de Jong/OneDrive - Valcon Business Development A S/Documenten/ABN/codc-interviews/codc-interviews-wessel/dataset_one.csv")
df_financial_data = spark.read.format("csv").option("header", "True").load("C:/Users/Wessel de Jong/OneDrive - Valcon Business Development A S/Documenten/ABN/codc-interviews/codc-interviews-wessel/dataset_two.csv")

print(df_financial_data.count())

#TODO: 
# Chispa voor elkaar krijgen

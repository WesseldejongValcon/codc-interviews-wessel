from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local").appName("codc_interview_wessel").getOrCreate())

import pyspark.sql.functions as F
import utils.functions as codc_interview_functions
import logging
import datetime

logging.basicConfig(filename=f"app/logs/codc_interview_logs.log", level=logging.DEBUG)

pyspark_log = logging.getLogger("pyspark").setLevel(logging.ERROR)
py4j_logger = logging.getLogger("py4j").setLevel(logging.ERROR)
logging.info(f"Application codc_interview_wessel started at {datetime.datetime.now().strftime('%d %B, %Y - %H:%M:%S')}")

df_customer = codc_interview_functions.read_csv_data(spark, csv_path="C:/Users/Wessel de Jong/OneDrive - Valcon Business Development A S/Documenten/ABN/codc-interviews/codc-interviews-wessel/data/dataset_one.csv")
df_financial_data = codc_interview_functions.read_csv_data(spark, csv_path="C:/Users/Wessel de Jong/OneDrive - Valcon Business Development A S/Documenten/ABN/codc-interviews/codc-interviews-wessel/data/dataset_two.csv")
df_customer = codc_interview_functions.filter_countries(spark, df_customer, "country", ["United Kingdom", "Netherlands"])
financial_data_column_renames_dict = {
    "id": "client_identifier",
    "btc_a": "bitcoin_address",
    "cc_t": "credit_card_type"
}
df_financial_data = codc_interview_functions.rename_columns_from_dict(spark, df_financial_data, financial_data_column_renames_dict)
df_financial_data = df_financial_data.drop("cc_n")
df_joined = df_customer.join(df_financial_data, on=df_customer.id == df_financial_data.client_identifier, how="inner")

print(df_customer)
print(df_financial_data)
print(df_customer.count())
print(df_joined)

logging.info(f"Application has run successfully and ended at {datetime.datetime.now().strftime('%d %B, %Y - %H:%M:%S')}")
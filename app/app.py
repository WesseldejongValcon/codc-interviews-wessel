from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import utils.functions as codc_interview_functions
import logging
import datetime

def main():
    # Get or create Spark session
    spark = (SparkSession.builder.master("local").appName("codc_interview_wessel").getOrCreate())

    # Configure logging
    codc_interview_functions.configure_logging()
    logging.info(f"Application codc_interview_wessel started at {datetime.datetime.now().strftime('%d %B, %Y - %H:%M:%S')}")

    # Get filepaths
    filepath_customer_data = codc_interview_functions.get_filepath_data(spark, "Select customer data")
    filepath_financial_data = codc_interview_functions.get_filepath_data(spark, "Select financial data")

    # Get countries
    country_list = codc_interview_functions.get_countries(spark)
    logging.info(f"User gave the following input: {', '.join(f'{country}' for country in country_list)}")

    # Read in data
    df_customer = codc_interview_functions.read_csv_data(spark, csv_path=filepath_customer_data)
    df_financial_data = codc_interview_functions.read_csv_data(spark, csv_path=filepath_financial_data)

    # Transform data
    df_customer = codc_interview_functions.filter_countries(spark, df_customer, "country", country_list)
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

if __name__ == '__main__':
    main()
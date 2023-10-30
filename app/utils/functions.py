from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import logging
from tkinter.filedialog import askopenfilename
import tkinter as tk

def configure_logging():
    # Set up and configure logging
    logging.basicConfig(filename=f"app/logs/codc_interview_logs.log", level=logging.DEBUG)
    pyspark_log = logging.getLogger("pyspark").setLevel(logging.ERROR)
    py4j_logger = logging.getLogger("py4j").setLevel(logging.ERROR)


def get_filepath_data(spark: SparkSession, title: str) -> str:
    return askopenfilename(title=title)


def get_countries(spark: SparkSession) -> List[str]:
    """
    Ask user to enter the desired country names.

    Parameters:
    spark (SparkSession): The Spark session used for DataFrame operations.

    Returns:
    A list with country names in String format.
    """
    root = tk.Tk()
    root.title("Country Input")

    input_country_list = []

    # Function to open input dialog
    def get_input():
        input_window = tk.Toplevel(root)
        input_window.title("Country")
        input_window.geometry("700x350")

        entry = tk.Entry(input_window)
        entry.pack()

        def confirm_input():
            user_input = entry.get()
            input_country_list.append(user_input)
            entry.delete(0, tk.END)
            input_window.destroy()

        confirm_button = tk.Button(input_window, text="Confirm", command=confirm_input)
        confirm_button.pack()

        stop_button = tk.Button(input_window, text="Stop", command=input_window.destroy)
        stop_button.pack()
    
    # Function to exit the input window
    def exit_input():
        root.destroy()
        root.quit()
    
    get_input_button = tk.Button(root, text="Add Country Name", command=get_input)
    get_input_button.pack()

    stop_input_button = tk.Button(root, text="Done", command=exit_input)
    stop_input_button.pack()

    root.mainloop()

    return input_country_list


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
    """
    Renames a column of an input DataFrame

    Parameters:
    spark (SparkSession): The Spark session used for DataFrame operations.
    df (DataFrame): The DataFrame to be filtered.
    old_column_name (str): The name of the column to be changed.
    new_column_name (str): The name the column should become.
    """
    return df.withColumnRenamed(old_column_name, new_column_name)    


def rename_columns_from_dict(spark: SparkSession, df: DataFrame, dict_name_changes: dict) -> DataFrame:
    print(f"dict_name_changes: {dict_name_changes}")
    for old_column_name, new_column_name in dict_name_changes.items():
        df = df.withColumnRenamed(old_column_name, new_column_name)
    return df


def drop_column(spark: SparkSession, df: DataFrame, column_name_to_drop: str) -> DataFrame:
    return df.drop(column_name_to_drop)
          
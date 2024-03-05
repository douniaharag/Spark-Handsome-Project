from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def calculate_population_by_department(df):
    return df.groupBy("department").agg(count("*").alias("nbpeople")).orderBy("nbpeople", "department")


def write_csv(df, output_path):
    # Coalesce the DataFrame into 1 partition; this will result in a single CSV part file in the output directory
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

def main():
    # Creation of the SparkSession
    spark = SparkSession.builder.master("local[*]").appName("aggregate_job").getOrCreate()

    # Reading the clean file
    clean_df = spark.read.parquet("../../../../fr/hymaia/exo2/clean/data/exo2/output")  

    # Calculating the population by department
    population_by_department_df = calculate_population_by_department(clean_df)

    # Writing the result in CSV format
    write_csv(population_by_department_df, "data/exo2/aggregate")  # Using output_path

    # Reading the CSV file
    df = spark.read.csv("data/exo2/aggregate", header=True)  # Using output_path

    # Stopping the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()

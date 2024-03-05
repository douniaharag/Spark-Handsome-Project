from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import time


def add_category_name_udf(category):
    if category < 6:
        return "food"
    else:
        return "furniture"

add_category_name = udf(add_category_name_udf, StringType())

def main():
    spark = SparkSession.builder.master("local[*]").appName("python_udf").getOrCreate()

    sell_df = spark.read.csv("resources/exo4/sell.csv", header=True, inferSchema=True)

    start_time = time.time()

    sell_df = sell_df.withColumn("category_name", add_category_name(col("category")))


    print("\nTemps d'exécution :")
    print(f"\t{round(time.time() - start_time, 2)} seconds")
   

if __name__ == "__main__":
    main()

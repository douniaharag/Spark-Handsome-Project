import time

from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq

spark = SparkSession.builder.appName("exo4").master(
    "local[*]").config('spark.jars', 'resources/exo4/udf.jar').getOrCreate()


def addCategoryName(col):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    sell_df = spark.read.csv("resources/exo4/sell.csv", header=True)
    start_time = time.time()
    sell_df = sell_df.withColumn("category_name", addCategoryName(sell_df["category"]))

    print("\nTemps d'exécution :")
    print(f"\t{round(time.time() - start_time, 2)} seconds")

if __name__ == "__main__":
    main()
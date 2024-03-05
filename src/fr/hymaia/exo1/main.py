import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col


def main():
    # Création de la SparkSession
    spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()

    # Chemin correct du fichier d'entrée en se basant sur la structure de dossier fournie
    inputfilepath = "../../../resources/exo1/data.csv"
    df = spark.read.csv(inputfilepath, header=True, inferSchema=True)

    # Assurez-vous que la colonne 'text' est bien présente dans votre CSV
    result = wordcount(df)
    result.show()

    # Chemin correct du dossier de sortie
    output_path = "../../../data/exo1/output"

    # Écriture du résultat au format parquet, partitionné par count et en mode overwrite
    result.write.partitionBy("count").parquet(output_path, mode="overwrite")

    # Le print n'est pas nécessaire pour le fonctionnement du script Spark
    # print("Hello world!")

    spark.stop()

def wordcount(df):
    result_df = (
        df.select(explode(split(col("text"), " ")).alias("word")) \
          .groupBy("word") \
          .count() \
    )
    return result_df




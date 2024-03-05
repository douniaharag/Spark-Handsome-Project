# spark_clean_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, when

def main():
    
    spark = SparkSession.builder.master("local[*]").appName("cleanjob").getOrCreate()

    clients_df = read_csv(spark, "../../../../resources/exo2/clients_bdd.csv")
    villes_df = read_csv(spark, "../../../../resources/exo2/city_zipcode.csv")

    # Filtrage des clients majeurs
    filtered_clients_df = filter_major_clients(clients_df)

    # Jointure avec les villes
    result_df = join_with_cities(filtered_clients_df, villes_df)

    # Ajout de la colonne département
    result_df = add_department_column(result_df)

    # Correction pour la Corse
    result_df = specifiteCorse(result_df)

    # Écriture du résultat au format Parquet
    write_parquet(result_df, "data/exo2/output")  

    
    # Arrêt de la SparkSession
    spark.stop()

def read_csv(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def filter_major_clients(df):
    return df.filter(col("age") >= 18)

def join_with_cities(clients_df, villes_df):
    # Effectuer une jointure externe gauche sur la colonne "zip"
    joined_df = clients_df.join(villes_df, "zip", "left_outer")
    return joined_df

def add_department_column(df):
    df = df.withColumn("department", 
                       when(col("zip").substr(1, 3).isin(["971", "972", "973", "974", "975", "976", "977", "978", "984", "986", "987", "988"]),
                            col("zip").substr(1, 3))
                       .otherwise(substring(col("zip"), 1, 2)))
    return df

def specifiteCorse(df):
    df = df.withColumn(
        "department",
        when((col("department") == "20") & (col("zip").cast("int") <= 20190), "2A")
        .otherwise(when((col("department") == "20") & (col("zip").cast("int") > 20190), "2B")
        .otherwise(col("department")))
    )
    return df

def write_parquet(df, output_path):
    df.write.parquet(output_path, mode="overwrite")

if __name__ == "__main__":
    main()

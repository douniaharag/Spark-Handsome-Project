import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.clean.spark_clean_job import add_department_column, join_with_cities, specifiteCorse


class SparkCleanJobTests(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    # _______________________________________Tests unitaires______________________________________________

    def test_join_clients_cities(self):
        # Given
        clients_data = [("John", 25, "12345"), ("Alice", 30, "67890")]
        villes_data = [("12345", "Paris"), ("67890", "Berlin")]

        clients_df = self.spark.createDataFrame(clients_data, ["name", "age", "zip"])
        villes_df = self.spark.createDataFrame(villes_data, ["zip", "city"])

        # When
        actual = join_with_cities(clients_df, villes_df)

        # Then
        expected = self.spark.createDataFrame([Row(name="John", age=25, zip="12345", city="Paris"),
                                               Row(name="Alice", age=30, zip="67890", city="Berlin")])

        actual_sorted = actual.orderBy("zip")
        expected_sorted = expected.orderBy("zip")

        actual_values = actual_sorted.collect()
        expected_values = expected_sorted.collect()

        for actual_row, expected_row in zip(actual_values, expected_values):
            self.assertEqual(actual_row.asDict(), expected_row.asDict())

    def test_specifite_corse(self):
        # Given
        df = self.spark.createDataFrame([Row(department="20", zip=20180), Row(department="20", zip=20200)])

        # When
        actual = specifiteCorse(df)

        # Then
        expected = self.spark.createDataFrame([Row(department="2A", zip=20180), Row(department="2B", zip=20200)])

        actual_sorted = actual.orderBy("zip")
        expected_sorted = expected.orderBy("zip")
        self.assertTrue(actual_sorted.collect() == expected_sorted.collect())


# _______________________________________Test d'intégration______________________________________________

    def test_integration_clean(self):
        # Given
        clients_data = [
            ("Pierre", 25, "20190", "Ajaccio"),
            ("Paul", 30, "97206", "Fort-de-France"),
            ("Jacques", 22, "75001", "Paris")
        ]

        villes_data = [
            ("20190", "Ajaccio"),
            ("97206", "Fort-de-France"),
            ("75001", "Paris")
        ]

        clients_df = self.spark.createDataFrame(clients_data, ["name", "age", "zip", "city"])
        villes_df = self.spark.createDataFrame(villes_data, ["zip", "city"])

        # When
        joined_df = join_with_cities(clients_df, villes_df).drop(villes_df.city)
        df_with_department = add_department_column(joined_df)
        df_finale = specifiteCorse(df_with_department)

        # Then
        expected_data = [
            ("Pierre", 25, "20190", "Ajaccio", "2A"),
            ("Paul", 30, "97206", "Fort-de-France", "972"),
            ("Jacques", 22, "75001", "Paris", "75")
        ]

        expected_df = self.spark.createDataFrame(expected_data, ["name", "age", "zip", "city", "department"])

        df_finale_sorted = df_finale.orderBy("zip").select("name", "age", "zip", "city", "department") 
        expected_df_sorted = expected_df.orderBy("zip")

        self.assertTrue(df_finale_sorted.collect() == expected_df_sorted.collect())



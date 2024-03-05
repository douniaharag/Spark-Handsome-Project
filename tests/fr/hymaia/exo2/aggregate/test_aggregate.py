import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from src.fr.hymaia.exo2.aggregate.spark_aggregate_job import calculate_population_by_department, write_csv
import tempfile
import shutil

class SparkAggregateJobTests(unittest.TestCase):
    
# _______________________________________Tests unitaires______________________________________________

    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_calculate_population_by_departement(self):
        # Given
        df = self.spark.createDataFrame([Row(department="75"), Row(department="2B"), Row(department="75")])

        # When
        actual = calculate_population_by_department(df)

        # Then
        expected = self.spark.createDataFrame([Row(department="2B", nb_people=1),
                                               Row(department="75", nb_people=2)])
        self.assertTrue(sorted(actual.collect()) == sorted(expected.collect()))



# _______________________________________Test d'intégration______________________________________________
  
    def create_test_data(self):
        # Given
        data = [
            ("Pierre", 25, "75001", "Paris", "75"),
            ("Paul", 30, "97206", "Fort-de-France", "972"),
            ("Jacques", 22, "75001", "Paris", "75"),
            ("Jean", 35, "13001", "Marseille", "13")
        ]
        return self.spark.createDataFrame(data, ["name", "age", "zip", "city", "department"])

    def test_integration_aggregate(self):
        # Given: A DataFrame created from test data
        test_df = self.create_test_data()

        # When: Calculating population by department and writing to CSV
        population_by_department_df = calculate_population_by_department(test_df)
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = tmp_dir
            write_csv(population_by_department_df, output_path)
            
            # Then: The written data should match the expected result
            expected_data = [("75", 2), ("972", 1), ("13", 1)]
            expected_df = self.spark.createDataFrame(expected_data, ["department", "nbpeople"])
            result_sorted = population_by_department_df.sort("department").collect()
            expected_sorted = expected_df.sort("department").collect()
            self.assertEqual(len(result_sorted), len(expected_sorted), "Number of rows mismatch in calculation")
            for r, e in zip(result_sorted, expected_sorted):
                self.assertEqual(r, e, f"Row mismatch in calculation: {r} != {e}")
            
            written_df = self.spark.read.csv(output_path + "/*.csv", header=True)
            self.assertEqual(written_df.count(), population_by_department_df.count(), "Mismatch in number of rows written to CSV")

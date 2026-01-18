from pyspark.sql import SparkSession

from iceberg_tables.iceberg_table import IcebergTable


class ParquetWriter:
    def __init__(self, spark: SparkSession, table_list: set[IcebergTable]):
        self.spark = spark
        self.table_list = table_list

    def execute(self):
        for table in self.table_list:
            random_data = table.generate_random_data()
            df_parquet = self.spark.createDataFrame(random_data, table.schema)
            df_parquet.write \
                .partitionBy(table.partition_column) \
                .mode("append") \
                .parquet(table.data_path)

            print(f"Written {df_parquet.count()} rows in the {table.name} flow ... Not yet committed.")

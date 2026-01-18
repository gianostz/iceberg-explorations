from pyspark.sql import SparkSession

from iceberg_tables.iceberg_table import IcebergTable
from iceberg_tables.sales_model import SalesModel
from writer.parquet_writer import ParquetWriter
from maintainer.table_maintainer import TableMaintainer


def loop(spark: SparkSession, maintainer: TableMaintainer, writer: ParquetWriter):
    while True:
        maintainer.execute()
        print("Maintainer has committed!")
        get_tables_count(spark, maintainer.table_list)
        writer.execute()
        print("Writer has been executed!")
        get_tables_count(spark, maintainer.table_list)

def get_tables_count(spark: SparkSession, table_list: set[IcebergTable]):
    for table in table_list:
        full_table_name = f"{table.database}.{table.name}"
        count = spark.read.table(full_table_name).count()
        print(f"{full_table_name}: count = {count}")
        count_distinct = spark.read.table(f"{table.database}.{table.name}").distinct().count()
        print(f"{full_table_name}: count_distinct = {count_distinct}")
        if count != count_distinct:
            raise RuntimeError(f"Duplicates in table {full_table_name}!")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Iceberg Test") \
        .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0') \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.local.type", "hadoop") \
        .config(f"spark.sql.catalog.local.warehouse", "../warehouse") \
        .config("spark.sql.defaultCatalog", "local") \
        .config("spark.sql.catalog.local.default.write.metadata-flush-after-create", "true") \
        .getOrCreate()

    warehouse_path = "../warehouse"
    database = "iceberg_db"

    sales_table = SalesModel(warehouse_path, database)
    table_list: set[IcebergTable] = {sales_table}

    tables_maintainer = TableMaintainer(spark, table_list)
    parquet_writer = ParquetWriter(spark, table_list)

    loop(spark, tables_maintainer, parquet_writer)

    spark.stop()


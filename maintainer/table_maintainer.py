from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
from pyspark.sql.functions import col

from iceberg_tables.iceberg_table import IcebergTable


class TableMaintainer:
    def __init__(self, spark: SparkSession, table_list: set[IcebergTable]):
        self.spark = spark
        self.table_list = table_list

    def execute(self):
        for table in self.table_list:
            self.__create_if_not_exists(table)
            if self.__path_exists(table.data_path):
                self.__create_view_by_newdata(table)
                self.__execute_upsert(table)
                self.__delete_expired_data(table)

    def __path_exists(self, path: str) -> bool:
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        spark_jvm = self.spark._jvm
        java_import(spark_jvm, 'org.apache.hadoop.fs.FileSystem')
        java_import(spark_jvm, 'org.apache.hadoop.fs.Path')
        jvm_path = spark_jvm.Path(path)
        fs = spark_jvm.FileSystem.get(hadoop_conf)
        return fs.exists(jvm_path)

    def __create_if_not_exists(self, table: IcebergTable):
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {table.database}")
        self.spark.sql(table.create_table_string())


    def __create_view_by_newdata(self, table: IcebergTable):
        df_parquet_files = self.spark.read.parquet(table.data_path)
        df_deduplicated = df_parquet_files \
            .orderBy(col(table.timestamp_column).desc()) \
            .dropDuplicates([table.primary_key])
        df_deduplicated.createOrReplaceTempView(f"{table.name}_view")


    def __execute_upsert(self, table: IcebergTable):
        view_name = f"{table.name}_view"
        self.spark.sql(
            f'''
            MERGE INTO {table.database}.{table.name} AS target
            USING {view_name} AS source
            ON target.{table.primary_key} = source.{table.primary_key}
            WHEN MATCHED AND target.{table.timestamp_column} < source.{table.timestamp_column} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            '''.lstrip()
        )

    def __delete_expired_data(self, table: IcebergTable):
        expiration_timestamp: str = self.spark.sql(f"SELECT CURRENT_TIMESTAMP() - INTERVAL {table.retention}").first()[0]
        vacuum_timestamp: str = self.spark.sql(f"SELECT CURRENT_TIMESTAMP() - INTERVAL 1 DAYS").first()[0]
        self.spark.sql(
            f'''
            DELETE FROM {table.database}.{table.name}
            WHERE event_time < '{expiration_timestamp}'
            '''.lstrip()
        )
        self.spark.sql(f"CALL local.system.expire_snapshots('{table.database}.{table.name}', TIMESTAMP '{expiration_timestamp}', 10)")
        self.spark.sql(
            f'''
            CALL local.system.remove_orphan_files(
                table => '{table.database}.{table.name}',
                older_than => TIMESTAMP '{vacuum_timestamp}'
            )
            '''.lstrip()
        )

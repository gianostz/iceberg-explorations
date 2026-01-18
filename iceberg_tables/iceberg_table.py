from abc import ABC

from pyspark.sql.types import StructType
import random


class IcebergTable(ABC):
    def __init__(self, warehouse_path: str, database: str):
        self.warehouse_path = warehouse_path
        self.database = database
        self.data_path = f"{self.warehouse_path}/{self.database}/{self.name}/data"
        self.metadata_path = f"{self.warehouse_path}/{self.database}/{self.name}/metadata"

    name = None
    schema = StructType()
    primary_key = None
    timestamp_column = "event_time"
    partition_column = "category"
    retention = "1 DAYS"

    def create_table_string(self): pass

    @staticmethod
    def generate_random_data(num_rows: int = random.randint(1, 100)): pass

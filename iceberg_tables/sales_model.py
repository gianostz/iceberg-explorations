from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType
import random
from datetime import datetime, timedelta
from faker import Faker

from iceberg_tables.iceberg_table import IcebergTable


class SalesModel(IcebergTable):
    def __init__(self, warehouse_path: str, database: str):
        super().__init__(warehouse_path, database)

    name = "sales"
    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("store_id", StringType(), True),
        StructField("sales_amount", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("category", IntegerType(), False)
    ])
    primary_key = "product_id"


    def create_table_string(self):
        return f'''
            CREATE TABLE IF NOT EXISTS {self.database}.{self.name} (
                product_id INT,
                store_id STRING,
                sales_amount INT,
                event_time TIMESTAMP,
                category INT
            ) 
            USING iceberg
            PARTITIONED BY (category)
            TBLPROPERTIES (
                'write.metadata.previous-versions-max' = 10,
                'write.metadata.delete-after-commit.enabled' = 'true'
            )
            '''.lstrip()

    @staticmethod
    def generate_random_data(num_rows: int = random.randint(1, 100)):
        fake = Faker()
        data = []
        for _ in range(num_rows):
            product_id = random.randint(1, 50)
            store_id = fake.uuid4()[:8]
            sales_amount = random.randint(1, 500)
            event_time = datetime.now() - timedelta(days=random.randint(0, 1))
            category = random.randint(1, 5)
            data.append((product_id, store_id, sales_amount, event_time, category))
        return data

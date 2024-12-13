import pandas as pd
import pyarrow.parquet as pq

class ParquetReader:
    def __init__(self, filepath):
        self.filepath = filepath
        self.parquet_file = None

    def read_data(self):
        return pd.read_parquet(self.filepath)

    def get_metadata(self):
        self.parquet_file = pq.ParquetFile(self.filepath)
        return {
            'num_rows': self.parquet_file.metadata.num_rows,
            'num_columns': self.parquet_file.metadata.num_columns,
            'created_by': self.parquet_file.metadata.created_by,
            'schema': self.parquet_file.schema,
        }
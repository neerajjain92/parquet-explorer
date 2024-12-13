import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class ParquetCreator:
    def __init__(self):
        self.data = None

    def create_sample_data(self):
        self.data = {
            'Name': ['Alice', 'Bob', 'Charlie', 'David'],
            'Age': [25, 30, 35, 40],
            'Salary': [50000, 60000, 70000, 80000],
        }
        return pd.DataFrame(self.data)

    def write_parquet(self, df: pd.DataFrame, filepath, compression='snappy'):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filepath, compression=compression)
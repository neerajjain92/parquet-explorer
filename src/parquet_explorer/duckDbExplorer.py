from pathlib import Path

import duckdb


class DuckDbExplorer:
    def __init__(self, parquet_file_path):
        self.conn = duckdb.connect()
        self.parquet_path = Path(parquet_file_path)

    def query_parquet(self, sql_query):
        """Execute SQL Query on parquet file"""
        try:
            result = self.conn.execute(sql_query).fetchdf()
            return result
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def get_table_info(self):
        """Get actual table schema"""
        try:
            query = f"DESCRIBE SELECT * FROM '{self.parquet_path}'"
            return self.query_parquet(query)
        except Exception as e:
            print(f"Error getting schema: {e}")
            return None

    def sample_data(self, limit=5):
        """Get sample rows from parquet file"""
        try:
            query = f"SELECT * FROM read_parquet('{self.parquet_path}') LIMIT {limit}"
            return self.query_parquet(query)
        except Exception as e:
            print(f"Error getting sample data: {e}")
            return None

    def count_records(self):
        """Count total number of records from parquet file"""
        query = f"SELECT COUNT(*) as total_records FROM `{self.parquet_path}`"
        result = self.query_parquet(query)

    def close(self):
        """Close the DuckDB connection"""
        self.conn.close()
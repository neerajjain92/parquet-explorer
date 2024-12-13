from random import expovariate

from src.parquet_explorer.creator import ParquetCreator
from src.parquet_explorer.duckDbExplorer import DuckDbExplorer
from src.parquet_explorer.reader import ParquetReader
from src.parquet_explorer.visualizer import ParquetVisualizer


def main():
    # Create and write parquet file
    parquet_file_path = "data/sample.parquet"
    creator = ParquetCreator()
    df = creator.create_sample_data()
    creator.write_parquet(df, parquet_file_path)

    # Read Parquet file
    reader = ParquetReader(parquet_file_path)
    df_read = reader.read_data()
    metadata = reader.get_metadata()

    print('Parquet file contents')
    print(df_read)
    print("\n Metadata:")
    for key, value in metadata.items():
        print(f"{key}: {value}")

    # Visualize Data
    visualizer = ParquetVisualizer(df_read)
    visualizer.plot_bar('Name', 'Age', 'Employee Age Distribution')

    # Initialize DuckDB explorer
    explorer = DuckDbExplorer(parquet_file_path)

    # Get schema info
    print("\n Table Schema:")
    print(explorer.get_table_info())

    # Get sample data
    print("\n Sample Data:")
    print(explorer.sample_data())

    # Run custom query
    custom_query = f"SELECT Name, Salary FROM'{parquet_file_path}' WHERE Age > 25 ORDER BY Salary ASC"
    print("\n Custom Query Results:")
    print(explorer.query_parquet(custom_query))

    # Close connection
    explorer.close()

if __name__ == '__main__':
    main()
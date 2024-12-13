import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

class ParquetVisualizer:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def plot_bar(self, x_col, y_col, title=None):
        plt.figure(figsize=(10, 6))
        sns.barplot(x=x_col, y=y_col, data=self.df)
        plt.title(title or f'{y_col} Distribution by {x_col}')
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
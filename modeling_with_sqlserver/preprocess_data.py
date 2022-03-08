import pandas as pd

class DataPipeLine:
    def __init__(self, data_path) -> None:
        self.data_path = data_path
        self.data = pd.read_csv(self.data_path)
    
    def get_data(self, col_name):
        return self.data[col_name].to_list()

    def HandlingNullData(self):
        missing = self.data.isna().sum().reset_index()
        missing.columns = ['column_name', 'num_nan']
        column_missing = missing.loc[missing['num_nan'] > 0, 'column_name'].to_list()

        # fill NA by median
        def fill_na(column_name):
            median = self.data[column_name].median()
            self.data[column_name].fillna(median, inplace=True)
        
        for col in column_missing:
            fill_na(col)
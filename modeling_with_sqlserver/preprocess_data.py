import pandas as pd
from pandas.api.types import is_string_dtype

class DataPipeLine:
    def __init__(self, datapath, data) -> None:
        super(DataPipeLine, self).__init__()
        self.data_path = datapath
        self.data = data

    @classmethod
    def preprocess_data(cls, data_path):
        raw_data = pd.read_csv(data_path)
        data = raw_data.copy(deep=True)

        # rename column
        data.rename(columns={'ID':'User_ID'}, inplace=True)
        data.rename(columns={' Income ':'Income'}, inplace=True)
        data.rename(columns={'Dt_Customer': 'Date_Enroll'}, inplace = True)
        data.rename(columns={'MntMeatProducts': 'MntMeats'}, inplace = True)
        data.rename(columns={'MntFishProducts': 'MntFishs'}, inplace = True)
        data.rename(columns={'MntSweetProducts': 'MntSweets'}, inplace = True)
        data.rename(columns={'MntGoldProds': 'MntGolds'}, inplace = True)

        # categorical country
        data['Country'].unique()
        value_to_replace = {'SA': 'South Africa', 
                    'CA': 'Canada', 
                    'SP': 'Spain', 
                    'GER': 'Germany', 
                    'IND': 'India', 
                    'US': 'United States',
                    'AUS': 'Australia',
                   'ME': 'Montenegro'}
        data['Country'].replace(value_to_replace, inplace=True)

        # format datetime
        data['Date_Enroll'] = pd.to_datetime(data['Date_Enroll'])

        # format type
        data["Income"] = data["Income"].str.replace("$","").str.replace(",", "")
        data["Income"] = data["Income"].astype(float)

        data["MntWines"] = data["MntWines"].astype(float)
        data["MntFruits"] = data["MntFruits"].astype(float)
        data["MntMeats"] = data["MntMeats"].astype(float)
        data["MntFishs"] = data["MntFishs"].astype(float)
        data["MntSweets"] = data["MntSweets"].astype(float)
        data["MntGolds"] = data["MntGolds"].astype(float)

        # handling null data
        data = cls.HandlingNullData(data)

        return cls(data_path, data)

    @staticmethod
    def HandlingNullData(dataframe):
        data = dataframe.copy(deep=True)
        missing = data.isna().sum().reset_index()
        missing.columns = ['column_name', 'num_nan']
        column_missing = missing.loc[missing['num_nan'] > 0, 'column_name'].to_list()

        def fill_na(column_name):
            if is_string_dtype(column_name):
                data[column_name] = data[column_name].ffill().bfill()
            else:
                median = data[column_name].median()
                data[column_name].fillna(median, inplace=True)
        
        for col in column_missing:
            fill_na(col)
        
        return data
        
        
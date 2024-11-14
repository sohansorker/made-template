import pandas as pd
import json, sys, calendar
from sqlalchemy import create_engine

class Pipeline:
    def __init__(self):
        self.engine = create_engine('sqlite:///data//FinalDB.sqlite')

    def download_temperature_data(self):
        # Define the list of URLs
        url_list = [
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_01.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_02.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_03.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_04.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_05.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_06.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_07.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_08.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_09.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_10.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_11.txt",
            "https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_12.txt",  
        ]

        # Initialize an empty list to store the DataFrames
        dataframes = []

        # Loop through each URL and read data into a DataFrame
        for url in url_list:
            # Use pd.read_csv to read data from the URL
            df_temp = pd.read_csv(url, sep=';', skiprows=1, skipfooter=0)
            dataframes.append(df_temp)


        # Concatenate the list of DataFrames into a single DataFrame
            df = pd.concat(dataframes, ignore_index=True)

        #removed unnamed column
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

            df = df.drop(columns=['Deutschland'])

        #sorting data
            df = df.sort_values(by=['Jahr', 'Monat'])

        #rename header
            df = df.rename(columns={"Jahr": "Year", "Monat": "Month"})
        #keep data from 2018 to 2020
            df = df.loc[(df['Year'] >= 2018) & (df['Year'] <= 2020)]
        #change month value int to month name
            df['Month'] = df['Month'].replace(
                            [num for num in range(1, 13)],
                            [month for month in calendar.month_name[1:]])
        #creating new column Date with year and month
            df.insert(loc=2, column="Date", value=df['Month'] + "-" + df['Year'].astype(str)) 
            df = df.reset_index(drop=True)


        # Store the temperature data in the database
        df.to_sql('AvgTemp', self.engine, if_exists='replace', index=False)

    def collect_additional_data(self):
        # collect additional data from another URL
        url2 = "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD"
        df2 = pd.read_csv(url2, sep=';', skiprows=6, skipfooter=3, engine='python')

        # Deleting rows 0 and 2
        df2 = df2.drop([0, 2])

        df2 = df2.reset_index(drop=True)

       


        # Store the additional data in the same database
        df2.to_sql('AccData', self.engine, if_exists='replace', index=False)
        self.engine.dispose()
        

    def run_pipeline(self):
        # Run the entire pipeline
        self.download_temperature_data()
        self.collect_additional_data()

# Create an instance of the Pipeline class
pipeline_instance = Pipeline()

# Run the pipeline
pipeline_instance.run_pipeline()

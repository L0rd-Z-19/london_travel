import pandas as pd
import sqlalchemy
from sqlalchemy.types import Integer, Text, String, Float
import psycopg2
from config import user, pas

#https://github.com/mdpickett2
london_201718 = pd.read_csv('Resources/2017-18-1.csv', encoding = "ISO-8859-1")
london1_df = london_201718[['Type', 'Trip Length Days', 'Reason for travel']].copy()
london1_df = london1_df.rename(columns={"Type": "Travel Type"})
london1_df.drop(london1_df.loc[london1_df['Travel Type']=='Hotel'].index, inplace=True)
london1_df.drop(london1_df.loc[london1_df['Travel Type']=='Ancillary'].index, inplace=True)
london1_df.reset_index(drop=True, inplace=True)
london1_df.rename(columns= {'Travel Type': 'travel_type','Trip Length Days':'trip_length','Reason for travel':'reason_for_travel'}, inplace=True)

#https://github.com/alfredflowss
df = pd.read_csv("Resources/2018-19.csv", encoding = "ISO-8859-1")
travel_data1819 = df[['Travel Type','Trip Length (Days)','Reason for travel']]
travel_data1819.drop(travel_data1819.loc[travel_data1819['Travel Type']=='Hotel'].index, inplace=True)
travel_data1819.drop(travel_data1819.loc[travel_data1819['Travel Type']=='Ancillary'].index, inplace=True)
travel_data1819.reset_index(drop=True, inplace=True)
travel_data1819.rename(columns= {'Travel Type': 'travel_type','Trip Length (Days)':'trip_length','Reason for travel':'reason_for_travel'}, inplace=True)

#https://github.com/L0rd-Z-19 (Me)
clean_london = pd.read_csv('Resources/london_17-18.csv', encoding = "ISO-8859-1")
clean_london = clean_london.drop(columns=['quarter','market','area','Visits (000s)','Spend (£m)','Nights (000s)','sample','year'])
clean_london.rename(columns={'dur_stay':'trip_length','purpose':'reason_for_travel','mode':'travel_type'},inplace=True)

#Merge the first two tables into one table
tables = [travel_data1819, london1_df]
combined_travel = pd.concat(tables, axis=0, join='outer', ignore_index=True,copy=True)

#SQLAlchemy to set up an SQL DataBase connection to PSQL
engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{user}:{pas}@127.0.0.1:5432/london_trips')

#Send the tables that were created in pandas to an SQL DB
combined_travel.to_sql("local_london",
                        engine,
                        if_exists="replace",
                        chunksize=500,
                        dtype={"Travel_ID": Integer,
                                "Travel_Type": String,
                                "Trip_Length_Days": Float,
                                "Reason_For_Travel":  Text})

clean_london.to_sql("international_london",
                        engine,
                        if_exists="replace",
                        chunksize=500,
                        dtype={"Travel_ID": Integer,
                                "Trip_Length_Days": Text,
                                "Travel_Type": String,
                                "Reason_For_Travel": Text})
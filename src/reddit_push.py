import mysql.connector
import pandas as pd
import os

host = "feenix-mariadb.swin.edu.au"
user = "s104489467"
password = "101100"
database = "s104489467_db"
def push_data(path):
    try:
        connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        cursor = connection.cursor()
    except:
        print("Error in connection")
    df = pd.read_csv(path)
    solar_power = "solarpower"
    wind_power = "windpower"
    nuclear_power = "nuclearpower"
    hydro_power = "hydro"
    subsidies = "subsidies"
    FMAA = "FMAA"
    HHS = "HHS"
    RET = "RET"
    SSP = "SSP"
    country = "Australia"
    type_list = [solar_power,wind_power,nuclear_power,hydro_power,subsidies,FMAA,HHS,RET,SSP]
    for i in type_list:
        if i in path:
            type_value = i
            break
    for i in range(len(df)):
        comment = df.iloc[i,0]
        sql_query = "INSERT INTO energy_data (country, type, comment) VALUES (%s, %s, %s)"
        cursor.execute(sql_query, (country, type_value, comment))
        connection.commit()
    cursor.close()
    connection.close()
directory =r"C:\Users\HP Envy\OneDrive - Swinburne University\Data Science\Innovation project\craw_data\Dat\data"
path_names = os.listdir(directory)
for path in path_names:
    path = directory + "\\" + path
    push_data(path)
                

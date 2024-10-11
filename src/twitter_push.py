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
    austrlia = "Australia"  
    china = "China"
    japan = "Japan"
    country_list = [austrlia,china,japan]
    for i in country_list:
        if i in path:
            country = i
            break
    for i in range(len(df)):
        type = df['Category'][i]
        comment = df["Post Body"][i]
        sql_query = "INSERT INTO energy_data (country, type, comment) VALUES (%s, %s, %s)"
        cursor.execute(sql_query,(country,type,comment))
        connection.commit()
    cursor.close()
    connection.close()
    print("End Push a File")
directory = r"C:\Users\HP Envy\OneDrive - Swinburne University\Data Science\Innovation project\craw_data\Jun\data"
path_names = os.listdir(directory)
for path in path_names:
    print("Start a New File")
    path = directory + "\\" + path
    push_data(path)
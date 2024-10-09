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
    if path == r"C:\Users\HP Envy\OneDrive - Swinburne University\Data Science\Innovation project\craw_data\Dat":
        path = path + "\\" + "output_sentiment.csv"
        df = pd.read_csv(path)
        for i in range(len(df)):
            country = df['country'][i]
            energy_source = df['type'][i]
            comment = df["comment"][i]
            sentiment_score = df['rescaled_compound'][i]
            sql_query = "INSERT INTO public_sentiment (country, energy_source, comment, sentiment_score) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql_query,(country,energy_source,comment,sentiment_score))
            connection.commit()
    else:
        directory = path
        path_names = os.listdir(directory)
        australia = "Australia"
        china = "China"
        japan = "Japan"
        country_list = [australia,china,japan]
        for path in path_names:
            path = directory + "\\" + path
            for i in country_list:
                if i in path:
                    country = i
                    break
            df = pd.read_csv(path)
            for i in range(len(df)):
                energy_source = df['Category'][i]
                comment = df["Post Body"][i]
                sentiment_score = df['bert_sentiment'][i]
                sql_query = "INSERT INTO public_sentiment (country, energy_source, comment, sentiment_score) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql_query,(country,energy_source,comment,sentiment_score))
                connection.commit()
    cursor.close()
    connection.close()
    print("End Push a File")
path = input("Enter the path of the file: ")
push_data(path)
    
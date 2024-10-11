import mysql.connector
import pandas as pd
import os
from openai import OpenAI
import re
host = "feenix-mariadb.swin.edu.au"
user = "s104489467"
password = "101100"
database = "s104489467_db"
client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")

def push_data():
    paths = [r'/Users/trangnguyenha/Design-Innovation-Project/craw_data/Dat',r'/Users/trangnguyenha/Design-Innovation-Project/craw_data/Jun/data']
    df= pd.DataFrame(columns=['country','energy_source','comment','sentiment_score'])
    for path in paths:
        # try:
        #     print("go try")
        #     connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        #     cursor = connection.cursor()
        # except:
        #     print("Error in connection")
        if path == r"/Users/trangnguyenha/Design-Innovation-Project/craw_data/Dat":
            print("go if")
            path = path + "/" + "output_sentiment.csv"
            df2 = pd.read_csv(path)
            for i in range(len(df2)):
                country = df2['country'][i]
                energy_source = df2['type'][i]
                comment = df2["comment"][i]
                sentiment_score = df2['rescaled_compound'][i]
                df1 = pd.DataFrame({'country':[country],'energy_source':[energy_source],'comment':[comment],'sentiment_score':[sentiment_score]})
                df = pd.concat([df,df1],ignore_index=True)
                # sql_query = "INSERT INTO public_sentiment (country, energy_source, comment, sentiment_score) VALUES (%s, %s, %s, %s)"
                # cursor.execute(sql_query,(country,energy_source,comment,sentiment_score))
                # connection.commit()
        else:
            directory = path
            path_names = os.listdir(directory)
            australia = "Australia"
            china = "China"
            japan = "Japan"
            country_list = [australia,china,japan]
            for path in path_names:
                path = directory + "/" + path
                for i in country_list:
                    if i in path:
                        country = i
                        break
                df2 = pd.read_csv(path)
                for i in range(len(df2)):
                    energy_source = df2['Category'][i]
                    comment = df2["Post Body"][i]
                    sentiment_score = str(df2['bert_sentiment'][i])
                    df1 = pd.DataFrame({'country':[country],'energy_source':[energy_source],'comment':[comment],'sentiment_score':[sentiment_score]})
                    df = pd.concat([df,df1],ignore_index=True)
                    # sql_query = "INSERT INTO public_sentiment (country, energy_source, comment, sentiment_score) VALUES (%s, %s, %s, %s)"
                    # cursor.execute(sql_query,(country,energy_source,comment,sentiment_score))
                    # connection.commit()
        # cursor.close()
        # connection.close()
        print("End Push a File")
    return df

# path = input("Enter the path of the file: ")
def add_keywords(keyword):
    completion = client.chat.completions.create(
    model="mlx-community/Llama-3.2-3B-Instruct-4bit",
    messages=[
      {"role": "system", "content": "Assign these types of keyword for each comment in these scope: Policy, Jobs, Economy, Tax, Investment, Technology, Environment, Cost, Infrastructure. One comment can have more than one keywords just give me keywords  and do not explain"},
      {"role": "user", "content": keyword}
    ],
    temperature=0.7,
  )
    pattern = r"content\s*=\s*'([^']*)'"
    try:
        keyword = re.findall(pattern, str(completion))[0]
    except:
        keyword = "None"
    return keyword
def main():
    df = push_data()
    connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
    cursor = connection.cursor()
    for i in range(len(df)):
        country = df['country'][i]
        energy_source = df['energy_source'][i]
        comment = df['comment'][i]
        keyword = add_keywords(comment)
        sentiment_score = str(df['sentiment_score'][i])
        sql_query = "INSERT INTO public_sentiment (country, energy_source, comment, key_word, sentiment_score) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(sql_query,(country,energy_source,comment,keyword,sentiment_score))
        connection.commit()
    cursor.close()
    connection.close()      
main()

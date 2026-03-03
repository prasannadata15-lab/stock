import json
import datetime as dt
import requests
import pandas as pd
pd.set_option('display.max_columns', None)
import boto3
pd.set_option('display.max_columns', None)
github_api_url = "https://api.github.com/repos/squareshift/stock_analysis/contents/"
response = requests.get(github_api_url)
# print(response)
# print(response.status_code)
b = response.json()
a = response.text
# print(a)
# print(b)

csv_files = [file['download_url'] for file in b if file['name'].endswith('.csv')]
# a=csv_files[0]
csv_file = csv_files.pop()
d = pd.read_csv(csv_file)
dataframes=[]
file_names=[]
for url in csv_files:
    file_name = url.split("/")[-1].replace(".csv", "")
    df = pd.read_csv(url)
    # print(df)
    df['Symbol'] = file_name
    # print(len(df['Symbol']))
    dataframes.append(df)
    file_names.append(file_name)
    # print(type(dataframes))

combined_df = pd.concat(dataframes, ignore_index=True)
o_df = pd.merge(combined_df,d,on='Symbol',how='left')
result = o_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
# print(result)
o_df["timestamp"] = pd.to_datetime(o_df["timestamp"])
# print(o_df["timestamp"])
print(o_df.dtypes)
# filtered_df = o_df[(o_df['timestamp'] >= "2021-01-01") & (o_df['t imestamp'] <= "2021-05-26")]
# result_time = filtered_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
# list_sector = ["TECHNOLOGY","FINANCE"]
# result_time = result_time[result_time["Sector"].isin(list_sector)].reset_index(drop=True)
# print(result_time)
# path = r"stock_data.csv"
# result_time.to_csv(path, header=True)
# print('data saved sucessfully')

#
# # def lambda_handler(event, context):
# #     current_datetime = dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
# #     csv_data = result_time.to_csv(index=False)
# #     bucket_name = 'stock-dev-54'
# #     file_name = f'stock_data_{current_datetime}'
# #     s3 = boto3.client('s3')
# #     s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_data.encode('utf-8'))
# #     # print("Current time is ", dt)
# #     # TODO implement
# #     return {
# #         'statusCode': 200,
# #         'body': json.dumps('Hello from Lambda!')
# #     }
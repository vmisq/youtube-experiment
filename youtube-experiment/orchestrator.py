import mysql.connector
import os
from dotenv import load_dotenv
import requests

load_dotenv()

connection = mysql.connector.connect(
    host=os.environ['MYSQL_HOST'],
    user=os.environ['MYSQL_USER'],
    password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE']
)

cursor = connection.cursor()

query = "SELECT url FROM url_for_webpagecrawler"
cursor.execute(query)

res = []
results = cursor.fetchall()
for result in results:
    res.append(result[0])

cursor.close()
connection.close()

for i in res:
    try:
        requests.get(i)
        print(i)
    except Exception as e:
        print(e)
        print(f"Error: {i}")
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']
DATA_FOLDER = 'data'
FUNCTION_REGION = 'LOCAL'

WEB_PAGE_URL = 'https://youtube.com'

def get_webpages():
    file_date_func = lambda file_path: datetime.utcfromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
    for file in os.listdir(DATA_FOLDER):
        if 'html' not in file:
            continue
        with open(os.path.join(DATA_FOLDER, file)) as f:
            webpage = f.read()
        yield webpage, file_date_func(os.path.join(DATA_FOLDER, file)), file

def save_to_mysql(url, html_content, ip_address_of_request, country_of_request, timestamp_of_request):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = connection.cursor()

        insert_query = """
            INSERT INTO webpagecrawler (url, html_content, ip_address_of_request, country_of_request, timestamp_of_request)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (url, html_content, ip_address_of_request, country_of_request, timestamp_of_request))
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def main():
    my_ip = ''
    my_country = 'LOCAL'

    for webpage, timestamp, file in get_webpages():
        save_to_mysql(
            url=WEB_PAGE_URL,
            html_content=webpage,
            ip_address_of_request=my_ip,
            country_of_request=my_country + '-' + FUNCTION_REGION,
            timestamp_of_request=timestamp
        )
        os.rename(os.path.join(DATA_FOLDER, file), os.path.join(DATA_FOLDER, 'moved', file+datetime.now().strftime("%Y%m%d%H%M%S")))

if __name__=='__main__':
    main()
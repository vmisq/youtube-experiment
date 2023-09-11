import requests
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']

WEB_PAGE_URL = 'https://youtube.com'


def get_my_ip():
    response = requests.get('https://ipinfo.io/ip')
    if response.status_code == 200:
        return response.text
    else:
        return ''

def get_ip_details(ip: str):
    if ip=='':
        return dict()
    
    try: # using iplocation.net
        url = f'https://api.iplocation.net/?ip={ip}'
        response = requests.get(url)
        data_dict = response.json()
        return data_dict, data_dict.get('country_code2', '')
    except Exception as e: # using who.is
        print(e)
        url = f'https://who.is/whois-ip/ip-address/{ip}'
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            content = set(soup.find('pre').text.split('\n'))
            content.remove('')

            data_dict = dict()
            for item in content:
                try:
                    key, value = item.split(':', 1)
                    data_dict[key.strip()] = value.strip()
                except Exception as e:
                    print(f'Failed to parse this entry: {item}.')
                    print(e)

            return data_dict, data_dict.get('Country', '')
        else:
            return dict(), ''

def get_webpage(url: str = WEB_PAGE_URL):
    timestamp = datetime.now()
    
    response = requests.get(url)
    if response.status_code == 200:
        webpage = response.text
    
        return webpage, timestamp
    else:
        return 'Failed to get webpage', timestamp

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
    my_ip = get_my_ip()
    my_ip_details = get_ip_details(my_ip)
    my_country = my_ip_details[1]
    webpage, timestamp = get_webpage()

    save_to_mysql(
        url=WEB_PAGE_URL,
        html_content=webpage,
        ip_address_of_request=my_ip,
        country_of_request=my_country,
        timestamp_of_request=timestamp
    )

if __name__=='__main__':
    main()
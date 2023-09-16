from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
import os
from dotenv import load_dotenv
from processor import get_yt_suggestions_from_soup, find_videos, extract_info, save_data

load_dotenv()

MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']
RETRY_IDS = os.environ.get('RETRY_IDS', 'ALL')

def get_webpages():
    connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
    
    cursor = connection.cursor()

    query = f"""
        SELECT id, html_content
        FROM webpagecrawler
        {f"WHERE id IN (SELECT source_id FROM need_manual_retry WHERE fixed='N')"
         if RETRY_IDS=='ALL' else
         'WHERE id IN ({RETRY_IDS})'}
    """

    cursor.execute(query)

    res = []
    results = cursor.fetchall()
    for result in results:
        res.append(result)

    cursor.close()
    cursor.close()

    return res

def update_manual_retry(source_id):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = connection.cursor()

        update_query = f"""
            UPDATE need_manual_retry
            SET fixed='Y'
            WHERE source_id={source_id}
        """

        cursor.execute(update_query)
        connection.commit()

        if connection.is_connected():
            cursor.close()
            connection.close()

    except mysql.connector.Error as err:
        if connection.is_connected():
            cursor.close()
            connection.close()
        raise err
        

def main():
    try:
        webpages = get_webpages()
        number_of_webpages = len(webpages)
        if number_of_webpages < 1:
            raise Exception('No more webpages to process')
    except Exception as e:
        return None
    
    for source_id, webpage in webpages:
        try:
            soup = BeautifulSoup(webpage, "html.parser")
            yt_suggestions = get_yt_suggestions_from_soup(soup)
            videos = find_videos(yt_suggestions)
            videos_info = extract_info(videos)
            data_list = []
            for video_info in videos_info:
                data_list.append(video_info)
            save_data(source_id, data_list)
            update_manual_retry(source_id)
        except Exception as e:
            print(e)
            print(f'Failed retry on webpagecrawler for id {source_id}')


if __name__=='__main__':
    main()

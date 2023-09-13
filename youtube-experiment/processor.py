from bs4 import BeautifulSoup
from datetime import datetime
import json
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']
MYSQL_TABLE = os.environ['MYSQL_TABLE']
BATCH_SIZE = os.environ['BATCH_SIZE']
TIME_OUT = os.environ['TIME_OUT']

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
        FROM {MYSQL_TABLE} s
        WHERE NOT EXISTS (
            SELECT 1 a FROM processed_webpage pw WHERE pw.source_id = s.id AND pw.source_table = '{MYSQL_TABLE}'
            UNION
            SELECT 1 a FROM need_manual_retry mr WHERE mr.source_id = s.id AND mr.source_table = '{MYSQL_TABLE}'
        )
        LIMIT {BATCH_SIZE}
    """
    cursor.execute(query)

    res = []
    results = cursor.fetchall()
    for result in results:
        res.append(result)

    cursor.close()
    cursor.close()

    return res

def get_yt_suggestions_from_soup(soup):
    for script in soup.find_all("script"):
        if script.text.startswith('var ytInitialData'):
            content = script.text
            yt_initial_data = content[content.find('{'):content.rfind('}')+1]
            yt_initial_data = json.loads(yt_initial_data)
            return yt_initial_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][0]['tabRenderer']['content']['richGridRenderer']['contents']

def find_videos(yt_suggestions):
    for yt_suggestion in yt_suggestions:
        if list(yt_suggestion.keys())[0] == 'richItemRenderer':
            yield ('No Shelf', 'video', yt_suggestion['richItemRenderer']['content']['videoRenderer'])
        elif list(yt_suggestion.keys())[0] == 'richSectionRenderer':
            yt_suggestion_shelf = yt_suggestion['richSectionRenderer']['content']['richShelfRenderer']
            yt_suggestion_shelf_type = yt_suggestion_shelf['title']['runs'][0]['text']
            for yt_suggestion_in_shelf in yt_suggestion_shelf['contents']:
                try:
                    yield (yt_suggestion_shelf_type, 'video', yt_suggestion_in_shelf['richItemRenderer']['content']['videoRenderer'])
                except KeyError:
                    yield (yt_suggestion_shelf_type, 'reel', yt_suggestion_in_shelf['richItemRenderer']['content']['reelItemRenderer'])

def extract_info(videos):
    for video_gross_position, yt_suggestion in enumerate(videos):
        video_id = yt_suggestion[2]['videoId']
        video_info = str(yt_suggestion[2])
        video_shelf = yt_suggestion[0]
        video_type = yt_suggestion[1]
        if video_type=='video':
            channel_id = yt_suggestion[2]['longBylineText']['runs'][0]['navigationEndpoint']['browseEndpoint']['browseId']
        else:
            channel_id = ''
        
        yield video_id, channel_id, video_shelf, video_type, video_gross_position, video_info

def save_data(source_id, videos_info):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = connection.cursor()

        insert_query = """
            INSERT INTO processed_webpage (source_id, source_table, video_id, channel_id, video_shelf, video_type, video_gross_position, video_info)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for each_video in videos_info:
            video_id, channel_id, video_shelf, video_type, video_gross_position, video_info = each_video      
            cursor.execute(insert_query, (source_id, MYSQL_TABLE, video_id, channel_id, video_shelf, video_type, video_gross_position, video_info))
            connection.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def mark_for_manual_retry(source_id, error_msg):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = connection.cursor()

        insert_query = """
            INSERT INTO need_manual_retry (source_id, source_table, error_msg)
            VALUES (%s, %s, %s)
        """
            
        cursor.execute(insert_query, (source_id, MYSQL_TABLE, error_msg))
        connection.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def main():
    webpages = get_webpages()
    if len(webpages) < 1:
        raise Exception('No more webpages to process')
    for source_id, webpage in webpages:
        try:
            soup = BeautifulSoup(webpage, "html.parser")
            yt_suggestions = get_yt_suggestions_from_soup(soup)
            videos = find_videos(yt_suggestions)
            videos_info = extract_info(videos)
            save_data(source_id, videos_info)
        except Exception as e:
            mark_for_manual_retry(source_id, str(e))

if __name__=='__main__':
    start = datetime.now()
    while (datetime.now() - start).seconds <= int(TIME_OUT):
        main()
    raise Exception('TIME OUT')

from bs4 import BeautifulSoup
from datetime import datetime
import time
import json
import mysql.connector
import os
from dotenv import load_dotenv
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)

file_handler = logging.FileHandler("processor.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

load_dotenv()

MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']
MYSQL_TABLE = os.environ['MYSQL_TABLE']
BATCH_SIZE = os.environ['BATCH_SIZE']
TIME_OUT = os.environ['TIME_OUT']

logger.debug("Env vars OK")

def get_webpages():
    connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
    
    cursor = connection.cursor()
    query = f"""
        SELECT id
        FROM {MYSQL_TABLE} s
        WHERE NOT EXISTS (
            SELECT * FROM processed_webpage pw WHERE pw.source_id = s.id AND pw.source_table = '{MYSQL_TABLE}'
        )
        LIMIT {BATCH_SIZE}
    """
    logger.info(query)
    cursor.execute(query)

    ids = []
    results = cursor.fetchall()
    for result in results:
        ids.append(result)
    
    query = f"""
        SELECT id, html_content 
        FROM webpagecrawler_gcp WHERE id IN ({','.join([str(id[0]) for id in ids])})
    """

    logger.info(query)
    cursor.execute(query)

    res = []
    results = cursor.fetchall()
    for result in results:
        res.append(result)

    cursor.close()
    connection.close()
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
        if list(yt_suggestion.keys())[0]=='continuationItemRenderer':
            continue
        try:
            if list(yt_suggestion['richItemRenderer']['content'].keys())[0]=='adSlotRenderer':
                continue
            yield ('No Shelf', 'video', yt_suggestion['richItemRenderer']['content']['videoRenderer'])
        except KeyError:
            try:
                if list(yt_suggestion['richSectionRenderer']['content'].keys())[0] in ['counterfactualRenderer', 'primetimePromoRenderer', 'statementBannerRenderer']:
                    continue
                yt_suggestion_shelf = yt_suggestion['richSectionRenderer']['content']['richShelfRenderer']
                yt_suggestion_shelf_type = yt_suggestion_shelf['title']['runs'][0]['text']
                for yt_suggestion_in_shelf in yt_suggestion_shelf['contents']:
                    try:
                        yield (yt_suggestion_shelf_type, 'video', yt_suggestion_in_shelf['richItemRenderer']['content']['videoRenderer'])
                    except KeyError:
                        try:
                            yield (yt_suggestion_shelf_type, 'reel', yt_suggestion_in_shelf['richItemRenderer']['content']['reelItemRenderer'])
                        except KeyError:
                            yield (yt_suggestion_shelf_type, 'movie', yt_suggestion_in_shelf['richItemRenderer']['content']['movieRenderer'])
            except KeyError:
                yt_suggestion_shelf = yt_suggestion['richSectionRenderer']['content']['brandVideoShelfRenderer']
                yt_suggestion_shelf_type = yt_suggestion_shelf['title']['runs'][0]['text']
                for yt_suggestion_in_shelf in yt_suggestion_shelf['content']:
                    try:
                        yield (yt_suggestion_shelf_type, 'video', yt_suggestion_in_shelf['videoRenderer'])
                    except KeyError:
                        yield (yt_suggestion_shelf_type, 'reel', yt_suggestion_in_shelf['reelItemRenderer'])


def extract_info(videos):
    for video_gross_position, yt_suggestion in enumerate(videos):
        video_id = yt_suggestion[2]['videoId']
        video_info = str(yt_suggestion[2])
        video_shelf = yt_suggestion[0]
        video_type = yt_suggestion[1]
        try:
            channel_id = yt_suggestion[2]['longBylineText']['runs'][0]['navigationEndpoint']['browseEndpoint']['browseId']
        except KeyError:
            channel_id = ''
        yield (video_id, channel_id, video_shelf, video_type, video_gross_position, video_info)

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
    try:
        webpages = get_webpages()
        number_of_webpages = len(webpages)
    except Exception as e:
        logger.error('Error on getting content from database')
        logger.error(MYSQL_TABLE + ' - ' + str(e))
        logger.info('Try again in 30 seconds')
        time.sleep(30)
        return None
    
    if number_of_webpages < 1:
        raise Exception('No more webpages to process')
    logger.info(f'Successfully pulled {number_of_webpages} webpages')
    
    for source_id, webpage in webpages:
        try:
            soup = BeautifulSoup(webpage, "html.parser")
            logger.debug('Soup created')
            
            yt_suggestions = get_yt_suggestions_from_soup(soup)
            logger.debug('Suggestions extracted from soup')

            videos = find_videos(yt_suggestions)
            logger.debug('Videos extracted from suggestions')

            videos_info = extract_info(videos)
            logger.debug('Info extracted from videos')

            save_data(source_id, videos_info)
            logger.info('Info uploaded to db')
        except Exception as e:
            logger.error(f'Error in source_id {source_id}')
            logger.error(MYSQL_TABLE + ' - ' + str(e))
            try:
                mark_for_manual_retry(source_id, str(e))
            except Exception as e:
                logger.error('Could no mark for retry')
                logger.error(MYSQL_TABLE + ' - ' + str(e))


if __name__=='__main__':
    start = datetime.now()
    while (datetime.now() - start).seconds <= int(TIME_OUT):
        main()
    logger.info(MYSQL_TABLE + ' - TIME OUT')
    raise Exception('TIME OUT')

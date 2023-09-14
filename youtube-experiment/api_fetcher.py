import os
import mysql.connector
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from dotenv import load_dotenv
import json

load_dotenv()

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
api_service_name = "youtube"
api_version = "v3"
GOOGLE_API_KEY = os.environ['GOOGLE_API_KEY']
BATCH_SIZE = 50

MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']

def execute_statement(query, kind, data=None):
    connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
    cursor = connection.cursor()
    if isinstance(data, list):
        if isinstance(data[0], list):
            for dt in data:
                cursor.execute(query, dt)
        else:
            cursor.execute(query, dt)
    else:
        cursor.execute(query)
    if kind=='SELECT':
        res = []
        results = cursor.fetchall()
        for result in results:
            res.append(result)
    else:
        connection.commit()
        res = None
    cursor.close()
    connection.close()
    return res

def split_in_chunks(src_list):
    result = []
    for i in range(0, len(src_list), BATCH_SIZE):
        chunk = src_list[i:i + BATCH_SIZE]
        result.append(chunk)
    return result

def manage_videos():
    query = """
        INSERT INTO video_id_list (id)
        SELECT DISTINCT video_id AS id
        FROM processed_webpage
        WHERE video_id NOT IN (SELECT id FROM video_id_list)
    """
    execute_statement(query, 'INSERT')

def manage_channels():
    query = """
        INSERT INTO channel_id_list (id)
        SELECT DISTINCT channel_id AS id
        FROM processed_webpage
        WHERE channel_id NOT IN (SELECT id FROM channel_id_list)
    """
    execute_statement(query, 'INSERT')

    query = """
        INSERT INTO channel_id_list (id)
        SELECT DISTINCT channel_id AS id
        FROM video_tracker
        WHERE channel_id NOT IN (SELECT id FROM channel_id_list)
    """
    execute_statement(query, 'INSERT')


def get_videos():
    query = """
        SELECT id FROM video_id_list WHERE id <> ''
    """
    return execute_statement(query, 'SELECT')

def get_channels():
    query = """
        SELECT id FROM channel_id_list WHERE id <> ''
    """
    return execute_statement(query, 'SELECT')

def get_info(id, kind):
    if isinstance(id, list):
        id = ','.join([i[0] for i in id])
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=GOOGLE_API_KEY)
    if kind=='video':
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=id
        )
    elif kind=='channel':
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=id
        )
    response = request.execute()
    return response

def get_results(response):
    for i in response['items']:
        etag = i['etag']
        id =  i['id']
        snippet = i['snippet']
        details = i['contentDetails']
        statistics = i['statistics']
        yield (etag, id, snippet, details, statistics)

def prep_batch(data, kind):
    etag, id, snippet, details, statistics = data
    if kind=='video':
        channel_id = snippet.get('channelId', '')
        return [etag, id, channel_id, str(snippet), str(details), str(statistics)]
    elif kind=='channel':
        return [etag, id, str(snippet), str(details), str(statistics)]

def save_to_mysql(data, kind):
    if kind=='video':
        query = """
            INSERT INTO video_tracker (etag, video_id, channel_id, snippet, details, statistics)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
    elif kind=='channel':
        query = """
            INSERT INTO channel_tracker (etag, channel_id, snippet, details, statistics)
            VALUES (%s, %s, %s, %s, %s)
        """
    execute_statement(query, 'INSERT', data)

def process_batches(content_list_of_lists, kind):
    for content_list in content_list_of_lists:
        content_info = get_info(content_list, kind)
        batch_data = []
        for result in get_results(content_info):
            batch_data.append(prep_batch(result, kind))
        save_to_mysql(batch_data, kind)

def main():
    manage_videos()
    video_list = get_videos()
    video_lists = split_in_chunks(video_list)
    process_batches(video_lists, 'video')

    manage_channels()
    channel_list = get_channels()
    channel_lists = split_in_chunks(channel_list)
    process_batches(channel_lists, 'channel')

if __name__ == '__main__':
    main()
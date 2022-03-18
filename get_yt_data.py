from requests import get
from collections import namedtuple
import csv
import time


timemarks = ['2021-11-11T00:00:00Z', '2021-10-01T00:00:00Z',
            '2021-09-01T00:00:00Z','2021-08-01T00:00:00Z', '2021-07-01T00:00:00Z',
            '2021-06-01T00:00:00Z', '2021-05-01T00:00:00Z']

api_key_idx = 0

def getVideoIdList(*, channel_id, api_key, after, before, next_page_token):
    try:
        res = get(f'https://www.googleapis.com/youtube/v3/search',
                    params={'channelId':channel_id, 
                            'part': 'snippet', 
                            'fields': 'nextPageToken,pageInfo,items(id(videoId),snippet(publishedAt,title))',
                            'maxResults': 50, 
                            'order': 'date', 
                            'key': api_key,
                            'publishedAfter': after,
                            'publishedBefore': before,
                            'pageToken': next_page_token}).json()
    except Exception as e:
        print(e)
        return None

    return res


def getVideoStats(*, video_id, api_key):
    try:
        res = get('https://www.googleapis.com/youtube/v3/videos',
                params={'part': 'id,statistics, snippet',
                        'maxResult': 50,
                        'key': api_key,
                        'id': video_id,
                        'fields': 'items(id,snippet,statistics)'}).json()

    except Exception as e:
        print(e)
        return None

    return res


def writeData(filename, data_list):
    with open(filename, 'a', newline='') as f:
        dumper = csv.writer(f)
        dumper.writerows(data_list)


for (filename, channel_id) in filename.items():
    for i in range(len(timemarks)-1):
        
        timemark_after = timemarks[i+1]
        timemark_before = timemarks[i]

        print(f'channel: {channel_id}')
        print(f'[{timemark_after} - {timemark_before}]')

        api_key_idx = (api_key_idx + 1) % len(api_keys)
        api_key = api_keys[api_key_idx]

        video_counter = 0
        total_video = 0

        video_data = []
        next_page_token = ''

        while True:

            data = getVideoIdList(channel_id=channel_id, api_key=api_key, after=timemark_after, before=timemark_before, next_page_token=next_page_token)

            if not data:
                print('Network failed')

            else:
                if 'error' in data:
                    if data['error']['code'] == 403:
                        print(f'api key hết quota')
                        api_key_idx = (api_key_idx + 1) % len(api_keys)
                        api_key = api_keys[api_key_idx]

                    if data['error']['code'] == 400:
                        print(f'!!! [Youtube] Invalid params: {channel_id}')
                        print(data['error']['message'])

                else:
                    
                    video_counter += data['pageInfo']['resultsPerPage']
                    total_video = data['pageInfo']['totalResults']

                    print(f'{video_counter}/{total_video}')

                    videos = data['items']
                    video_id_list = [] 

                    for v in videos:
                        try:
                            video_id_list.append(v['id']['videoId'])
                        except:
                            pass

                    video_id_str = ','.join(video_id_list)

                    video_stat_data = getVideoStats(video_id=video_id_str, api_key=api_key)

                    if not video_stat_data:
                        print('Network failed')

                    else:
                        if 'error' in video_stat_data:
                            if video_stat_data['error']['code'] == 403:
                                print(f'api key hết quota')
                                api_key_idx = (api_key_idx + 1) % len(api_keys)
                                api_key = api_keys[api_key_idx]

                            if video_stat_data['error']['code'] == 400:
                                print(video_stat_data['error']['message'])

                        else:
                            stat_data = video_stat_data['items']

                            for video in stat_data:
                                try:
                                    video_info = [video['id'], video['snippet']['title'], video['snippet']['publishedAt'], 
                                                video['statistics']['viewCount'], video['statistics']['likeCount'],
                                                video['statistics']['dislikeCount'], video['statistics']['commentCount']]

                                    video_data.append(video_info)
                                except Exception as e:
                                    pass

                    if 'nextPageToken' in data:
                        next_page_token = data['nextPageToken']

                        time.sleep(3)
                    else:
                        writeData(f'youtube_videos/{filename}', video_data)
                        break
            

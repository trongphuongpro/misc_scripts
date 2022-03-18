import argparse

from typing import Dict, List, Union
from requests import get
from datetime import datetime
from datetime import timedelta
import csv
from signal import signal, SIGINT
from sys import exit
import logging


def interrupt_callback(sig, frame):
    print("Stop program")
    exit(0)


signal(SIGINT, interrupt_callback)


class Report:
    def __init__(self, *, target_name, platform, post_time, post_id, content=None):
        self.target_name = target_name
        self.post_time = post_time
        self.post_url = TEMPLATES[platform] + post_id
        self.content = content


class TokenException(Exception):
    pass

class UnknownException(Exception):
    pass

class InvalidParamsException(Exception):
    pass

class EmptyTokenListError(Exception):
    pass

cli = argparse.ArgumentParser()
cli.add_argument('--begin_time', required=True, help='Mốc thời gian bắt đầu, định dạng "hh:mm DD/MM/YY"')
cli.add_argument('--end_time', required=True, help='Mốc thời gian kết thúc, định dạng "hh:mm DD/MM/YY"')
cli.add_argument('--tokens', required=True, help='File chứa uid và token')
cli.add_argument('--targets', required=True, help='File chứa uid và tên mục tiêu')
cli.add_argument('--keywords', default=None, required=False, help='File chứa keywords')
cli.add_argument('--output', required=True, help='File chứa output')
cli.add_argument('--log', default='log.txt', help='File chứa uid của access token đã hết hạn')
args = vars(cli.parse_args())


def log(message):
    logging.info(message)


def logExpiredToken(token_list):
    with open(args['log'], 'w') as file:
        file.write('\n'.join(token_list))


def writePostToFile(filename, content):
    with open(filename, 'a+') as file:
        file.write(content)


def getTokens(token_file) -> List[Dict[str,str]]:
    """Get token list from csv file.

        params:
            token_file: string - filename
        return:
            list of tokens
    """


    with open(token_file, 'r', encoding='utf-8') as file:
        tokens = list(csv.DictReader(file, ['uid', 'token']))

    return tokens


def getTargets(target_file) -> List[Dict[str,str]]:
    """Get target list from csv file.

        params:
            target_file: string - filename
        return:
            list of targets
    """


    with open(target_file, 'r', encoding='utf-8') as file:
        targets = list(csv.DictReader(file, ['uid', 'name']))
 
    return targets


def getKeywords(file):
    """Get keyword list from txt file.

        params:
            file: string - filename
        return:
            list of keywords
    """

    with open(file, 'r', encoding='utf-8') as file:
        keywords = [word.strip() for word in file.readlines()]
   
    return keywords


def checkContent(content: str, keywords: List[str]) -> bool:
    if not args['keywords']:
        return True

    data = content.lower().encode()

    res = []
    
    for word in keywords:
        if data.find(word.encode()) > -1:
            res.append(word)

    if res:
        return True
    return False


def sendMessage(report: Report):
    text = f'[{report.post_time}] [{report.target_name}] {report.post_url}\n'

    if report.content:
        content = report.content.replace("\n", " ")
        text += f'"{content}..."\n'

    print(text)

    with open(args['output'], 'a+') as file:
        file.write(text)


def sendNotification(title: str, content: List[Report] = None):
    print(title)

    with open(args['output'], 'a+') as file:
        file.write(title)

    if content:
        for message in content:
            sendMessage(message)

    print('='*50)
    with open(args['output'], 'a+') as file:
        file.write('='*50)


def getFeed(target, target_name, token):
    global token_index
    global access_token_list
    global expired_token_list
    global begin_time, end_time

    data = []
    next_page = ''

    try:
        while True:
            res = get(f'https://graph.facebook.com/{target}/feed', 
                        params={'access_token': token['token'], 
                                'fields': 'id,message,created_time',
                                'limit': 100, 
                                'after': next_page}).json()

            if 'error' in res:
                log(f"!!! Bot [{token['uid']}]: {res['error']}: {target}")

                if res['error']['code'] in [1, 104, 190, 368]:
                    # log(f"!!! Bot [{access_token_list[token_index]['uid']}]: Access token đã hết hạn")
                    access_token_list.pop(token_index)
                    expired_token_list.append(token['uid'])

                    raise TokenException(f"token của bot {token['uid']} đã hết hạn hoặc không hợp lệ.")


                if res['error']['code'] == 100 and res['error'].get('error_subcode', 0) == 33:
                    log(f'!!! [Facebook] Invalid params: {target}')
                    raise InvalidParamsException(f"{target} không tồn tại")

                raise UnknownException(f"Có lỗi unknown xảy ra khi theo dõi mục tiêu {target}")

            if not res['data']:
                return data

            for obj in res['data']:
                post_time = datetime.fromisoformat(obj['created_time'][0: len(obj['created_time']) - len('+0000')]) + timedelta(hours=7)

                if begin_time <= post_time <= end_time:
                    object_id = obj['id']
                    content = obj.get('message', '')

                    if checkContent(content, keywords):

                        if len(content) > 150:
                            content = content[:150]

                        data.append(Report(target_name=target_name,
                                            platform='facebook', 
                                            post_time=post_time, 
                                            post_id=object_id, 
                                            content=content))

                elif post_time < begin_time:
                    return data

            next_page = res['paging']['cursors']['after']

    except Exception as e:
        raise e


def trackFacebook(target, target_name) -> Union[List[Report], None]:
    global token_index
    global access_token_list
    global expired_token_list

    # if all tokens are expired
    # then return empty list to continue tracking another target
    if not access_token_list:
        log('Đã hết FB access token!')
        raise EmptyTokenListError('Đã hết token')

    # change token for each request
    if token_index >= len(access_token_list) - 1:
        token_index = 0
    else:
        token_index += 1

    try:
        feed = getFeed(target, target_name, access_token_list[token_index])
    except Exception as e:
        log(e)
        raise e

    return feed


def track():
    
    global from_sleep
    global urgent
    global access_token_list
    global expired_token_list
    global keywords

    print(f"[{datetime.now()}] Start tracking...")

    messages = []
    expired_token_list = []

    # get targets list from db or file
    try:
        targets = getTargets(args['targets'])
    except Exception as e:
        print(f"Không thể mở file {args['targets']}")
        return

    # get keywords from file
    if args['keywords']:
        try:
            keywords = getKeywords(args['keywords'])
        except Exception as e:
            print(f"Không thể mở file {args['keywords']}")
            return

    # get access token from managing server
    try:
        access_token_list = getTokens(args['tokens'])
    except Exception as e:
        print("Không thể lấy token")
        return    
        
    print(f'Access tokens: {len(access_token_list)}')

    
    for target in targets:
        uid = target['uid']
        name = target['name']
        data = []

        # tracking
        while True:
            try:
                data = trackFacebook(uid, name)
            except (TokenException, UnknownException):
                continue
            except InvalidParamsException:
                break
            except (ConnectionError, EmptyTokenListError):
                return
            else:
                break

        messages.extend(data)

    sendNotification(title=f"\nTừ {args['begin_time']} đến {args['end_time']} phát hiện {len(messages)} bài viết:\n",
                    content=messages)

    logExpiredToken(expired_token_list)

    print(f"[{datetime.now()}] Tracking done")


# global variables
TEMPLATES = {'facebook': 'https://facebook.com/'}

token_index = 0
access_token_list: List = []
keywords: List = []
expired_token_list: List = []


def run():
    global begin_time
    global end_time

    try:
        begin_time = datetime.strptime(args['begin_time'], '%H:%M %d/%m/%Y')

        if begin_time > datetime.now():
            print("begin_time > now")
            raise Exception("begin_time > now")

    except ValueError as e:
        print(e)
        print(f'{args["begin_time"]} không đúng định dạng, yêu cầu theo định dạng hh:mm dd/mm/yyyy')
        return

    try:
        end_time = datetime.strptime(args['end_time'], '%H:%M %d/%m/%Y')

        if end_time > datetime.now():
            end_time = datetime.now()

        if end_time <= begin_time:
            raise Exception("begin_time > end_time")

    except ValueError as e:
        print(e)
        print(f'{args["end_time"]} không đúng định dạng, yêu cầu theo định dạng hh:mm dd/mm/yyyy')
        return

    track()


run()

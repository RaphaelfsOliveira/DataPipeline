import requests
import os
import json
from datetime import datetime, timedelta

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def auth():
    return os.environ.get('BEARER_TOKEN')


def date_filter():
    date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    start_time = (datetime.now() - timedelta(days=5)).strftime(date_format)
    end_time = (datetime.now()).strftime(date_format)
    
    return f'start_time={start_time}&end_time={end_time}'


def tweet_fields():
    tweet = ','.join([
        'id',
        'author_id',
        'conversation_id',
        'created_at',
        'in_reply_to_user_id',
        'public_metrics',
        'text',
    ])
    
    return f'tweet.fields={tweet}'


def user_fields():
    user = ','.join([
        'id',
        'name',
        'username',
        'created_at',
    ])
    
    return f'expansions=author_id&user.fields={user}'


def create_url():
    query = 'AluraOnline'

    tweets = tweet_fields()
    users = user_fields()
    dates = date_filter()

    params = '&'.join([
        tweets,
        users,
        dates,
    ])
    
    url = 'https://api.twitter.com/2/tweets/search/recent?query={}&{}'.format(
        query, 
        params
    )
    return url


def create_headers(bearer_token):
    headers = {'Authorization': 'Bearer {}'.format(bearer_token)}
    return headers


def request_data(url, headers):
    response = requests.request('GET', url, headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    return response.json()


def paginate(url, headers, paginate_token=None):
    if paginate_token:
        full_url = f'{url}&next_token={paginate_token}'
    else:
        full_url = url

    data = request_data(full_url, headers)
    if data:
        yield data
    
    paginate_token = data.get('meta', {}).get('next_token')
    if paginate_token:
        yield from paginate(url, headers, paginate_token)


def main():
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)

    for data in paginate(url, headers):
        print(json.dumps(data, indent=4, sort_keys=True))


if __name__ == '__main__':
    main()
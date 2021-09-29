from airflow.hooks.http_hook import HttpHook
from datetime import datetime, timedelta
import requests
import json


class TwitterHook(HttpHook):


    def __init__(self, query, conn_id=None, start_time='', end_time='') -> None:
        self.query = query
        self.conn_id = conn_id or 'twitter_default'
        self.start_time = start_time
        self.end_time = end_time

        super().__init__(http_conn_id=self.conn_id)
    

    def date_filter(self):
        date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        start_time = (datetime.now() - timedelta(days=5)).strftime(date_format)
        end_time = (datetime.now()).strftime(date_format)
        
        return f'start_time={start_time}&end_time={end_time}'
    

    def tweet_fields(self):
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


    def user_fields(self):
        user = ','.join([
            'id',
            'name',
            'username',
            'created_at',
        ])
        
        return f'expansions=author_id&user.fields={user}'


    def create_url(self):
        query = self.query

        tweets = self.tweet_fields()
        users = self.user_fields()

        start_time = f'start_time={self.start_time}' if self.start_time else ''
        end_time = f'end_time={self.end_time}' if self.end_time else ''
        dates = '&'.join([start_time, end_time]) if all([start_time, end_time]) else ''

        params = [tweets, users]
        params.append(dates) if dates else None
        params = '&'.join(params)
        
        url = '{}/2/tweets/search/recent?query={}&{}'.format(
            self.base_url,
            query,
            params
        )

        return url


    def request_data(self, url, session):
        response = requests.Request('GET', url)
        prep = session.prepare_request(response)
        self.log.info(f'URL: {url}')

        return self.run_and_check(session, prep, {}).json()


    def paginate(self, url, session, paginate_token=None):
        if paginate_token:
            full_url = f'{url}&next_token={paginate_token}'
        else:
            full_url = url

        data = self.request_data(full_url, session)
        if data:
            yield data
    
        paginate_token = data.get('meta', {}).get('next_token')
        if paginate_token:
            yield from self.paginate(url, session, paginate_token)


    def run(self):
        session = self.get_conn()
        url = self.create_url()

        yield from self.paginate(url, session)


if __name__ == '__main__':
    for data in TwitterHook('AluraOnline').run():
        print(json.dumps(data, indent=4, sort_keys=True))

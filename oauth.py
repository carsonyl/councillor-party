import json
import os
import requests
import time
from datetime import datetime, timedelta
from requests import Session


TOKENS_DIR = 'auth'


def tokens_file_for_id(project_id):
    return os.path.join(TOKENS_DIR, project_id + '.token.json')


def load_client_credentials(cred_file=os.path.join(TOKENS_DIR, 'client_id.json')):
    # https://console.developers.google.com/apis/credentials
    if not os.path.isfile(cred_file):
        raise ValueError("{} is missing".format(cred_file))
    with open(cred_file) as f:
        return json.load(f)['installed']


def obtain_user_code(client_id):
    resp = requests.post('https://accounts.google.com/o/oauth2/device/code', params={
        'client_id': client_id,
        'scope': 'https://www.googleapis.com/auth/youtube https://www.googleapis.com/auth/youtube.upload'
    })
    resp.raise_for_status()
    return resp.json()


def get_channel_credentials(config_id):
    credentials_file = config_id + '.tokens.json'
    if os.path.isfile(credentials_file):
        with open(credentials_file) as inf:
            return json.load(inf)
    return None


def _get_oauth_token(client_id, client_secret, grant_type, extra=None):
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': grant_type,
    }
    if extra:
        params.update(extra)
    resp = requests.post('https://www.googleapis.com/oauth2/v4/token', params=params)
    return resp.json()


def poll_for_authorization(client_id, client_secret, user_code_response):
    poll_interval = user_code_response['interval']
    for i in range(0, user_code_response['expires_in'], poll_interval):
        time.sleep(poll_interval)
        resp = _get_oauth_token(client_id, client_secret, 'http://oauth.net/grant_type/device/1.0',
                                {'code': user_code_response['device_code']})
        error = resp.get('error')
        if error == 'authorization_pending':
            continue
        elif error:
            raise ValueError("Authorization failed: " + error)
        return resp
    raise ValueError("Timed out getting authorization")


class OAuth2Session(Session):
    def __init__(self, client, credentials):
        super(OAuth2Session, self).__init__()
        self._client_id = client['client_id']
        self._client_secret = client['client_secret']
        self._refresh_token = credentials['refresh_token']
        self._expires_at = datetime.now()

    def _get_access_token(self):
        resp = _get_oauth_token(self._client_id, self._client_secret,
                                'refresh_token', {'refresh_token': self._refresh_token})
        self.headers['Authorization'] = 'Bearer ' + resp['access_token']
        self._expires_at = datetime.now() + timedelta(seconds=resp['expires_in'])

    def request(self, method, url, **kwargs):
        if datetime.now() >= self._expires_at:
            self._get_access_token()
        return super().request(method, url, **kwargs)

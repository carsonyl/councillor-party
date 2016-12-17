"""
Script and functions for uploading videos onto YouTube.
"""
import argparse
import json
import os
import pytz
import requests
import yaml
import time
from datetime import datetime
from requests import HTTPError
from tqdm import tqdm

from config import get_config, get_tz
from oauth import load_client_credentials, obtain_user_code, poll_for_authorization, OAuth2Session


def parse_timestamp_naively(ts):
    """
    Parse a timestamp that ends in +00:00, because Python datetime can't do it.

    :param ts: ISO 8601 timestamp, ending in +00:00.
    :return: datetime in UTC.
    """
    ts = ts.replace('+00:00', 'Z')
    dt = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ')
    return dt.replace(tzinfo=pytz.utc)


def get_minutes_url(config, metadata):
    start_date = parse_timestamp_naively(metadata['start']).astimezone(get_tz(config))
    meeting_type = None
    for key, value in config.get('minutes_abbrs', {}).items():
        if key in metadata[config['minutes_abbrs_for']]:
            meeting_type = value
            break

    minutes_url = metadata.get('minutes_url')
    if minutes_url:
        requests.head(minutes_url).raise_for_status()
        return minutes_url
    elif config['id'] == 'vancouver':
        if not meeting_type:
            return 'N/A'
        if metadata['title'] == 'Inaugural Council Meeting':
            meeting_type = 'inau'
        mins = 'http://council.vancouver.ca/{dt:%Y%m%d}/{type}{dt:%Y%m%d}ag.htm'.format(
            type=meeting_type, dt=start_date)
        requests.head(mins).raise_for_status()
        return mins

    return 'N/A'


def build_youtube_video_resource(config, metadata, minutes_url):
    """
    Build a YouTube video resource as per https://developers.google.com/youtube/v3/docs/videos.

    :param dict config:
    :param dict metadata: Metadata for the video.
    :returns: dict representing the video resource, ready to serialize to JSON.
    """
    ytconfig = config['youtube']
    ts = parse_timestamp_naively(metadata['start'])

    title_no_date = metadata['title']
    if title_no_date[-4:].isdigit():
        title_no_date = title_no_date.split(' - ')[0]
    if title_no_date in ('Full Meeting', 'Entire Meeting', 'Whole Meeting') and config['id'] == 'vancouver':
        title_no_date = metadata['project_name']

    kwargs = {
        'clip_title': title_no_date,
        'clip_date': ts.astimezone(get_tz(config)),
        'minutes_url': minutes_url,
        'project_name': metadata.get('project_name', ''),
    }
    timecodes = "\n".join('{entry[time]} - {entry[title]}'.format(entry=entry) for entry in metadata['timecodes'])
    if timecodes:
        timecodes = "\n" + timecodes
    kwargs['timecodes'] = timecodes

    recordingDetails = {
        'recordingDate': ts.isoformat().replace('+00:00', '.0Z'),
    }
    location = ytconfig.get('location')
    if location:
        recordingDetails['location'] = {
            'latitude': location[0],
            'longitude': location[1],
            'altitude': 0.0,
        }
    locationDesc = ytconfig.get('location_desc')
    if locationDesc:
        recordingDetails['locationDescription'] = locationDesc

    description = ytconfig['desc'].format(**kwargs).rstrip()
    description += "\n\nThis is an automated re-upload."
    missing_seconds = metadata.get('missing_seconds', 0)
    if missing_seconds:
        description += " Due to technical difficulties, this video is missing {} seconds.".format(missing_seconds)

    return {
        'snippet': {
            'title': ytconfig['title'].format(**kwargs),
            'description': description,
            'tags': ytconfig.get('tags', []),
            'categoryId': 25,  # News & Politics
            'defaultLanguage': 'en',
            'defaultAudioLanguage': 'en',
        },
        'status': {
            'privacyStatus': ytconfig.get('privacy', 'unlisted'),
        },
        'recordingDetails': recordingDetails,
    }


def resumable_upload_status(session, upload_session_url, video_file):
    resp = session.put(upload_session_url, headers={'Content-Range': 'bytes */{}'.format(os.path.getsize(video_file))})
    if resp.status_code == 308:
        return int(resp.headers['Range'].split('-')[1]) + 1, int(resp.headers.get('Retry-After', 0))
    else:
        resp.raise_for_status()


def chunked_read(handle, chunk_size, progress):
    """
    Get a generator to go through a file's contents. For progress reporting of uploads.

    :param handle: File handle.
    :param chunk_size: Max bytes per chunk.
    :param tqdm progress: Report progress to this progress bar.
    """
    chunk = handle.read(chunk_size)
    while chunk:
        progress.update(len(chunk))
        yield chunk
        chunk = handle.read(chunk_size)


class YoutubeSession(OAuth2Session):
    def __init__(self, client, credentials):
        super().__init__(client, credentials)

    def start_resumable_upload(self, file_size, video_resource, notify_subscribers):
        print("Starting a resumable upload session")
        print("Notify subscribers: {}".format(notify_subscribers))
        resp = self.post(
            'https://www.googleapis.com/upload/youtube/v3/videos', params={
                'uploadType': 'resumable',
                'part': ','.join(video_resource.keys()),
                'notifySubscribers': notify_subscribers,
            }, headers={
                'X-Upload-Content-Length': str(file_size),
                'X-Upload-Content-Type': 'application/octet-stream',
            },
            json=video_resource,
            allow_redirects=False)
        resp.raise_for_status()
        return resp.headers['Location']

    def resumable_upload_status(self, upload_session_url, video_file):
        resp = self.put(upload_session_url,
                        headers={'Content-Range': 'bytes */{}'.format(os.path.getsize(video_file))})
        if resp.status_code == 308:
            return int(resp.headers['Range'].split('-')[1]) + 1, int(resp.headers.get('Retry-After', 0))
        else:
            resp.raise_for_status()

    def upload_video(self, video_path, config, metadata, minutes_url):
        video_size = os.path.getsize(video_path)
        video_resource = build_youtube_video_resource(config, metadata, minutes_url)
        print(video_resource['snippet']['title'])
        print(video_resource['snippet']['description'])
        print("Visibility: " + video_resource['status']['privacyStatus'])

        notify_subscribers = config['youtube'].get('notify_subscribers', False)
        session_url = self.start_resumable_upload(video_size, video_resource, notify_subscribers)

        seek_to = 0
        print("Starting upload of {} ({} bytes)".format(video_path, video_size))
        chunk_size = 1024 * 1024
        with open(video_path, 'rb') as video:
            while True:
                headers = {'Content-Type': 'application/octet-stream'}
                if seek_to > 0:
                    headers['Content-Range'] = 'bytes {}-{}/{}'.format(seek_to, video_size, video_size)
                video.seek(seek_to)
                progress = tqdm(total=video_size, initial=seek_to, unit_scale=True, dynamic_ncols=True)
                resp = session.put(session_url, data=chunked_read(video, chunk_size, progress), headers=headers)
                progress.close()
                if resp.status_code in (500, 502, 503, 504):
                    seek_to, retry_after = resumable_upload_status(session, session_url, video_path)
                    print("Failed after {}/{}. Retrying in {} seconds".format(seek_to, video_size, retry_after))
                    time.sleep(retry_after)
                    continue
                if resp.ok:  # Not necessarily 201, contrary to the doc.
                    js = resp.json()
                    print("Upload succeeded: https://www.youtube.com/watch?v=" + js['id'])
                    return js['id']
                print(resp.text)
                resp.raise_for_status()
                break

    def get_playlists(self):
        params = {
            'part': 'id,snippet',
            'mine': 'true',
        }
        while True:
            resp = self.get('https://www.googleapis.com/youtube/v3/playlists', params=params)
            resp.raise_for_status()
            resp = resp.json()
            for playlist in resp['items']:
                yield playlist
            if 'nextPageToken' in resp:
                params['pageToken'] = resp['nextPageToken']
                continue
            break

    def get_playlist(self, name):
        for playlist in self.get_playlists():
            if playlist['snippet']['title'] == name:
                return playlist['id']

    def add_playlist(self, name, privacy):
        playlist = {
            'snippet': {'title': name},
            'status': {
                'privacyStatus': privacy,
            }
        }
        resp = self.post('https://www.googleapis.com/youtube/v3/playlists',
                         params={'part': 'snippet,id,status'}, json=playlist)
        resp.raise_for_status()
        return resp.json()['id']

    def add_video_to_playlist(self, playlist_name, video_id, privacy_for_new_playlist):
        print("Adding {} to playlist '{}'".format(video_id, playlist_name))
        playlist_id = self.get_playlist(playlist_name)
        if not playlist_id:
            print("Playlist does not exist. Creating it")
            playlist_id = self.add_playlist(playlist_name, privacy_for_new_playlist)
        print("Playlist '{}' is {}".format(playlist_name, playlist_id))
        playlistItem = {
            'snippet': {
                'playlistId': playlist_id,
                'resourceId': {
                    'kind': 'youtube#video',
                    'videoId': video_id,
                }
            }
        }
        resp = self.post('https://www.googleapis.com/youtube/v3/playlistItems',
                         params={'part': 'snippet'},
                         json=playlistItem)
        resp.raise_for_status()


parser = argparse.ArgumentParser(description='Video uploader')
parser.add_argument('config_id')
parser.add_argument('action', choices=['authorize', 'upload'])
parser.add_argument('--keep-uploaded', action="store_true", help="Don't delete uploaded videos.")
parser.add_argument('--title-contains', help="Only upload videos containing this in its title.")
parser.add_argument('--for-date', help="Only upload videos for this date (YYYY-MM-DD), in local time.")

if __name__ == '__main__':
    args = parser.parse_args()
    config = get_config(args.config_id)
    local_tz = get_tz(config)
    for_date = None
    if args.for_date:
        for_date = datetime.strptime(args.for_date, '%Y-%m-%d')
        for_date = for_date.replace(tzinfo=local_tz).date()

    client_creds = load_client_credentials()
    credentials_file = config['id'] + '.tokens.json'

    if args.action == 'authorize':
        if os.path.isfile(credentials_file):
            print("This will replace the existing credentials in " + credentials_file)
        client_id = client_creds['client_id']
        user_code_resp = obtain_user_code(client_id)
        print("Visit " + user_code_resp['verification_url'])
        print("Enter code " + user_code_resp['user_code'])
        auth_resp = poll_for_authorization(client_id, client_creds['client_secret'], user_code_resp)
        with open(credentials_file, 'w') as outf:
            json.dump(auth_resp, outf)
        print("Credentials written to " + credentials_file)

    elif args.action == 'upload':
        if not os.path.isfile(credentials_file):
            print("Need to call 'youtube.py {} authorize' first.".format(config['id']))
        with open(credentials_file) as inf:
            credentials = json.load(inf)

        session = YoutubeSession(client_creds, credentials)

        for filename in sorted(os.listdir('videos')):
            if not filename.startswith(config['id']):
                continue
            if not filename.endswith('.mp4'):
                continue
            if filename.endswith('.tmp.mp4'):
                print('{} is not ready'.format(filename))
                break
            video_path = os.path.join('videos', filename)
            metadata_path = os.path.join('videos', filename.replace('.mp4', '.yaml'))
            with open(metadata_path) as inf:
                metadata = yaml.safe_load(inf)

            if args.title_contains and args.title_contains not in metadata['title']:
                continue
            video_ts = parse_timestamp_naively(metadata['start']).astimezone(local_tz)
            if for_date and for_date != video_ts.date():
                continue

            print("Working on: " + metadata['title'])

            minutes_url = None
            if '{minutes_url}' in config['youtube']['desc']:
                try:
                    minutes_url = get_minutes_url(config, metadata)
                except HTTPError as e:
                    print(e)
                    while True:
                        minutes_url = input("Enter minutes URL for this video: ")
                        if minutes_url.startswith('http') and not session.head(minutes_url).ok:
                            print("Invalid URL. Try again")
                            continue
                        break


            max_retries = 6
            video_id = None
            for i in range(max_retries):
                try:
                    video_id = session.upload_video(video_path, config, metadata, minutes_url)
                    break
                except HTTPError as e:
                    if i == (max_retries-1):
                        raise
                    if e.response.status_code == 410:
                        print(e)
                        print("Reattempt {} after unresumable failure. Sleeping before retry...".format(i+1))
                        time.sleep(60)
                        continue
                    raise

            playlists = config['youtube'].get('playlist')
            if playlists:
                if not isinstance(playlists, list):
                    playlists = [playlists]
                for playlist in playlists:
                    ts = parse_timestamp_naively(metadata['start'])
                    playlist = playlist.format(
                        clip_date=ts.astimezone(get_tz(config)),
                        project_name=metadata.get('project_name', ''),
                    ).strip()
                    session.add_video_to_playlist(playlist, video_id, config['youtube'].get('privacy', 'unlisted'))

            if not args.keep_uploaded:
                os.remove(video_path)
                os.remove(metadata_path)

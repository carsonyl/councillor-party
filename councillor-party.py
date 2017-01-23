import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from itertools import groupby
from typing import Iterable

import click
import pendulum
from tqdm import tqdm

from common import VideoProvider, yaml_dump, yaml_load, build_substitutions_dict, tweak_metadata
from config import get_config
from insinc import InsIncScraperApi
from neulion import NeulionScraperApi
from oauth import load_client_credentials, obtain_user_code, poll_for_authorization, tokens_file_for_id
from youtube import YouTubeSession, build_youtube_resource

METADATA_DIR = 'metadata'
DOWNLOADS_DIR = 'downloads'
VIDEOS_DIR = 'videos'


def get_provider_obj(config) -> VideoProvider:
    provider = config['provider']
    if provider == 'insinc':
        return InsIncScraperApi(config['url'])
    elif provider == 'neulion':
        return NeulionScraperApi(config['url'])


def parse_date_range(for_dates):
    start_date, end_date = for_dates.split('..')
    return pendulum.parse(start_date), pendulum.parse(end_date)


def parse_dates(for_dates) -> Iterable[pendulum.Date]:
    return [pendulum.parse(dt) for dt in for_dates.split(',')]


@click.group()
@click.argument('config')
@click.pass_context
def cli(ctx, config):
    ctx.obj = get_config(config)


@cli.command(help='Query for dates with videos available.')
@click.argument('start_date')
@click.argument('end_date')
@click.pass_obj
def dates(config, start_date, end_date):
    start_date, end_date = pendulum.parse(start_date), pendulum.parse(end_date)
    provider = get_provider_obj(config)
    dt = provider.available_dates(start_date, end_date)
    print('Available dates:\n' + '\n'.join(d.isoformat() for d in dt))


@cli.command(help='Download metadata for videos on the given dates.')
@click.argument('for_dates')
@click.pass_obj
def metadata(config, for_dates):
    provider = get_provider_obj(config)
    if '..' in for_dates:
        start_date, end_date = parse_date_range(for_dates)
        for_dates = provider.available_dates(start_date, end_date)
    else:
        for_dates = parse_dates(for_dates)

    project_metadata_dir = os.path.join(METADATA_DIR, config['id'])
    if not os.path.exists(project_metadata_dir):
        os.makedirs(project_metadata_dir)

    for dt in for_dates:
        print(dt.to_date_string())
        metadata_path = os.path.join(project_metadata_dir, dt.to_date_string() + '.yaml')
        date_metadata = list(provider.get_metadata(dt))
        if not date_metadata:
            print("No available videos for " + dt.to_date_string())
            continue
        yaml_dump(date_metadata, metadata_path)
        with open(metadata_path) as inf:
            print(inf.read())


@cli.command(help='Download videos for the specified dates. Metadata must be downloaded first.')
@click.argument('for_dates')
@click.option('--threads', default=4)
@click.pass_obj
def download(config, for_dates, threads):
    provider = get_provider_obj(config)
    metadata_dir = os.path.join(METADATA_DIR, config['id'])
    if '..' in for_dates:
        start_date, end_date = parse_date_range(for_dates)
        for_dates = []
        for filename in sorted(os.listdir(metadata_dir)):
            metadata_date = pendulum.parse(filename.split('.')[0]).date()
            if start_date <= metadata_date <= end_date:
                for_dates.append(metadata_date)
    else:
        for_dates = parse_dates(for_dates)

    def load_date_metadata():
        for dt in for_dates:
            date_metadata_path = os.path.join(metadata_dir, dt.to_date_string() + '.yaml')
            if not os.path.exists(date_metadata_path):
                raise ValueError("No metadata downloaded for " + dt.to_date_string())
            yield yaml_load(date_metadata_path)

    if config['provider'] == 'insinc':
        futures = []
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for date_metadata in load_date_metadata():
                for mms_url, video_metadatas in groupby(date_metadata, key=lambda m: m.url):
                    video_metadatas = list(video_metadatas)
                    dest = os.path.join('downloads', config['id'], video_metadatas[0].video_id)
                    if not os.path.exists(dest):
                        os.makedirs(dest)
                    print("Starting task to save {} to {}".format(mms_url, dest))
                    yaml_dump(video_metadatas, os.path.join(dest, '_metadata.yaml'))
                    futures.append(executor.submit(provider.download, mms_url, dest))
            progressbar = tqdm(total=len(futures), dynamic_ncols=True)
            for future in as_completed(futures):
                future.result()
                progressbar.update()
            progressbar.close()
    else:
        for date_metadata in load_date_metadata():
            for root in date_metadata:
                dest = os.path.join('downloads', config['id'], root.video_id)
                if not os.path.exists(dest):
                    os.makedirs(dest)
                print("Starting task to save {} to {}".format(root.url, dest))
                yaml_dump([root], os.path.join(dest, '_metadata.yaml'))
                provider.download(root.url, dest)


@cli.command(help='Do any needed post-processing for downloaded videos.')
@click.option('--delete-after', default=False)
@click.option('--startswith', help='Process directories starting with this text.', default=None)
@click.pass_obj
def process(config, delete_after, startswith):
    yt_config = config['youtube']
    provider = get_provider_obj(config)

    project_dir = os.path.join(DOWNLOADS_DIR, config['id'])
    for download_dir in filter(lambda d: not d.startswith('_'), sorted(os.listdir(project_dir))):
        if startswith and not download_dir.startswith(startswith):
            continue
        download_dir = os.path.join(project_dir, download_dir)
        if not os.path.isdir(download_dir):
            continue

        metadatas = yaml_load(os.path.join(download_dir, '_metadata.yaml'))
        mono = config.get('audio_mono', False)
        for video_metadata in metadatas:
            prepped_video_info = provider.postprocess(video_metadata, download_dir, VIDEOS_DIR, mono=mono)
            prepped_video_info.config_id = config['id']

            overrides = tweak_metadata(config['id'], prepped_video_info.video_metadata)
            subs = build_substitutions_dict(prepped_video_info.video_metadata)
            subs.update(overrides)

            prepped_video_info.title = yt_config['title'].format(**subs)
            prepped_video_info.description = yt_config['desc'].format(**subs).strip()
            prepped_video_info.playlists = [pl.format(**subs) for pl in yt_config['playlists']]

            prepped_video_info_path = os.path.join(VIDEOS_DIR, prepped_video_info.video_filename + '.yaml')
            yaml_dump(prepped_video_info, prepped_video_info_path)
            print("Updated " + prepped_video_info_path)
        if delete_after:
            print("Deleting " + project_dir)
            shutil.rmtree(project_dir)


@cli.group()
def youtube():
    pass


@youtube.command(help='Obtain OAuth 2.0 refresh token for the YouTube channel.')
@click.pass_obj
def authorize(config):
    client_creds = load_client_credentials()
    tokens_file = tokens_file_for_id(config['id'])

    if os.path.isfile(tokens_file):
        print("This will replace the existing credentials in " + tokens_file)
    client_id = client_creds['client_id']
    user_code_resp = obtain_user_code(client_id)
    print("Visit " + user_code_resp['verification_url'])
    print("Enter code " + user_code_resp['user_code'])
    auth_resp = poll_for_authorization(client_id, client_creds['client_secret'], user_code_resp)
    with open(tokens_file, 'w') as outf:
        json.dump(auth_resp, outf)
    print("Credentials written to " + tokens_file)


@youtube.command(help='Upload finished videos to YouTube.')
@click.option('--delete-after', default=False)
@click.pass_obj
def upload(config, delete_after):
    yt_config = config['youtube']
    client_creds = load_client_credentials()
    tokens_file = tokens_file_for_id(config['id'])
    if not os.path.isfile(tokens_file):
        print("Need to perform 'youtube authorize' first.")

    with open(tokens_file) as inf:
        tokens = json.load(inf)

    session = YouTubeSession(client_creds, tokens)
    for metadata_filename in sorted(filter(lambda f: f.endswith('.yaml'), os.listdir(VIDEOS_DIR))):
        metadata_path = os.path.join(VIDEOS_DIR, metadata_filename)
        prepped_video_info = yaml_load(metadata_path)
        if prepped_video_info.config_id != config['id']:
            continue

        video_path = os.path.join(VIDEOS_DIR, prepped_video_info.video_filename)
        if not os.path.exists(video_path):
            print(video_path + " doesn't exist")
            continue
        yt_video_res = build_youtube_resource(
            prepped_video_info.title,
            prepped_video_info.description,
            pendulum.parse(prepped_video_info.video_metadata.start_ts),
            coords=yt_config['location'],
            location_desc=yt_config['location_desc'],
            tags=yt_config['tags'],
            privacy=yt_config['privacy'],
        )
        print(yt_video_res)
        yt_video_id = session.upload(video_path, yt_video_res, yt_config['notify_subscribers'])

        for playlist in prepped_video_info.playlists:
            session.add_video_to_playlist(playlist, yt_video_id, yt_config['privacy'])

        if delete_after:
            for f in (metadata_path, video_path):
                print("Deleting " + f)
                os.remove(f)


if __name__ == '__main__':
    cli()

"""
Script and functions for downloading videos and their metadata.
"""
import argparse
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time

import pytz
import yaml

from common import group_root_and_subclips
from config import get_config, get_tz
from granicus import GranicusScraperApi
from insinc import InsIncScraperApi, group_clips
from neulion import adaptive_url_to_segment_urls, NeulionScraperApi, group_video_clips, calculate_timecodes, \
    parse_time_range_from_url, duration_to_timecode
from segments import download_clip

log = logging.getLogger()
SEGMENT_FILE_PATTERN = '%Y%m%d%H%M%S.mp4'


def write_video_metadata(config, clip_info, project_id_map, timecodes, out_file):
    print("Writing video metadata to " + out_file)
    start_ts, end_ts, duration = parse_time_range_from_url(clip_info.url)
    timecodes = [{'time': timecode, 'title': clip.name} for timecode, clip in timecodes.items()]
    timecodes = sorted(timecodes, key=lambda x: x['time'])

    metadata = {
        'config_id': config['id'],
        'recorded_date': clip_info.start_utc.isoformat(),
        'start': start_ts.isoformat(),
        'end': end_ts.isoformat(),
        'duration': duration_to_timecode(duration),
        'title': clip_info.name,
        'video_url': clip_info.url,
        'project_id': clip_info.project,
        'project_name': project_id_map[clip_info.project].replace(';', ''),
        'id': clip_info.id,
        'timecodes': timecodes,
    }
    with open(out_file, 'w') as outf:
        yaml.dump(metadata, outf)


parser = argparse.ArgumentParser(description='Download the video segments for video clips.')
parser.add_argument('config_id', help='ID of the config document to use from config.yaml.')
parser.add_argument('dates', help='Download video segments for videos on this date (YYYY-MM-DD) in local time.')
parser.add_argument('--title-contains', help='Only download video segments for clips that contain this in its title.')
parser.add_argument('--workers', type=int, default=8, help='Max number of concurrent downloads.')


def prepare_segments_output_dir(segments_dir):
    outdir = os.path.join('segments', segments_dir)
    if not os.path.isdir(outdir):
        os.makedirs(outdir)
    return outdir


def download_neulion(args, dates):
    api = NeulionScraperApi(config['url'])
    all_projects = next(api.projects())
    project_id_map = {project.id: project.name for project in api.projects()}
    for date in dates:
        clips = list(api.clips(date, all_projects.id))
        clip_groups = group_video_clips(clips)
        for root_clip, subclips in clip_groups.items():
            print("Root clip: " + root_clip.name)
            for subclip in subclips:
                print("Subclip: " + subclip.name)

        for root_clip, subclips in clip_groups.items():
            if args.title_contains and args.title_contains not in root_clip.name:
                continue
            print("Working on {}".format(root_clip.name))
            timecodes = calculate_timecodes(root_clip, subclips)
            local_time = root_clip.start_utc.astimezone(get_tz(config))
            segments_dir = '{}_{:%Y%m%d}_{}'.format(config['id'], local_time, root_clip.id.replace(',', '.'))
            outdir = prepare_segments_output_dir(segments_dir)
            write_video_metadata(config, root_clip, project_id_map, timecodes, os.path.join(outdir, '_metadata.yaml'))
            download_clip(adaptive_url_to_segment_urls(root_clip.url), outdir, args.workers)

            with open(os.path.join(outdir, '_done.txt'), 'w') as f:
                f.write('yes')


def download_granicus(args, dates):
    api = GranicusScraperApi(config['url'])
    for video in api.get_videos():
        if video.date not in dates:
            continue
        print("Working on '{}' for {}".format(video.title, video.date.isoformat()))
        clip_id = api.get_clip_id(video.video_url)
        segments_dir = '{}_{:%Y%m%d}_{}'.format(config['id'], video.date, clip_id)
        outdir = prepare_segments_output_dir(segments_dir)

        local_tz = pytz.timezone('America/Vancouver')
        video_ts_local = local_tz.localize(datetime.combine(video.date, time(0, 0)))
        video_ts_utc = video_ts_local.astimezone(pytz.utc)

        stripped_title = re.sub(r' \(\w+\.? \d+, \d+\)', '', video.title, flags=re.I).strip()
        metadata = {
            'config_id': config['id'],
            'recorded_date': video_ts_utc.isoformat(),
            'start': video_ts_utc.isoformat(),
            'end': video_ts_utc.isoformat(),
            'title': stripped_title,
            'video_url': video.video_url,
            'id': clip_id,
            'timecodes': [],
            'minutes_url': 'http://www.surrey.ca/bylawsandcouncillibrary/MIN_{}_{:%Y_%m_%d}.pdf'.format(
                video.minutes_url_title.split()[0], video.date),
        }
        with open(os.path.join(outdir, '_metadata.yaml'), 'w') as outf:
            yaml.dump(metadata, outf)

        streams = api.get_streams(clip_id)
        download_clip(api.get_video_piece_urls(streams.m3u8_url), outdir, args.workers)

        with open(os.path.join(outdir, '_done.txt'), 'w') as f:
            f.write('yes')


def download_insinc(args, dates):
    api = InsIncScraperApi(config['url'])
    with ThreadPoolExecutor(max_workers=8) as executor:
        for dt in dates:
            print("Looking for clips on {:%Y-%m-%d}".format(dt))
            for mms_url, grouped_clips in group_clips(api.get_clips(dt)).items():
                download_dest = os.path.join(ar)
                executor.submit(download_mms, mms_url, download_dest)
                for root_clip, subclips in group_root_and_subclips(grouped_clips).items():
                    print("Working on '{}' for {}".format(root_clip.title, dt.isoformat()))
                    for subclip in subclips:
                        print("Subclip: {}".format(subclip))
                    mms_filename = os.path.basename(mms_url) + root_clip.start_time.strftime('%H%M%S')
                    segments_dir = '{}_{:%Y%m%d}_{}'.format(config['id'], dt, mms_filename)
                    outdir = prepare_segments_output_dir(segments_dir)

                    local_tz = pytz.timezone('America/Vancouver')
                    video_ts_local = local_tz.localize(datetime.combine(dt, time(0, 0)))
                    video_ts_utc = video_ts_local.astimezone(pytz.utc)

                    metadata = {
                        'config_id': config['id'],
                        'recorded_date': video_ts_utc.isoformat(),
                        'start': video_ts_utc.isoformat(),
                        'end': video_ts_utc.isoformat(),
                        'title': root_clip.title,
                        'video_url': mms_url,
                        'id': mms_filename,
                        'timecodes': [],
                    }
                    with open(os.path.join(outdir, '_metadata.yaml'), 'w') as outf:
                        yaml.dump(metadata, outf)

                    download_mms(mms_url, root_clip.start_time, root_clip.duration, outdir)

                    with open(os.path.join(outdir, '_done.txt'), 'w') as f:
                        f.write('yes')


if __name__ == '__main__':
    args = parser.parse_args()
    config = get_config(args.config_id)
    dates = [datetime.strptime(date, '%Y-%m-%d').date() for date in args.dates.split(',')]

    provider = config['provider']
    if provider == 'neulion':
        download_neulion(args, dates)
    elif provider == 'granicus':
        download_granicus(args, dates)
    elif provider == 'insinc':
        download_insinc(args, dates)

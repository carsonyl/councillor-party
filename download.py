"""
Script and functions for downloading videos and their metadata.
"""
import argparse
import logging
import os
import yaml
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import datetime
from requests import Session
from tqdm import tqdm

from config import get_config, get_tz
from granicus import GranicusScraperApi
from neulion import adaptive_url_to_segment_urls, NeulionScraperApi, group_video_clips, calculate_timecodes, \
    parse_time_range_from_url, duration_to_timecode, segment_url_to_timestamp

log = logging.getLogger()
SEGMENT_FILE_PATTERN = '%Y%m%d%H%M%S.mp4'


class MissingSegmentError(ValueError):
    def __init__(self, clip_url):
        super(MissingSegmentError, self).__init__()
        self.clip_url = clip_url


def download_segment(session, clip_url, dest):
    resp = session.get(clip_url, stream=True)
    resp.raise_for_status()
    with open(dest, 'wb') as outvid:
        for chunk in resp.iter_content(chunk_size=2048):
            outvid.write(chunk)
    if not os.path.getsize(dest):
        os.remove(dest)
        raise MissingSegmentError(clip_url)


def download_clip(segment_urls, destination, workers):
    if not os.path.isdir(destination):
        raise ValueError("destination must be directory")

    for trailing_file in sorted(filter(lambda filename: filename.endswith('.mp4'), os.listdir(destination)))[-workers:]:
        print("Deleting potentially incomplete segment {}".format(trailing_file))
        os.remove(os.path.join(destination, trailing_file))

    session = Session()
    num_skipped_because_already_exists, num_missing_segments = 0, 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []

        for i, segment_url in enumerate(segment_urls):
            try:
                timestamp = segment_url_to_timestamp(segment_url)
                filename = timestamp.strftime(SEGMENT_FILE_PATTERN)
            except ValueError:
                filename = str(i).zfill(5) + '.' + os.path.basename(segment_url).split('.')[-1]
            dest = os.path.join(destination, filename)
            if os.path.exists(dest) and os.path.getsize(dest):
                # print("{} Already exists - skipping".format(dest))
                num_skipped_because_already_exists += 1
                continue

            future = executor.submit(download_segment, session, segment_url, dest)
            futures.append(future)

        print("{} segments were previously downloaded".format(num_skipped_because_already_exists))
        progressbar = tqdm(total=num_skipped_because_already_exists + len(futures),
                           initial=num_skipped_because_already_exists, dynamic_ncols=True)
        with open(os.path.join(destination, '_missing_segments.txt'), 'w') as missing_segments:
            for future in as_completed(futures):
                try:
                    future.result()
                except MissingSegmentError as e:
                    missing_segments.write(e.clip_url + '\n')
                    num_missing_segments += 1
                except Exception as e:
                    print(e)
                    raise
                progressbar.update()
        progressbar.close()

    if num_missing_segments:
        log.warning("{} segments were empty and omitted".format(num_missing_segments))

    total_size = sum(os.path.getsize(os.path.join(destination, f)) for f in os.listdir(destination))
    print("Downloaded {:.1f} MB".format(total_size / 1024 / 1024))


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

        metadata = {
            'config_id': config['id'],
            'recorded_date': video.date.isoformat(),
            'start': video.date.isoformat() + 'T00:00:00Z',
            'end': video.date.isoformat() + 'T00:00:00Z',
            'title': video.title,
            'video_url': video.video_url,
            'id': clip_id,
            'timecodes': [],
        }
        with open(os.path.join(outdir, '_metadata.yaml'), 'w') as outf:
            yaml.dump(metadata, outf)

        streams = api.get_streams(clip_id)
        download_clip(api.get_video_piece_urls(streams.m3u8_url), outdir, args.workers)

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

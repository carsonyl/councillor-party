"""
Script and functions for concatenating downloaded video segments using ffmpeg.
"""
import argparse
import os
import shutil
import time
from datetime import timedelta, datetime

import yaml
from yaml import safe_load

from config import get_config
from download import SEGMENT_FILE_PATTERN
from ffmpeg import ffmpeg_concat, ffmpeg_duration
from neulion import duration_to_timecode, timecode_to_duration
from segment_tools import segment_url_to_timestamp, write_ffmpeg_concat_file, segment_files


def adjust_timecodes_for_missing_segments(start_ts, missing_timestamps, timecodes):
    """
    Adjust timecodes for missing segments.

    :param datetime start_ts: Start of the video.
    :param Iterable[datetime] missing_timestamps: Each timestamp represents 2 seconds missing, starting at that time.
    :param list[dict] timecodes: Timecodes to update.
    :return: Number of seconds missing in total.
    """
    new_timecodes = [timecode_to_duration(entry['time']) for entry in timecodes]
    num_missing_segments = 0
    for missing_timestamp in missing_timestamps:
        num_missing_segments += 1
        delta = (missing_timestamp - start_ts).seconds
        for i, secs in enumerate(new_timecodes):
            if secs >= delta:
                new_timecodes[i] -= 2
    for i, secs in enumerate(new_timecodes):
        timecode = timecodes[i]
        timecode['old_time'] = timecode['time']
        timecode['time'] = duration_to_timecode(timedelta(seconds=secs))
    print("Adjusted timecodes for {} missing segments".format(num_missing_segments))
    return num_missing_segments * 2


parser = argparse.ArgumentParser(description='Video segment concatenator')
parser.add_argument('--dir-name-contains', help='Only concatenate segment directories containing this in its name.')
parser.add_argument('--monitor', action="store_true",
                    help='Continuously look for clip segments ready to be concatenated.')
parser.add_argument('--keep-inputs', action="store_true",
                    help="Don't delete segments directory after concatenating it into a video.")
parser.add_argument('--ffmpeg-log-level', default='error')

if __name__ == '__main__':
    args = parser.parse_args()
    while True:
        if not os.path.isdir('videos'):
            os.mkdir('videos')

        for segment_dir in sorted(os.listdir('segments')):
            if args.dir_name_contains and args.dir_name_contains not in segment_dir:
                continue
            segment_dir_path = os.path.join('segments', segment_dir)
            if not os.path.exists(os.path.join(segment_dir_path, '_done.txt')):
                print("{} is not yet complete".format(segment_dir))
                continue

            metadata_path = os.path.join(segment_dir_path, '_metadata.yaml')
            with open(metadata_path) as inf:
                metadata = safe_load(inf)
            config = get_config(metadata['config_id'])
            provider = config['provider']
            if provider not in ('neulion', 'granicus'):
                continue

            try:
                start_ts = datetime.strptime(segment_files(segment_dir_path)[0], SEGMENT_FILE_PATTERN)
                with open(os.path.join(segment_dir_path, '_missing_segments.txt')) as msf:
                    missing_segment_timestamps = map(
                        lambda segment_url: segment_url_to_timestamp(segment_url.rstrip()), msf
                    )
                    num_missing_seconds = adjust_timecodes_for_missing_segments(start_ts, missing_segment_timestamps,
                                                                                metadata['timecodes'])
                metadata['missing_seconds'] = num_missing_seconds
            except ValueError:
                pass

            segment_duration_s = 2 if provider == 'neulion' else None
            concat_file = write_ffmpeg_concat_file(segment_dir_path, segment_duration=segment_duration_s)
            video_out = os.path.join('videos', segment_dir + '.wmv')
            ffmpeg_concat(concat_file, video_out, config.get('audio_mono', False), args.ffmpeg_log_level)
            print("Finished concatenating " + video_out)

            concat_duration = ffmpeg_duration(video_out)
            concat_duration_txt = duration_to_timecode(timedelta(seconds=int(round(concat_duration))))
            print("Original duration: {}. Concatenated duration: {}".format(metadata.get('duration', '?'), concat_duration_txt))

            # Update metadata with concatenated duration.
            metadata['concat_duration'] = concat_duration_txt
            with open(os.path.join('videos', segment_dir + '.yaml'), 'w') as outf:
                yaml.dump(metadata, outf)

            if not args.keep_inputs:
                print("Deleting segments directory " + os.path.join('segments', segment_dir))
                shutil.rmtree(segment_dir_path)

        if args.monitor:
            time.sleep(10)
        else:
            break

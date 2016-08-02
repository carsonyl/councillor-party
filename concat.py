"""
Script and functions for concatenating downloaded video segments using ffmpeg.
"""
import argparse
import codecs
import os
import subprocess

import time
import yaml
from datetime import timedelta
from yaml import safe_load

import shutil

from config import get_config
from neulion import duration_to_timecode, timecode_to_duration


def ffmpeg_concat(concat_file, video_out, mono, loglevel='error'):
    # http://stackoverflow.com/questions/7333232/concatenate-two-mp4-files-using-ffmpeg
    # http://superuser.com/questions/924364/ffmpeg-how-to-convert-stereo-to-mono-using-audio-pan-filter
    print("Concatenating videos listed in {} to {}".format(concat_file, video_out))
    tmp_video_out = video_out.replace('.mp4', '.tmp.mp4')
    for check_existing in (tmp_video_out, video_out):
        if os.path.isfile(check_existing):
            print("Deleting existing video " + check_existing)
            os.remove(check_existing)

    cmd = ['ffmpeg', '-loglevel', loglevel, '-safe', '0', '-f', 'concat', '-i', concat_file]
    if mono:
        print("Converting to mono")
        cmd.extend(['-af', 'pan=mono|c0=c0', '-c:v', 'copy'])
    else:
        cmd.extend(['-c', 'copy'])
    cmd.append(tmp_video_out)
    subprocess.check_call(cmd)
    os.rename(tmp_video_out, video_out)


def ffmpeg_duration(video_path):
    """
    Get video duration using ffprobe.

    :param video_path: Video to inspect.
    :return: Video duration in float seconds.
    """
    result = subprocess.check_output(['ffprobe', '-show_entries', 'format=duration', video_path])
    result = codecs.decode(result, 'utf8')
    result = result[result.find('[FORMAT]'):result.find('[/FORMAT]')]
    return float(result.split('=')[1].strip())


def adjust_timecodes(original_duration_s, actual_duration_s, timecodes):
    """
    Adjust timecodes for drift that occurred after concatenation.
    As the video length increases, the concatenated video becomes longer than the original duration.

    :param original_duration_s: Original duration, as claimed by the server.
    :param actual_duration_s: Actual duration of the concatenated result.
    :param list[dict] timecodes: List of timecode entries. Each entry is a dict with a `time` key.
        Entries are modified in-place, with the value of `time` replaced, and the old value saved to `old_time`.
    """
    factor = actual_duration_s / original_duration_s
    print("Adjusting timecodes using factor of {}".format(factor))
    for timecode in timecodes:
        timecode['old_time'] = timecode['time']
        time_s = timecode_to_duration(timecode['time']) * factor
        timecode['time'] = duration_to_timecode(timedelta(seconds=int(round(time_s))))


parser = argparse.ArgumentParser(description='Video segment concatenator')
parser.add_argument('--dir-name-contains', help='Only concatenate segment directories containing this in its name.')
parser.add_argument('--monitor', action="store_true",
                    help='Continuously look for clip segments ready to be concatenated.')
parser.add_argument('--keep-inputs', action="store_true",
                    help="Don't delete segments directory after concatenating it into a video.")

if __name__ == '__main__':
    args = parser.parse_args()
    while True:
        if not os.path.isdir('videos'):
            os.mkdir('videos')

        for segment_dir in sorted(os.listdir('segments')):
            if args.dir_name_contains and args.dir_name_contains not in segment_dir:
                continue
            concat_file = os.path.join('segments', segment_dir, '_concat.txt')
            if not os.path.isfile(concat_file):
                print("{} is not yet complete".format(segment_dir))
                continue
            metadata_path = os.path.join('segments', segment_dir, '_metadata.yaml')
            with open(metadata_path) as inf:
                metadata = safe_load(inf)
            config = get_config(metadata['config_id'])
            audio_mono = config.get('audio_mono', False)

            video_out = os.path.join('videos', segment_dir + '.mp4')
            ffmpeg_concat(concat_file, video_out, audio_mono)
            print("Finished concatenating " + video_out)

            concat_duration = ffmpeg_duration(video_out)
            concat_duration_txt = duration_to_timecode(timedelta(seconds=int(round(concat_duration))))
            print("Original duration: {}. Concatenated duration: {}".format(metadata['duration'], concat_duration_txt))

            # Update metadata with new duration and timecodes adjusted for the new duration.
            metadata['new_duration'] = concat_duration_txt
            adjust_timecodes(timecode_to_duration(metadata['duration']), concat_duration, metadata['timecodes'])
            with open(metadata_path, 'w') as outf:
                yaml.dump(metadata, outf)
            shutil.copy(metadata_path, os.path.join('videos', segment_dir + '.yaml'))

            if not args.keep_inputs:
                print("Deleting segments directory " + os.path.join('segments', segment_dir))
                shutil.rmtree(os.path.join('segments', segment_dir))

        if args.monitor:
            time.sleep(10)
        else:
            break

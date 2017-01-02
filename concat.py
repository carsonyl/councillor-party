"""
Script and functions for concatenating downloaded video segments using ffmpeg.
"""
import argparse
import codecs
import os
import shutil
import subprocess
import time
import yaml
from datetime import timedelta, datetime
from yaml import safe_load

from config import get_config
from download import SEGMENT_FILE_PATTERN
from neulion import duration_to_timecode, timecode_to_duration, segment_url_to_timestamp


def segment_files(segments_dir):
    return sorted(filter(lambda filename: not filename.startswith('_'), os.listdir(segments_dir)))


def write_ffmpeg_concat_file(segments_dir, segment_duration):
    concat_file_path = os.path.join(segments_dir, '_concat.txt')
    print("Writing ffmpeg concat file to " + concat_file_path)
    tmp_out = concat_file_path + '.tmp'
    with open(tmp_out, 'w') as concat_file:
        for filename in segment_files(segments_dir):
            # Windows ffmpeg needs paths relative to ffmpeg binary.
            # Linux ffmpeg needs paths relative to the concat file.
            segment_path = os.path.join(segments_dir, filename) if os.name == 'nt' else filename
            concat_file.write("file '{}'\n".format(segment_path))
            # Be explicit about duration instead of letting ffmpeg infer it.
            # Otherwise, error accumulates and video lengthens over time.
            if segment_duration:
                concat_file.write("duration {}\n".format(segment_duration))
    if os.path.isfile(concat_file_path):
        os.remove(concat_file_path)
    os.rename(tmp_out, concat_file_path)
    return concat_file_path


def ffmpeg_concat(concat_file, video_out, mono, loglevel):
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
        # The encoder 'aac' is experimental but experimental codecs are not enabled, add '-strict -2' if you want to use it.
        cmd.extend(['-af', 'pan=mono|c0=c0', '-c:v', 'copy', '-strict', '-2'])
    else:
        # '-bsf:a', 'aac_adtstoasc'
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

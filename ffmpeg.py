import codecs
import os
import subprocess
from subprocess import check_call


def tempfile_suffix(original_path):
    filename = os.path.basename(original_path)
    filename = filename.split('.')
    filename.insert(len(filename)-1, 'tmp')
    return os.path.join(os.path.dirname(original_path), '.'.join(filename))


def get_temp_destination(original_path):
    temp_destination = tempfile_suffix(original_path)
    if os.path.exists(temp_destination):
        os.remove(temp_destination)
    return temp_destination


def download_mms(mms_url, destination_path):
    mms_url = mms_url.replace('mms://', 'mmsh://')
    temp_path = get_temp_destination(destination_path)
    check_call(['ffmpeg', '-loglevel', 'error', '-i', mms_url, '-c', 'copy', temp_path])
    os.rename(temp_path, destination_path)


def clip_video(video_path, ss, to, destination_path):
    temp_path = get_temp_destination(destination_path)
    check_call(['ffmpeg', '-loglevel', 'error', '-i', video_path, '-ss', ss, '-c', 'copy', '-to', to, temp_path])
    os.rename(temp_path, destination_path)


def ffmpeg_concat(concat_file, video_out, mono=False, loglevel='warning'):
    # http://stackoverflow.com/questions/7333232/concatenate-two-mp4-files-using-ffmpeg
    # http://superuser.com/questions/924364/ffmpeg-how-to-convert-stereo-to-mono-using-audio-pan-filter
    tmp_video_out = video_out.replace('.mp4', '.tmp.mp4')
    for check_existing in (tmp_video_out, video_out):
        if os.path.exists(check_existing):
            os.remove(check_existing)

    cmd = ['ffmpeg', '-loglevel', loglevel, '-safe', '0', '-f', 'concat', '-i', concat_file]
    if mono:
        # The encoder 'aac' is experimental but experimental codecs are not enabled, add '-strict -2' if you want to use it.
        # Must specify AAC encoder or else result is not to spec and will be silent in VLC.
        cmd.extend(['-c:a', 'aac', '-af', 'pan=mono|c0=c0', '-c:v', 'copy', '-strict', '-2'])
    else:
        # '-bsf:a', 'aac_adtstoasc'
        cmd.extend(['-c', 'copy'])
    cmd.append(tmp_video_out)
    check_call(cmd)
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

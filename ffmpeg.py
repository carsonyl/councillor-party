import os
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

import argparse
import os

from api import youtube_upload

parser = argparse.ArgumentParser(description='Surrey City Council video uploader')

if __name__ == '__main__':
    args = parser.parse_args()
    local_vids = sorted(filter(lambda name: name.endswith('.mp4') and '-temp-' not in name, os.listdir('.')))
    for vid in local_vids:
        print("Uploading " + vid)
        concat_file = vid.replace('.mp4', '.concat.txt')
        skip_if_present = [vid.replace('.mp4', '.concat.txt'), vid + '.tmp']
        if os.path.exists(skip_if_present[0]) or os.path.exists(skip_if_present[1]):
            print("{} is not ready. Skipping".format(vid))
            continue
        youtube_upload(vid)
        os.remove(vid)

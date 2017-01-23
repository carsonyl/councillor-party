import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import logging
from requests import Session
from tqdm import tqdm

log = logging.getLogger()
SEGMENT_FILE_PATTERN = '%Y%m%d%H%M%S.mp4'


class MissingSegmentError(ValueError):
    def __init__(self, clip_url):
        super(MissingSegmentError, self).__init__()
        self.clip_url = clip_url


def download_segment(session, clip_url, dest):
    resp = session.get(clip_url, stream=True)
    resp.raise_for_status()
    tmp_dest = dest + '.tmp'
    with open(tmp_dest, 'wb') as outvid:
        for chunk in resp.iter_content(chunk_size=2048):
            outvid.write(chunk)
    if os.path.getsize(tmp_dest):
        if os.path.exists(dest):
            os.remove(dest)
        os.rename(tmp_dest, dest)
    else:
        os.remove(tmp_dest)
        raise MissingSegmentError(clip_url)


def download_clip(segment_urls, destination, workers):
    if not os.path.isdir(destination):
        raise ValueError("destination must be directory")

    for incomplete_file in filter(lambda filename: filename.endswith('.mp4.tmp'), os.listdir(destination)):
        print("Deleting incomplete segment {}".format(incomplete_file))
        os.remove(os.path.join(destination, incomplete_file))

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


def segment_url_to_timestamp(segment_url):
    return datetime.strptime(''.join(segment_url.split('/')[-3:])[:-4], '%Y%m%d%H%M%S')


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


def segment_files(segments_dir):
    return sorted(filter(lambda filename: not filename.startswith('_'), os.listdir(segments_dir)))

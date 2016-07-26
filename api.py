import codecs
import os
import pytz
import requests
import subprocess
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import timedelta, datetime
from requests import Session
from urllib.parse import urlparse

import shutil

CLIPS_URL = ('http://civic.neulion.com/api/clipmanager.php?f=getClips&custname=&custid=&device=desktop&prid=2197&'
             'proj_from={:%Y-%m-%d}&proj_to=&month=false&keywords=&tz=America%252FLos_Angeles')
DATES_URL = 'http://nlds2.insinc.neulion.com/play?url=%2Fnlds%2Fcacivic%2Fcityofsurrey1%2Fas%2Flive%2Fcityofsurrey1_hd_pc'


class SurreyNeulionApi(object):
    def __init__(self):
        self.session = Session()

    def clips_for_date(self, dt):
        """
        :param date dt: In Pacific Time.
        """
        url = CLIPS_URL.format(dt)
        resp = self.session.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        parsed = []
        for tr in soup.find_all('tr'):
            onclick = tr.td.a['onclick']
            first_quote_index = onclick.find("'")
            values = {tag['name']: tag['value'] for tag in tr.td.find_all('input')}
            values['stream_url'] = onclick[first_quote_index + 1:onclick.find("'", first_quote_index + 1)]
            values['duration'] = tr.find_all('td')[-1].text.strip()
            values['name'] = tr.td.a.text.strip()
            parsed.append(values)
        return parsed

    def dates_with_video(self):
        """
        :return: Dates that *might* have video, in UTC.
        """
        resp = self.session.get(DATES_URL)
        soup = BeautifulSoup(resp.text, 'html.parser')
        ranges = soup.channel.streamdatas.streamdata.ranges
        encountered_dates = set()

        for rangetag in ranges.find_all('range'):
            encountered_dates.add(to_pacific(rangetag['begin']).date())
        return sorted(encountered_dates)


def to_pacific(utc_timestamp_str):
    pacific = pytz.timezone('America/Vancouver')
    ts = datetime.strptime(utc_timestamp_str, '%Y-%m-%d %H:%M:%S')
    ts = ts.replace(tzinfo=pytz.utc)
    return ts.astimezone(pacific)


def adaptive_stream_url_to_segment_urls(adaptive_url):
    parsed = urlparse(adaptive_url)
    quality_placeholder = 'pc_'
    newpath = parsed.path[:parsed.path.find(quality_placeholder)] + '1600'
    url = 'http://{}{}'.format(parsed.hostname, newpath)
    timeparts = parsed.path[parsed.path.find(quality_placeholder) + len(quality_placeholder):-4]
    date, start, duration = timeparts[:8], timeparts[8:timeparts.find('_')], timeparts[timeparts.find('_')+1:]
    start_time = datetime.strptime(date+start, '%Y%m%d%H%M%S')
    if start_time.second % 2 == 1:
        start_time -= timedelta(seconds=1)
    end_time = start_time + timedelta(hours=int(duration[:2]), minutes=int(duration[2:4]), seconds=int(duration[4:]))
    clip_length = timedelta(seconds=2)
    current_time = start_time
    while current_time < end_time:
        yield '{}/{:%Y%m%d/%H/%M%S}.mp4'.format(url, current_time)
        current_time += clip_length


def download_clip(session, clip_url, dest):
    print("Downloading {}".format(clip_url))
    resp = session.get(clip_url, stream=True)
    resp.raise_for_status()
    with open(dest, 'wb') as outvid:
        for chunk in resp.iter_content(chunk_size=2048):
            outvid.write(chunk)
    if not os.path.getsize(dest):
        os.remove(dest)
        raise ValueError("{} is size 0".format(clip_url))


def download_clips(adaptive_url, destination):
    if not os.path.isdir(destination):
        raise ValueError("destination must be directory")
    session = Session()
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for clip_url in adaptive_stream_url_to_segment_urls(adaptive_url):
            findpart = 'hd_1600/'
            filename = clip_url[clip_url.find(findpart) + len(findpart):].replace('/', '')
            dest = os.path.join(destination, filename)
            if os.path.exists(dest) and os.path.getsize(dest):
                # print("{} Already exists - skipping".format(dest))
                continue

            future = executor.submit(download_clip, session, clip_url, dest)
            futures.append(future)
        for future in as_completed(futures):
            try:
                future.result()
            except ValueError as e:
                print(e)


def sanitize_filename(filename):
    # ffmpeg is picky about input and output filenames.
    return filename.replace(' ', '_').replace('(', '').replace(')', '').replace(',', '')


def write_ffmpeg_concat_file(input_dir, out_filename):
    with open(out_filename, 'w') as outf:
        for clip in sorted(os.listdir(input_dir)):
            path = input_dir + '/' + clip
            outf.write("file '{}'\n".format(path))


def ffmpeg_concat(concat_file, output_file):
    # http://stackoverflow.com/questions/7333232/concatenate-two-mp4-files-using-ffmpeg
    # http://superuser.com/questions/924364/ffmpeg-how-to-convert-stereo-to-mono-using-audio-pan-filter
    print("Concatenating videos listed in {} to {} and convert to mono".format(concat_file, output_file))
    subprocess.check_call(['ffmpeg', '-loglevel', 'fatal', '-f', 'concat', '-i', concat_file,
                    '-af', 'pan=mono|c0=c0',
                    '-c:v', 'copy', output_file])


def minutes_pdf_url(metadata):
    title = metadata['name']
    start_date = to_pacific(metadata['clip_start_utc'])
    if 'RCPH' in title:
        meeting_type = 'RCPH'
    elif '(' in title and ')' in title:
        meeting_type = title[title.find('(')+1 : title.find(')')]
    elif 'Special Regular Council' in title:
        meeting_type = 'SRC'
    else:
        return None
    if start_date.day in (2, 9) and start_date.month == 5 and start_date.year == 2016 and meeting_type == 'RCPH':
        return 'http://www.surrey.ca/bylawsandcouncillibrary/MIN_RCPH_2016_05_02_and_09.pdf'
    return 'http://www.surrey.ca/bylawsandcouncillibrary/MIN_{}_{:%Y_%m_%d}.pdf'.format(meeting_type, start_date)


def set_tags(video_file, metadata):
    print("Writing video metadata")
    start_date = metadata['clip_start_utc']
    pacific = to_pacific(start_date)
    desc_title = '{} on {:%A, %B} {}, {:%Y}.'.format(metadata['name'], pacific, pacific.day, pacific)
    if metadata['clip_descr']:
        desc_title = metadata['clip_descr']
    pdf_url = minutes_pdf_url(metadata)
    if pdf_url:
        resp = requests.head(pdf_url)
        resp.raise_for_status()
        print("Meeting minutes found at " + pdf_url)
        pdf_url = 'Meeting minutes: ' + pdf_url
    else:
        print("No meeting minutes")
        pdf_url = ''

    temp = video_file + '.tmp'  # Ubuntu's AtomicParsley seems to disregard --overWrite.
    args = ['AtomicParsley', video_file, '-o', temp,
            '--title', 'Surrey {}, {:%Y-%m-%d}'.format(metadata['name'], pacific),
            '--description', '',
            '--comment', (
                desc_title + ' Surrey City Council in British Columbia, Canada. ' + pdf_url),
            '--year', start_date.replace(' ', 'T') + 'Z',
            ]
    subprocess.check_call(args)

    os.remove(video_file)
    os.rename(temp, video_file)


def extract_video_metadata(video_file):
    result = subprocess.check_output(['AtomicParsley', video_file, '-t'])
    atoms = {}
    for line in codecs.decode(result, 'utf8').splitlines():
        atpos = line.find('©')
        atoms[line[atpos + 1:line.find('"', atpos)]] = line[line.find(': ')+2:]
    return atoms


def download_and_concatenate(adaptive_url, title):
    working_name = sanitize_filename(title)
    if not os.path.exists(working_name):
        os.mkdir(working_name)
    elif not os.path.isdir(working_name):
        raise ValueError("{} exists but isn't a directory".format(working_name))
    download_clips(adaptive_url, working_name)
    concat_file = working_name + '.concat.txt'
    write_ffmpeg_concat_file(working_name, concat_file)

    video_file = title + '.mp4'
    if os.path.exists(video_file):
        print("Deleting existing {}".format(video_file))
        os.remove(video_file)

    ffmpeg_concat(concat_file, video_file)
    os.remove(concat_file)

    shutil.rmtree(working_name)

    return video_file


def youtube_upload(video_name):
    metadata = extract_video_metadata(video_name)
    desc = metadata['cmt'] + '\n\nThis is an automated re-upload of video from http://www.surrey.ca/city-government/6993.aspx.'
    subprocess.check_call([
        'youtube-upload',
        '--title={}'.format(metadata['nam']),
        '--description={}'.format(desc),
        '--tags=surrey, bc, canada, city council meeting',
        '--category=News & Politics',
        '--recording-date={}'.format(metadata['day'].replace('Z', '.0Z')),
        '--default-language=en',
        '--default-audio-language=en',
        '--privacy=public',
        '--location=latitude=49.191307,longitude=-122.848743',
        '--client-secrets=client_id.json',
        '--credentials=.youtube-upload-credentials.json',
        video_name,
    ])


def process_all_videos_for_date(date):
    api = SurreyNeulionApi()
    videos = api.clips_for_date(date)
    finished_filenames = []
    for vid in videos:
        working_name = '{:%Y%m%d}_{}'.format(date, sanitize_filename(vid['name']))
        print("Working on {}".format(working_name))
        video_name = working_name + '.mp4'
        if os.path.exists(video_name) and os.path.getsize(video_name):
            print("{} already exists".format(video_name))
            set_tags(video_name, vid)
            continue
        finished_video = download_and_concatenate(vid['stream_url'], working_name)
        set_tags(finished_video, vid)
        finished_filenames.append(finished_video)
    return finished_filenames

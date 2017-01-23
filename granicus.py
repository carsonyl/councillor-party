import json
import os
import re
from collections import namedtuple
from datetime import datetime, date
from urllib.parse import urlparse, parse_qs

import pendulum
import pytz
from bs4 import BeautifulSoup
from typing import Iterable

from common import VideoProvider, VideoMetadata, PreparedVideoInfo
from ffmpeg import ffmpeg_concat
from segment_tools import download_clip, write_ffmpeg_concat_file

GranicusVideo = namedtuple('GranicusVideo',
                           ['title', 'date', 'agenda_url', 'minutes_url', 'minutes_url_title', 'video_url'])


Streams = namedtuple('Streams', ['rtmp_url', 'm3u8_url'])


class GranicusVideoMetadata(VideoMetadata):
    def __init__(self, video_id, title, category, start_ts, url, agenda_title, agenda_url, minutes_title, minutes_url):
        super().__init__(video_id, title, category, start_ts, None, None, url)
        self.agenda_title = agenda_title
        self.agenda_url = agenda_url
        self.minutes_title = minutes_title
        self.minutes_url = minutes_url


def get_href_parts(td):
    a = td.find('a')
    if not a:
        return None, None
    return a['href'], remove_extraneous_spaces(a.string)


def get_url_from_onclick(onclick_value):
    start_val = "open('"
    start_index = onclick_value.find(start_val) + len(start_val)
    return onclick_value[start_index:onclick_value.rfind("','player'")]


def remove_extraneous_spaces(value):
    return ' '.join(value.split())


def uncommented_lines(full_text):
    for line in full_text.split('\n'):
        if line.startswith('#'):
            continue
        yield line


class GranicusScraperApi(VideoProvider):

    def __init__(self, site_url, tz='America/Vancouver'):
        """
        :param site_url: The Granicus page with the list of available videos.
        """
        super().__init__(site_url)
        self.tz = tz

    def available_dates(self, start_date: date, end_date: date) -> Iterable[pendulum.Date]:
        seen_dates = set()
        for video in self.get_videos():
            dt = pendulum.parse(video.start_ts)
            if start_date <=dt <= end_date and video.start_ts not in seen_dates:
                seen_dates.add(dt)
                yield dt.date()

    def get_metadata(self, for_date) -> Iterable[VideoMetadata]:
        for video in self.get_videos():
            if pendulum.parse(video.start_ts).date() != for_date:
                continue
            yield video

    def download(self, url, destination_dir):
        clip_guid = self.get_clip_id(url)
        streams = self.get_streams(clip_guid)
        download_clip(self.get_video_piece_urls(streams.m3u8_url), destination_dir, 16)

    def postprocess(self, video_metadata: VideoMetadata, download_dir, destination_dir, **kwargs) -> PreparedVideoInfo:
        concat_file_path = write_ffmpeg_concat_file(download_dir, None)
        video_filename = video_metadata.video_id + '.ts'
        video_path = os.path.join(destination_dir, video_filename)
        ffmpeg_concat(concat_file_path, video_path, mono=kwargs.get('mono', False))

        return PreparedVideoInfo(video_metadata, video_filename)

    def _parse_html(self, url):
        resp = self.session.get(url)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, 'html.parser')

    def get_videos(self):
        parsed_html = self._parse_html(self.provider_url)
        # The second table is the useful one.
        previous_meetings_table = parsed_html.select('table.listingTable')[1]
        for row in previous_meetings_table.find_all('tr'):
            cells = row.find_all('td')
            if not cells:
                continue
            date_value = remove_extraneous_spaces(cells[1].string).replace('.', '')
            date_value = datetime.strptime(date_value, '%b %d, %Y')
            date_value = pytz.timezone(self.tz).localize(date_value)
            agenda_url, agenda_title = get_href_parts(cells[-3])
            minutes_url, minutes_title = get_href_parts(cells[-2])

            # Remove date from title.
            title = remove_extraneous_spaces(next(cells[0].stripped_strings))
            title = re.sub(r'\s\(\w+\.? \d+, \d+\)', '', title)

            video_url = get_url_from_onclick(cells[-1].find('a')['onclick'])

            qs = parse_qs(urlparse(video_url).query)

            yield GranicusVideoMetadata(
                qs['clip_id'][0],
                title,
                title,
                date_value.isoformat(),
                video_url,
                agenda_title, agenda_url,
                minutes_title, minutes_url,
            )

    def get_clip_id(self, video_url):
        resp = self.session.get(video_url)
        resp.raise_for_status()
        match = re.search(r"\s+clipId:\s*'([\w\-]+)',", resp.text)
        return match.group(1)

    def get_streams(self, clip_id):
        parsed_site = urlparse(self.provider_url)
        streams_url = '{}://{}/player/GetStreams.php'.format(parsed_site.scheme, parsed_site.netloc)
        resp = self.session.get(streams_url, params={'clip_id': clip_id})
        js_text = resp.text.replace('\/', '/')
        js = json.loads(js_text)
        return Streams(js[0], js[1])

    def get_video_piece_urls(self, m3u8_url):
        resp = self.session.get(m3u8_url)
        resp.raise_for_status()
        dir_url_for_original_m3u8 = os.path.dirname(m3u8_url)
        next_m3u8_url = os.path.join(dir_url_for_original_m3u8, next(uncommented_lines(resp.text)))
        resp = self.session.get(next_m3u8_url)
        resp.raise_for_status()
        for ts_filename in uncommented_lines(resp.text):
            if not ts_filename.endswith('.ts'):
                continue
            yield os.path.join(dir_url_for_original_m3u8, ts_filename)

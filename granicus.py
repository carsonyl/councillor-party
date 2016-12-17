import json
import os
import re
from collections import namedtuple
from datetime import datetime
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from requests import Session

GranicusVideo = namedtuple('GranicusVideo',
                           ['title', 'date', 'agenda_url', 'minutes_url', 'minutes_url_title', 'video_url'])


Streams = namedtuple('Streams', ['rtmp_url', 'm3u8_url'])


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


class GranicusScraperApi(object):
    def __init__(self, site_url):
        """
        :param site_url: The Granicus page with the list of available videos.
        """
        self.site_url = site_url
        self.session = Session()

    def _parse_html(self, url):
        resp = self.session.get(self.site_url)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, 'html.parser')

    def get_videos(self):
        parsed_html = self._parse_html(self.site_url)
        # The second table is the useful one.
        previous_meetings_table = parsed_html.select('table.listingTable')[1]
        for row in previous_meetings_table.find_all('tr'):
            cells = row.find_all('td')
            if not cells:
                continue
            date_value = remove_extraneous_spaces(cells[1].string).replace('.', '')
            date_value = datetime.strptime(date_value, '%b %d, %Y').date()
            agenda_url, agenda_title = get_href_parts(cells[2])
            minutes_url, minutes_title = get_href_parts(cells[3])
            title = remove_extraneous_spaces(next(cells[0].stripped_strings))
            video_url = get_url_from_onclick(cells[4].find('a')['onclick'])
            yield GranicusVideo(title, date_value, agenda_url, minutes_url, minutes_title, video_url)

    def get_clip_id(self, video_url):
        resp = self.session.get(video_url)
        resp.raise_for_status()
        match = re.search(r"\s+clipId:\s*'([\w\-]+)',", resp.text)
        return match.group(1)

    def get_streams(self, clip_id):
        parsed_site = urlparse(self.site_url)
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

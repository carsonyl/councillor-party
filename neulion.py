"""
Utilities for interacting with Neulion and interpreting data retrieved from it.
"""
import os
from collections import OrderedDict
from collections import namedtuple
from datetime import datetime, timedelta, date
from typing import Iterable
from urllib.parse import urlparse

import pendulum
import pytz
from bs4 import BeautifulSoup
from requests import Session

from common import VideoProvider, VideoMetadata, group_root_and_subclips, TimeCode, PreparedVideoInfo, shift_timecodes
from ffmpeg import ffmpeg_concat
from segment_tools import download_clip, write_ffmpeg_concat_file

Project = namedtuple('Project', ['id', 'name'])
NeulionClip = namedtuple('Clip', ['url', 'title', 'rank', 'descr', 'start_utc', 'project', 'id', 'duration'])


class NeulionClipMetadata(VideoMetadata):
    def __init__(self, clip_id, project_id, rank, title, descr, clip_start_utc, url,
                 category=None, timecodes=None):
        url_parts = url.split('_')
        start_ts = url_parts[-2]
        start_ts = datetime.strptime(start_ts, '%Y%m%d%H%M%S')
        start_ts = start_ts.replace(tzinfo=pytz.utc)

        duration = url_parts[-1].replace('.mp4', '')
        duration = timedelta(hours=int(duration[0:2]), minutes=int(duration[2:4]), seconds=int(duration[4:6]))

        super().__init__(clip_id, title, category, start_ts.isoformat(), (start_ts + duration).isoformat(), timecodes, url)
        self.project_id = project_id
        self.rank = rank
        self.descr = descr
        self.clip_start_utc = clip_start_utc
        self.duration = duration


class NeulionScraperApi(VideoProvider):
    """
    Methods for discovering available videos.
    """

    def __init__(self, site_url, tz='America/Vancouver'):
        """
        :param site_url: URL of the Neulion Civic Streaming page to scrape.
        """
        super().__init__(site_url)
        self.tz = tz
        self._site_soup = None
        self.session = Session()

    def available_dates(self, start_date: date, end_date: date) -> Iterable[pendulum.Date]:
        for available_date in self.allowed_dates():
            if start_date <= available_date <= end_date:
                yield pendulum.Date.instance(available_date)

    def get_metadata(self, for_date) -> Iterable[VideoMetadata]:
        projects = OrderedDict()
        all_meetings_project = None
        for project in self.projects():
            if not all_meetings_project:
                all_meetings_project = project
            projects[project.id] = project

        clips = list(self.clips(for_date, all_meetings_project.id))
        clips.sort(key=lambda clip: clip.start_ts)
        for root, subclips in group_root_and_subclips(clips).items():
            root.category = projects[root.project_id].name
            timecodes = [TimeCode(pendulum.parse(c.start_ts).strftime('%H:%M:%S'), c.title, pendulum.parse(c.end_ts).strftime('%H:%M:%S')) for c in subclips]
            root.timecodes = timecodes
            yield root

    def download(self, url, destination_dir):
        download_clip(adaptive_url_to_segment_urls(url), destination_dir, 16)

    def postprocess(self, video_metadata: VideoMetadata, download_dir, destination_dir, **kwargs) -> PreparedVideoInfo:
        concat_file_path = write_ffmpeg_concat_file(download_dir, 2)
        video_filename = video_metadata.video_id + '.mp4'
        video_path = os.path.join(destination_dir, video_filename)
        ffmpeg_concat(concat_file_path, video_path, mono=kwargs.get('mono', False))

        shift_timecodes(video_metadata.timecodes, pendulum.parse(video_metadata.start_ts).strftime('%H:%M:%S'))
        return PreparedVideoInfo(video_metadata, video_filename)

    def _get_site_html(self):
        if not self._site_soup:
            resp = self.session.get(self.provider_url)
            resp.raise_for_status()
            self._site_soup = BeautifulSoup(resp.text, 'html.parser')
        return self._site_soup

    def projects(self):
        """
        Get the projects, also known as meeting categories.
        The first element should be a special entry that includes all categories.
        """
        soup = self._get_site_html()
        for option in soup.find(id='projectsSelector').find_all('option'):
            yield Project(option['value'], option.text.replace(';', ''))

    def allowed_dates(self):
        """
        Get the dates that are allowed to be picked in the video browser calendar,
        i.e. the dates that have videos available.
        """
        soup = self._get_site_html()
        search_term = 'SEARCH_VARS.allowedDates = ['
        for script in soup.find_all('script'):
            js_body = script.text
            start_index = js_body.find(search_term)
            end_index = js_body.find(']', start_index)
            if start_index == -1:
                continue
            for element in js_body[start_index + len(search_term) + 1:end_index].split(','):
                element = element.replace('"', '').strip()
                yield datetime.strptime(element, '%Y-%m-%d').date()
            break

    def clips(self, for_date: date, project_ids):
        """
        Get the video clips available for a given date and project.

        :param for_date: Date for which to obtain videos.
        :param project_ids: List of project IDs, or a single string to pass as-is.
        """
        if not isinstance(project_ids, str):
            project_ids = ','.join(project_ids)
        resp = self.session.get('http://civic.neulion.com/api/clipmanager.php', params={
            'f': 'getClips',
            'device': 'desktop',
            'prid': project_ids,
            'proj_from': pendulum.Date.instance(for_date).to_date_string(),
            'tz': self.tz,
        })
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for tr in soup.find_all('tr'):
            a = tr.find('a')
            url = a['onclick']
            url = url[url.find('initClip(')+10:url.find('.mp4') + 4]
            hidden_vals = {}
            for inp in tr.find_all('input'):
                hidden_vals[inp['name']] = inp['value']

            # This value lies: it's usually fixed to nearest hour and at start of entire meeting.
            start_utc = pendulum.parse(hidden_vals['clip_start_utc'])
            start_utc = start_utc.replace(tzinfo=self.tz)  # Not actually UTC.

            # Fix glitched titles by swapping in description that looks like title (Vancouver 2015-2-24).
            title = ' '.join(a.text.strip().split())
            clip_descr = hidden_vals['clip_descr']
            if title.startswith('adaptive://') and clip_descr:
                title = clip_descr

            yield NeulionClipMetadata(
                hidden_vals['clip_id'],
                hidden_vals['clip_project'],
                hidden_vals['clip_rank'],
                title,
                clip_descr,
                start_utc.isoformat(),
                url,
            )


def parse_time_range_from_url(adaptive_url):
    """
    Parse the time information available in a video URL.

    :param adaptive_url: Video URL, which contains time info.
    :return: Tuple of start time, end time, and duration
    """
    filename = adaptive_url.split('/')[-1]
    filename = filename.replace('.mp4', '')
    find_part = '_pc_'
    ts_part = filename[filename.find(find_part) + len(find_part):]
    start_ts, duration = ts_part.split('_')
    start_ts = datetime.strptime(start_ts, '%Y%m%d%H%M%S')
    start_ts = start_ts.replace(tzinfo=pytz.utc)
    duration = timedelta(hours=int(duration[:2]), minutes=int(duration[2:4]), seconds=int(duration[4:]))
    end_ts = start_ts + duration
    return start_ts, end_ts, duration


def group_video_clips(clips):
    """
    Group a set of video clips for a given date into root clips, and subclips within these root clips (if any).

    :param clips: List of clips.
    :return: Ordered dict where keys are root clips, and values are lists of subclips for it.
    """
    # Hack for when 'entire meeting' is incorrectly sized. (Burnaby 2016-07-11)
    first_clip = clips[0]
    _, _, duration = parse_time_range_from_url(first_clip.url)
    if len(clips) > 1 and first_clip.name == 'Entire Council Meeting' and duration < timedelta(seconds=5):
        return group_all_clips_under_first_clip(clips)

    def root_clip_by_name(clip):
        return clip.name.startswith('Regular Council') or clip.name.startswith('Public Hearing') \
               or 'Whole Meeting' in clip.name or 'Entire Meeting' in clip.name \
               or clip.name.startswith('Planning, Transportation') or clip.name.startswith('Policy &') \
               or clip.name.startswith('Complete Standing Committee')

    candidate_root_clips = list(filter(root_clip_by_name, clips))
    clips = candidate_root_clips + list(filter(lambda x: not root_clip_by_name(x), clips))

    sorted_clips = OrderedDict()
    for clip in clips:
        clip_start, clip_end, _ = parse_time_range_from_url(clip.url)
        is_subclip = False
        for root_clip in sorted_clips:
            root_start, root_end, _ = parse_time_range_from_url(root_clip.url)
            # Workaround for some subclips being slightly outside of a root clip's start time.
            if clip_start < root_start and abs((clip_start - root_start).total_seconds()) < 2:
                clip_start = root_start
            if root_start <= clip_start <= root_end:
                sorted_clips[root_clip].append(clip)
                is_subclip = True
                break
        if not is_subclip:
            sorted_clips[clip] = []
    return sorted_clips


def group_all_clips_under_first_clip(clips):
    root_clip, last_clip = clips[0], clips[-1]
    root_start, _, root_duration = parse_time_range_from_url(root_clip.url)
    final_start, final_end, final_duration = parse_time_range_from_url(last_clip.url)
    new_root_duration = duration_to_timecode(final_end - root_start).replace(':', '')
    new_root_clip_url = root_clip.url.replace(duration_to_timecode(root_duration).replace(':', ''), new_root_duration)
    root_clip = root_clip._replace(url=new_root_clip_url)
    return {root_clip: clips[1:]}


def duration_to_timecode(delta):
    hours = delta.seconds // 3600
    return "{:02d}:{:02d}:{:02d}".format(hours, (delta.seconds - (hours * 3600)) // 60, delta.seconds % 60)


def timecode_to_duration(code):
    code = list(map(int, code.split(':')))
    return code[0] * 3600 + code[1] * 60 + code[2]


def calculate_timecodes(root_clip, subclips):
    """
    For a given root clip, calculate the time code offsets into the video for all of its subclips.

    :param root_clip: The root clip.
    :param subclips: The subclips within the root clip.
    :return: Ordered dict where keys are string time codes and values are the subclips.
    """
    root_start, _, _ = parse_time_range_from_url(root_clip.url)
    timecodes = OrderedDict()
    for clip in subclips:
        clip_start, _, _ = parse_time_range_from_url(clip.url)
        # Workaround for some subclips being slightly outside of a root clip's start time.
        if clip_start < root_start:
            clip_start = root_start
        timecodes[duration_to_timecode(clip_start - root_start)] = clip
    return timecodes


def adaptive_url_to_segment_urls(adaptive_url):
    parsed = urlparse(adaptive_url)
    quality_placeholder = 'pc_'
    newpath = parsed.path[:parsed.path.find(quality_placeholder)] + '1600'
    url = 'http://{}{}'.format(parsed.hostname, newpath)
    start_ts, end_ts, _ = parse_time_range_from_url(adaptive_url)
    if start_ts.second % 2 == 1:
        start_ts -= timedelta(seconds=1)
    clip_length = timedelta(seconds=2)
    current_time = start_ts
    while current_time < end_ts:
        yield '{}/{:%Y%m%d/%H/%M%S}.mp4'.format(url, current_time)
        current_time += clip_length

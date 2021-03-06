import abc
import re
from collections import OrderedDict
from copy import copy
from datetime import date
from typing import Iterable, List

import pendulum
import yaml
from requests import Session


def get_value_in_delim(in_val, start_delim='(', end_delim=')'):
    return in_val[in_val.find(start_delim) + len(start_delim):in_val.rfind(end_delim)]


class VideoMetadata(yaml.YAMLObject):
    yaml_tag = '!VideoMetadata'

    def __init__(self, video_id=None, title=None, category=None, start_ts=None, end_ts=None, timecodes=None, url=None):
        if not timecodes:
            timecodes = []
        self.video_id = video_id
        self.category = category
        self.title = title
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.timecodes = timecodes
        self.url = url

    def __str__(self):
        return "{}: '{}' ({})".format(self.category, self.title, pendulum.parse(self.start_ts).isoformat())


class TimeCode(yaml.YAMLObject):
    yaml_tag = '!TimeCode'

    def __init__(self, start_ts, title, end_ts=None):
        self.start_ts = start_ts
        self.title = title
        self.end_ts = end_ts


class PreparedVideoInfo(yaml.YAMLObject):
    def __init__(self, video_metadata, video_filename, title='', description='', playlists=None, config_id=None):
        self.video_metadata = video_metadata
        self.video_filename = video_filename
        self.title = title
        self.description = description
        self.playlists = playlists if playlists else []
        self.config_id = config_id


class VideoProvider(object, metaclass=abc.ABCMeta):

    def __init__(self, provider_url):
        self.provider_url = provider_url
        self.session = Session()

    @abc.abstractmethod
    def available_dates(self, start_date: date, end_date: date) -> Iterable[pendulum.Date]:
        pass

    @abc.abstractmethod
    def get_metadata(self, for_date) -> Iterable[VideoMetadata]:
        pass

    @abc.abstractmethod
    def download(self, url, destination_dir):
        pass

    @abc.abstractmethod
    def postprocess(self, video_metadata: VideoMetadata, download_dir, destination_dir, **kwargs) -> PreparedVideoInfo:
        pass


def timecode_to_seconds(timecode):
    return int(timecode[0:2]) * (60*60) + int(timecode[3:5]) * 60 + int(timecode[6:8])


def adjust_timecode(timecode, seconds):
    time_s = timecode_to_seconds(timecode)
    time_s += seconds
    if time_s < 0:
        return '00:00:00'
    h = time_s // (60*60)
    m = (time_s - h*60*60) // 60
    s = time_s % 60
    return '{:02d}:{:02d}:{:02d}'.format(h, m, s)


def shift_timecodes(timecodes: List[TimeCode], relative_to: str):
    shift_timecode_s = timecode_to_seconds(relative_to)
    for timecode in timecodes:
        timecode.start_ts = adjust_timecode(timecode.start_ts, -shift_timecode_s)
        timecode.end_ts = adjust_timecode(timecode.end_ts, -shift_timecode_s)


def yaml_dump(obj, file_path, width=120):
    with open(file_path, 'w') as outf:
        yaml.dump(obj, outf, width=width)


def yaml_load(file_path):
    with open(file_path) as inf:
        return yaml.load(inf)


def build_substitutions_dict(video_metadata: VideoMetadata):
    obj = video_metadata.__dict__.copy()
    obj['start_ts'] = pendulum.parse(obj['start_ts']).in_tz('America/Vancouver')
    obj['timecodes'] = '\n'.join('{} - {}'.format(x.start_ts, x.title) for x in video_metadata.timecodes).strip()
    return obj


def tweak_metadata(config_id, metadata: VideoMetadata):
    if config_id == 'coquitlam':
        category_parts = metadata.category.split(' / ')
        for part in category_parts:
            found_part = None
            for timecode in metadata.timecodes:
                look_for = part.replace(' Meeting', '')
                if look_for == 'Regular Council' and timecode.title.startswith('RC '):
                    found_part = part
                elif look_for == 'Public Hearing' and timecode.title.startswith('PH '):
                    found_part = part
                elif timecode.title.startswith(look_for):
                    timecode.title = re.sub(r'^%s( Meeting)?( - )?' % look_for, '', timecode.title)
                    found_part = part
            if found_part:
                return {'category': found_part}
    return {}


def is_root_clip(clip_title, also_allow_startswith=None):
    clip_title = clip_title.lower()
    if also_allow_startswith and clip_title.startswith(also_allow_startswith):
        return True
    if clip_title in ('webcast unavailable', 'archive unavailable', 'inaugural council meeting',
                      ):
        return True
    # This section often needs to be tweaked to handle creative variants of root clip names.
    for keyword in ('regular council - ', 'regular council ',
                    'complete council ', 'entire council ', 'inaugural council ',
                    'edited entire', 'whole ', 'entire ', 'full ', 'special council '):
        if clip_title.startswith(keyword) or keyword + 'meeting' in clip_title:
            if 'minutes' in clip_title and 'audio' not in clip_title and 'sound' not in clip_title:
                return False
            return True
    return False


def group_root_and_subclips(clips: List[VideoMetadata]):
    grouped = OrderedDict()
    current_root = None
    # If video clips are ordered incorrectly, break here to reorder them before proceeding.
    for clip in clips:
        if is_root_clip(clip.title):
            current_root = clip
            grouped[current_root] = []
        else:
            try:
                grouped[current_root].append(clip)
            except KeyError:
                print(clip)
                if clip.title.endswith('session)') or (len(clips) == 1 and (clip.category == 'Other' or clip.title == 'committee')) \
                        or clip.title.startswith('Opening Remarks') or 'Call to Order' in clip.title:
                    # Continuation of existing session. Hack up a root clip for it.
                    artificial_root = copy(clip)
                    current_root = artificial_root
                    grouped[current_root] = [current_root]
                    continue
                raise
    return grouped

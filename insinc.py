import os
import re
from collections import OrderedDict

from datetime import date, timedelta, datetime
from itertools import groupby, chain
from operator import itemgetter

import pendulum
import yaml
from bs4 import BeautifulSoup
from copy import copy

from common import VideoProvider, VideoMetadata, TimeCode, adjust_timecode, timecode_to_seconds, PreparedVideoInfo
from ffmpeg import download_mms, clip_video


class InsIncVideoClip(object):
    def __init__(self, category, title, mms_url, for_date, start_time, end_time):
        self.category = category
        self.title = title
        self.mms_url = mms_url
        self.for_date = for_date
        self.start_time = start_time
        self.end_time = end_time

    def __str__(self):
        return "{}: '{} ({:%Y-%m-%d} at {})'".format(self.category, self.title, self.for_date, self.start_time)


def timestamp_to_timedelta(val):
    return timedelta(hours=int(val[0:2]), minutes=int(val[3:5]), seconds=int(val[6:]))


class InsIncScraperApi(VideoProvider):

    def __init__(self, search_url, tz='America/Vancouver'):
        super().__init__(search_url)
        self.tz = tz

    def available_dates(self, start_date: date, end_date: date):
        """
        Get dates with videos available, within the given date range (inclusive).

        Note that the server may respond oddly: a date returned by this method may yield no videos
        when used as an argument to :meth:`get_metadata`. This implies that there actually is a recording
        for that date, but it's grouped under some nearby previous date.
        There may also not actually be any recordings for that date, but a clip was mis-dated on the server.
        """
        first_of_month = pendulum.Date.instance(start_date).replace(day=1)
        while first_of_month < end_date:
            for available_date in self.get_available_dates(first_of_month.year, first_of_month.month):
                if available_date < start_date or available_date > end_date:
                    continue
                yield available_date
            first_of_month = first_of_month.add(months=1)

    def get_metadata(self, for_date):
        """
        Note that querying for video metadata on a particular date may yield video clips dated for other days.
        Their video MMS URLs will match the specified date, but their actual dates can differ.
        """
        for mms_url, clips in group_clips(self.get_clips(for_date)).items():
            clips = list(clips)
            if clips[0].title.startswith('Due to Technical Difficulties'):
                continue
            for root, subclips in group_root_and_subclips(clips).items():
                start_ts = pendulum.combine(root.for_date, pendulum.parse(root.start_time).time()).tz_(self.tz)
                timecodes = [TimeCode(c.start_time, c.title, c.end_time) for c in subclips]
                if not timecodes:
                    timecodes.append(TimeCode(root.start_time, root.title, root.end_time))
                yield VideoMetadata(
                    video_id=os.path.basename(root.mms_url).replace('.wmv', ''),
                    category=root.category,
                    title=root.title,
                    url=root.mms_url,
                    start_ts=start_ts.isoformat(),
                    end_ts=root.end_time,
                    timecodes=timecodes,
                )

    def download(self, mms_url, destination_dir):
        dest_file_path = os.path.join(destination_dir, os.path.basename(mms_url))
        if os.path.exists(dest_file_path):
            print("Already exists: " + dest_file_path)
            return

        start_time = datetime.now()
        print("Starting download of {} on {}".format(mms_url, start_time.isoformat()))
        download_mms(mms_url, dest_file_path)
        end_time = datetime.now()
        elapsed = end_time - start_time
        print("Download of {} completed on {} in {} seconds".format(
            mms_url, end_time.isoformat(), elapsed.total_seconds()))

    def postprocess(self, video_metadata, download_dir, destination_dir):
        filename_from_video_url = os.path.basename(video_metadata.url)
        video_path = os.path.join(download_dir, filename_from_video_url)
        if not os.path.exists(video_path):
            raise ValueError("{} doesn't exist".format(video_path))
        start_timestamp = pendulum.parse(video_metadata.start_ts)
        start_timecode = min(chain([m.start_ts for m in video_metadata.timecodes], [start_timestamp.to_time_string()]))
        end_timecode = max(chain([m.end_ts for m in video_metadata.timecodes], [video_metadata.end_ts]))
        start_timecode = adjust_timecode(start_timecode, -2)
        end_timecode = adjust_timecode(end_timecode, 2)

        shift_timecode_s = timecode_to_seconds(start_timecode)
        for timecode in video_metadata.timecodes:
            timecode.start_ts = adjust_timecode(timecode.start_ts, -shift_timecode_s)
            timecode.end_ts = adjust_timecode(timecode.end_ts, -shift_timecode_s)

        filename_parts = filename_from_video_url.split('.')
        filename_parts.insert(len(filename_parts)-1, '{}_{}'.format(
            start_timecode.replace(':', ''), end_timecode.replace(':', '')))
        final_video_filename = '.'.join(filename_parts)
        dest_file = os.path.join(destination_dir, final_video_filename)
        prepped_video_info = PreparedVideoInfo(video_metadata, final_video_filename)
        with open(dest_file + '.yaml', 'w') as outf:
            yaml.dump(prepped_video_info, outf)

        if os.path.exists(dest_file):
            print(dest_file + " already exists")
        else:
            clip_video(video_path, start_timecode, end_timecode, dest_file)

        return prepped_video_info

    def _search(self, url, rs, rsargs):
        resp = self.session.post(url, data={
            'rs': rs,
            'rsargs[]': rsargs,
        })
        resp.raise_for_status()
        return resp

    def get_available_dates(self, year, month):
        """
        Get dates for which videos are available. Dates are in local time.
        """
        print("Getting available dates in {}-{}".format(year, month))
        resp = self._search(self.provider_url + '/meeting_search.php', 'show_calendar', [year, str(month).zfill(2)])
        for match in re.finditer(r"javascript: write_date_string\(\\'(\d+)-(\d+)-(\d+)\\'\)", resp.text):
            y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
            dt = pendulum.Date(y, m, d)
            yield dt

    def get_clips(self, for_date: date):
        """
        Get all available video clips for the given local date.
        """
        # The '_sl' suffix yields mms:// URLs.
        resp = self._search(self.provider_url + '/meeting_search_sl.php',
                            'search_clips_sl', ['', for_date.strftime('%Y-%m-%d'), ''])
        start_bit, end_bit = "+:var res = { \"result\": '", "'}; res;"
        body = '"{}"'.format(resp.text[len(start_bit):-1 - len(end_bit)])
        body = body.replace("\\n", "\n").replace("\\'", "'").replace('\\"', '"')
        parsed_html = BeautifulSoup(body, 'html.parser')
        category = None
        for element in parsed_html.select('td.gameDate'):
            strong, a_link = element.find('strong'), element.find('a')
            if strong:
                category = str(strong.string).strip()
            elif a_link:
                # Back up to previous <td> and grab the date.
                # Asking for videos on a particular date may yield videos that are for nearby dates,
                # but on the same date according to the video URL.
                actual_date = str(list(element.previous_siblings)[1].string).strip()
                actual_date = pendulum.parse(actual_date).date()

                href = a_link['href']
                match = re.match(
                    r"javascript:reload_media_sl\('(mms://[\w\-./]+)', '(\d+:\d+:\d+)', '(\d+:\d+:\d+)'\)", href)
                if not match:
                    continue
                mms_url, start_time, end_time = match.group(1), match.group(2), match.group(3)
                if start_time == '41:09:00':
                    start_time = '00:41:09'
                title = str(element.string).strip()
                yield InsIncVideoClip(category, title, mms_url, actual_date, start_time, end_time)


def is_root_clip(clip_title, also_allow_startswith=None):
    clip_title = clip_title.lower()
    if also_allow_startswith and clip_title.startswith(also_allow_startswith):
        return True
    if clip_title in ('webcast unavailable', 'archive unavailable', 'inaugural council meeting', 'public hearing'):
        return True
    for keyword in ('edited entire', 'whole ', 'entire ', 'full ', 'special council '):
        if clip_title.startswith(keyword) or keyword + 'meeting' in clip_title:
            if 'minutes' in clip_title and 'audio' not in clip_title and 'sound' not in clip_title:
                return False
            return True
    return False


def group_clips(clips) -> dict:
    groups = OrderedDict()
    for mms_url, grouped_clips in groupby(clips, key=lambda clip: clip.mms_url):
        # First, break any ties with root clip start times. Ensure root clips come first.
        grouped_clips = list(grouped_clips)
        # for i, clip in enumerate(clips):
        #     if is_root_clip(clip.title) and i != 0:
        #         clip.start_time = adjust_timecode(clips[i-1].start_time, -2)
        if is_root_clip(grouped_clips[0].title):
            ordered_clips = [grouped_clips[0]]
            ordered_clips[1:] = sorted(grouped_clips[1:], key=lambda clip: clip.start_time)
        else:
            ordered_clips = sorted(grouped_clips, key=lambda clip: clip.start_time)

        if len(ordered_clips) > 1 and not is_root_clip(ordered_clips[0].title):
            if is_root_clip(ordered_clips[1].title, 'opening remarks'):
                ordered_clips[0].start_time, ordered_clips[1].start_time = ordered_clips[1].start_time, ordered_clips[0].start_time
            elif is_root_clip(ordered_clips[-1].title):
                ordered_clips[-1].start_time = adjust_timecode(ordered_clips[0].start_time, -1)
            elif is_root_clip(ordered_clips[-2].title):
                ordered_clips[-2].start_time = adjust_timecode(ordered_clips[0].start_time, -1)
            ordered_clips = sorted(grouped_clips, key=lambda clip: clip.start_time)

        dupes_removed = [ordered_clips[0]]
        for i, clip in enumerate(ordered_clips[1:], start=1):
            if clip.title == ordered_clips[i-1].title:
                continue
            dupes_removed.append(clip)

        groups[mms_url] = dupes_removed
    return groups


def group_root_and_subclips(clips):
    grouped = OrderedDict()
    current_root = None
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

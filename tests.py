import pytest
import pytz
from collections import OrderedDict
from vcr import VCR
from datetime import datetime, date, timedelta

from concat import adjust_timecodes_for_missing_segments
from config import get_config, get_all_configs
from neulion import NeulionScraperApi, parse_time_range_from_url, group_video_clips, calculate_timecodes, \
    adaptive_url_to_segment_urls

myvcr = VCR(
    cassette_library_dir='cassettes',
    path_transformer=VCR.ensure_suffix('.yaml'),
)

SURREY_URL = 'http://civic.neulion.com/cityofsurrey/'
BURNABY_URL = 'http://civic.neulion.com/cityofburnaby/'
VANCOUVER_URL = 'http://civic.neulion.com/cityofvancouver/'
LANGLEY_URL = 'http://civic.neulion.com/cityoflangley/'
ALL_SITE_URLS = (SURREY_URL, BURNABY_URL, VANCOUVER_URL, LANGLEY_URL)


@pytest.mark.parametrize('site_url', ALL_SITE_URLS)
def test_get_projects(site_url):
    with myvcr.use_cassette(site_url.split('/')[-2]):
        api = NeulionScraperApi(site_url)
        projects = list(api.projects())
        assert len(projects) > 0
        assert projects[0].name == 'All Meetings'


@pytest.mark.parametrize('site_url', ALL_SITE_URLS)
def test_get_allowed_dates(site_url):
    with myvcr.use_cassette(site_url.split('/')[-2]):
        api = NeulionScraperApi(site_url)
        dates = list(api.allowed_dates())
        assert len(dates) > 0


@pytest.mark.parametrize('site_url,for_date', [
    (SURREY_URL, date(2016, 7, 25)),
    (BURNABY_URL, date(2016, 7, 25)),
    (VANCOUVER_URL, date(2016, 7, 26)),
    (LANGLEY_URL, date(2016, 7, 25)),
])
def test_get_clips(site_url, for_date):
    cassette_name = '{}_clips_{:%Y%m%d}'.format(site_url.split('/')[-2], for_date)
    with myvcr.use_cassette(cassette_name):
        api = NeulionScraperApi(site_url)
        all_projects = next(api.projects())
        clips = list(api.clips(for_date, all_projects.id))
        assert len(clips) > 0
        for clip in clips:
            assert clip.url.startswith('adaptive://')
            assert clip.url.endswith('.mp4')
            assert len(clip.name) > 0


@pytest.mark.parametrize('url,expected_start,expected_duration', [
    ('adaptive://nlds2.insinc.neulion.com:443/nlds/cacivic/cityofvan1/as/live/cityofvan1_hd_pc_20160726163229_023233.mp4',
     datetime(2016, 7, 26, 16, 32, 29, tzinfo=pytz.utc), timedelta(hours=2, minutes=32, seconds=33)),
])
def test_parse_time_range_from_url(url, expected_start, expected_duration):
    start_ts, end_ts, duration = parse_time_range_from_url(url)
    assert start_ts == expected_start
    assert duration == expected_duration
    assert end_ts == start_ts + duration


@pytest.mark.parametrize('site_url,for_date,expected_root_clips,expected_subclips_for_first_root', [
    (SURREY_URL, date(2016, 7, 25), 2, 0),
    (BURNABY_URL, date(2016, 7, 25), 1, 9),
    (BURNABY_URL, date(2016, 7, 11), 1, 6),  # Has an incorrectly sized clip for entire meeting.
    (VANCOUVER_URL, date(2016, 7, 26), 2, 4),
    (LANGLEY_URL, date(2016, 7, 25), 1, 21),
])
def test_group_video_clips(site_url, for_date, expected_root_clips, expected_subclips_for_first_root):
    cassette_name = '{}_clips_{:%Y%m%d}'.format(site_url.split('/')[-2], for_date)
    with myvcr.use_cassette(cassette_name):
        api = NeulionScraperApi(site_url)
        all_projects = next(api.projects())
        clips = list(api.clips(for_date, all_projects.id))
    grouped_clips = group_video_clips(clips)
    assert len(grouped_clips) == expected_root_clips
    first_root_clip = list(grouped_clips.keys())[0]
    assert len(grouped_clips[first_root_clip]) == expected_subclips_for_first_root


def test_calculate_timecodes():
    site_url = BURNABY_URL
    for_date = datetime(2016, 7, 25)
    cassette_name = '{}_clips_{:%Y%m%d}'.format(site_url.split('/')[-2], for_date)
    with myvcr.use_cassette(cassette_name):
        api = NeulionScraperApi(site_url)
        all_projects = next(api.projects())
        clips = list(api.clips(for_date, all_projects.id))
    grouped_clips = group_video_clips(clips)
    root_clip = list(grouped_clips.keys())[0]
    timecodes = calculate_timecodes(root_clip, grouped_clips[root_clip])
    for timecode, clip in timecodes.items():
        timecodes[timecode] = clip.name
    assert isinstance(timecodes, OrderedDict)
    expected = OrderedDict()
    expected['00:00:01'] = 'Call to Order'
    expected['00:00:22'] = 'Proclamation'
    expected['00:02:25'] = 'Minutes'
    expected['00:02:37'] = "Delegations & Manager's Report Item 2"
    expected['00:24:24'] = 'Correspondence'
    expected['00:24:41'] = 'Reports'
    expected['01:30:49'] = 'Bylaws'
    expected['01:32:08'] = 'New Business/Inquiries'
    expected['01:43:57'] = 'Adjournment'
    assert timecodes == expected


def test_adaptive_url_to_segment_urls():
    adaptive_url = 'adaptive://nlds2.insinc.neulion.com:443/nlds/cacivic/cityofsurrey1/as/live/cityofsurrey1_hd_pc_20160712020109_020148.mp4'
    urls = list(adaptive_url_to_segment_urls(adaptive_url))
    assert len(urls) == 3655
    assert urls[0] == 'http://nlds2.insinc.neulion.com/nlds/cacivic/cityofsurrey1/as/live/cityofsurrey1_hd_1600/20160712/02/0108.mp4'
    assert urls[-1] == 'http://nlds2.insinc.neulion.com/nlds/cacivic/cityofsurrey1/as/live/cityofsurrey1_hd_1600/20160712/04/0256.mp4'


def test_load_all_configs():
    configs = list(get_all_configs())
    assert len(configs) > 0
    for config in configs:
        assert 'id' in config
        assert 'url' in config
        assert 'youtube' in config


def test_load_config():
    config = get_config('surrey')
    assert 'youtube' in config


def test_adjust_timecodes_for_missing_segments():
    start_ts = datetime(2016, 1, 1, 8, 0)
    missing_ts = [datetime(2016, 1, 1, 8, 1, 30)]
    timecodes = [
        {'time': '00:01:00'},
        {'time': '00:02:00'},
    ]
    num_missed_seconds = adjust_timecodes_for_missing_segments(start_ts, missing_ts, timecodes)
    assert num_missed_seconds == 2
    assert timecodes[0]['time'] == '00:01:00'
    assert timecodes[1]['time'] == '00:01:58'

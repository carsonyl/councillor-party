
from vcr import VCR
from datetime import datetime, date

from api import SurreyNeulionApi, adaptive_stream_url_to_segment_urls

myvcr = VCR(
    cassette_library_dir='cassettes',
    path_transformer=VCR.ensure_suffix('.yaml'),
)


@myvcr.use_cassette()
def test_parse_clipmanager_response():
    api = SurreyNeulionApi()
    clips = api.clips_for_date(datetime(2016, 6, 13))
    assert len(clips) == 3
    for clip in clips:
        assert 'stream_url' in clip
        assert 'duration' in clip


def test_adaptive_stream_urls():
    adaptive_url = 'adaptive://nlds2.insinc.neulion.com:443/nlds/cacivic/cityofsurrey1/as/live/cityofsurrey1_hd_pc_20160712020109_020148.mp4'
    urls = list(adaptive_stream_url_to_segment_urls(adaptive_url))
    assert len(urls) > 0


@myvcr.use_cassette()
def test_dates_with_video():
    api = SurreyNeulionApi()
    dates = api.dates_with_video()
    assert dates[0] == date(2014, 3, 31)
    assert dates[-2] == date(2016, 6, 27)  # 28th in UTC.
    assert dates[-1] == date(2016, 7, 11)

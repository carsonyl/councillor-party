import vcr
from datetime import date

from insinc import InsIncScraperApi, group_clips

api = InsIncScraperApi('http://coquitlam.insinc.com')


@vcr.use_cassette()
def test_get_available_dates():
    available_dates = list(api.get_available_dates(2016, 11))
    assert len(available_dates) > 0


@vcr.use_cassette()
def test_get_clips():
    clips = list(api.get_clips(date(2016, 12, 12)))
    assert len(clips) > 0
    for clip in clips:
        print(clip)
    grouped = group_clips(clips)
    assert len(grouped) == 2

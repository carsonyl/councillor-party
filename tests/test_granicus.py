import requests

from granicus import GranicusScraperApi

SURREY_URL = 'http://surrey.ca.granicus.com/ViewPublisher.php?view_id=1'


api = GranicusScraperApi(SURREY_URL)


def test_get_videos():
    for video in api.get_videos():
        print(video)
        assert '<' not in video.title
        assert video.agenda_url is None or video.agenda_url.startswith('http')
        assert video.minutes_url is None or video.minutes_url.startswith('http')
        assert video.url.startswith('http')


def test_get_streams():
    clip_id = api.get_clip_id('http://surrey.ca.granicus.com/MediaPlayer.php?view_id=1&clip_id=38')
    print("Clip ID: " + clip_id)
    assert clip_id
    streams = api.get_streams(clip_id)
    print(streams)
    assert streams.rtmp_url.startswith('rtmp://')
    assert streams.m3u8_url.startswith('http') and streams.m3u8_url.endswith('.m3u8')


def test_get_video_piece_urls():
    m3u8_url = 'http://207.7.130.55:1935/OnDemand/_definst_/mp4:surrey/surrey_a4812f4a-16bc-4c51-a825-24930e9582ca.mp4/playlist.m3u8'
    for i, url in enumerate(api.get_video_piece_urls(m3u8_url)):
        assert url.startswith('http')
        if i < 3:
            assert requests.head(url).ok

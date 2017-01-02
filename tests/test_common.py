import pytest

from common import adjust_timecode
from ffmpeg import tempfile_suffix


@pytest.mark.parametrize('timecode,adjustment,expected', [
    ('01:01:01', 60, '01:02:01'),
    ('00:00:01', -10, '00:00:00'),
])
def test_adjust_timecode(timecode, adjustment, expected):
    assert adjust_timecode(timecode, adjustment) == expected


def test_tempfile_suffix():
    assert tempfile_suffix('/a/b/c.wmv') == '/a/b/c.tmp.wmv'
    assert tempfile_suffix('/a/b/c.mp4') == '/a/b/c.tmp.mp4'

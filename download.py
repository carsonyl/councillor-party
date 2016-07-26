import argparse
from datetime import datetime

from api import process_all_videos_for_date

parser = argparse.ArgumentParser(description='Surrey City Council video downloader')
parser.add_argument('date')

if __name__ == '__main__':
    args = parser.parse_args()
    date = datetime.strptime(args.date, '%Y-%m-%d')
    process_all_videos_for_date(date)

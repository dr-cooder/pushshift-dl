#!/usr/bin/env python

# TODO: Auto-install modules
import argparse
from calendar import monthrange
from datetime import datetime
import html
import json
import os
import re
import requests
import subprocess
import time
import yt_dlp
import zstandard

try:
    from urllib.parse import unquote, urljoin, urlsplit
except ImportError: # Python 2
    from urlparse import unquote, urljoin, urlsplit

try: # Python 2
    from os import getcwdu as getcwd
except ImportError:
    from os import getcwd

REDARCS_SUBMISSIONS_FILE_ENDING = '_submissions.zst'
REDARCS_SUBMISSIONS_FILE_ENDING_LENGTH = len(REDARCS_SUBMISSIONS_FILE_ENDING)
SLEEP_SECONDS_INTERVAL = 5

# https://en.wikipedia.org/wiki/Reddit
REDDIT_LAUNCH_DAY = 23
REDDIT_LAUNCH_MONTH = 6
REDDIT_LAUNCH_YEAR = 2005

def chain_get(parent, *keys):
    child = parent
    try:
        for key in keys:
            child = child[key]
    except:
        child = None
    return child

def prepare_day_folders(download_basedirname, year):
    if year < REDDIT_LAUNCH_YEAR:
        return
    it_is_reddits_launch_year_my_dudes = year == REDDIT_LAUNCH_YEAR
    if it_is_reddits_launch_year_my_dudes:
        start_month = REDDIT_LAUNCH_MONTH
    else:
        start_month = 1
    for month in range(start_month, 13):
        if it_is_reddits_launch_year_my_dudes and month == REDDIT_LAUNCH_MONTH:
            start_day = REDDIT_LAUNCH_DAY
        else:
            start_day = 1
        end_day = monthrange(year, month)[1] + 1
        for day in range(start_day, end_day):
            day_dir = os.path.join(download_basedirname, str(year), str(month).zfill(2), str(day).zfill(2))
            if not os.path.isdir(day_dir):
                os.makedirs(day_dir)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('zst_filename', type=str, help='Path to the Pushshift subreddit posts Zstandard archive')
    parser.add_argument('-o', '--output', type=str, help='Output folder for media downloads; If left blank, the folder is named after the Zstandard archive based on the convention used by REDARCS and kept in the same parent directory as this script')
    # TODO: "Don't download, just check progress" and "Don't download, analyze media types (for needed space estimation)" options
    return parser.parse_args()

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f'Unable to decode frame after reading {bytes_read:,} bytes')
		log.info(f'Decoding error with {bytes_read:,} bytes, reading another chunk')
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)
			if not chunk:
				break
			lines = (buffer + chunk).split('\n')
			for line in lines[:-1]:
				yield line
			buffer = lines[-1]
		reader.close()

def simple_download(download_dirname, download_fileroot, download_main_ext, download_main_src):
    success = False
    download_main_filename = '{}.{}'.format(download_fileroot, download_main_ext)
    download_main_abspath = os.path.join(download_dirname, download_main_filename)
    if not os.path.isfile(download_main_abspath):
        # print('{} {}'.format(created_formatted, download_main_src))
        print('Downloading {} from {}'.format(download_main_filename, download_main_src))
        trying = True
        while trying:
            response = requests.get(download_main_src)
            status_code = response.status_code
            sleep_seconds = SLEEP_SECONDS_INTERVAL
            if status_code == 429:
                print('429 error - retrying after {} seconds'.format(sleep_seconds))
                print(response.headers)
                time.sleep(sleep_seconds)
                sleep_seconds += SLEEP_SECONDS_INTERVAL
            else:
                trying = False
                if status_code == 200:
                    with open(download_main_abspath, 'wb') as download_main:
                        download_main.write(response.content)
                    success = True
                    print('Saved')
                else:
                    error_message = '{} error'.format(status_code)
                    print(error_message)
                    if status_code != 404:
                        raise Exception(error_message)
    else:
        pass
    return success

def main() -> int:
    cwd = getcwd()
    args = parse_args()

    zst_filename = args.zst_filename
    zst_full_filename = os.path.join(cwd, args.zst_filename)
    zst_full_filename = os.path.abspath(os.path.realpath(zst_full_filename))
    if not os.path.isfile(zst_full_filename):
        print('"{}" was not found!'.format(zst_full_filename))
        return 1

    download_basedirname = args.output
    if download_basedirname is None:
        if zst_filename.endswith(REDARCS_SUBMISSIONS_FILE_ENDING):
            download_basedirname = zst_filename[:-REDARCS_SUBMISSIONS_FILE_ENDING_LENGTH]
        else:
            print('Unable to infer download folder name from "{}"! Please specify with -o'.format(zst_filename))
            return 1
    download_basedirname = os.path.join(cwd, download_basedirname)
    download_basedirname = os.path.abspath(os.path.realpath(download_basedirname))
    if not os.path.isdir(download_basedirname):
        os.makedirs(download_basedirname)

    bookmark = ''
    bookmark_not_reached = True
    bookmark_file_path = os.path.join(download_basedirname, 'bookmark.txt')
    try:
        with open(bookmark_file_path, 'r') as bookmark_file:
            bookmark = bookmark_file.readline().strip()
    except:
        pass
    if bookmark == '':
        print('Starting from the beginning')
        bookmark_not_reached = False
    else:
        print('Resuming from post {}'.format(bookmark))

    prepared_year_folders = set()
    interrupted = False
    try:
        for line in read_lines_zst(zst_full_filename):
            data = json.loads(line)
            post_id = chain_get(data, 'id')
            if bookmark_not_reached:
                if post_id == bookmark:
                    bookmark_not_reached = False
                continue
            created_timestamp = int(chain_get(data, 'created_utc'))
            created_datetime = datetime.utcfromtimestamp(created_timestamp)
            created_year = created_datetime.year
            if created_year not in prepared_year_folders:
                print('Preparing download folders for the year {}'.format(created_year))
                prepare_day_folders(download_basedirname, created_year)
                prepared_year_folders.add(created_year)

            created_formatted = created_datetime.strftime('%Y-%m-%d_%H-%M-%S_UTC')
            download_fileroot = created_formatted+'_'+post_id
            download_dirname = os.path.join(download_basedirname, created_datetime.strftime('%Y'), created_datetime.strftime('%m'), created_datetime.strftime('%d'))

            # Single image
            url = chain_get(data, 'url')
            # TODO: Default to https?
            if url is not None and url != '':
                if re.match(r'https?:\/\/i\.redd\.it\/.*', url): # or re.match(r'https?:\/\/i\.imgur\.com\/.*', url):
                    download_main_src = url
                    download_main_ext = url.split('?', 1)[0].rsplit(str('.'), 1)[-1]
                    download_main_ext = re.split(r'[^0-9A-Za-z]', download_main_ext)[0]
                    if simple_download(download_dirname, download_fileroot, download_main_ext, download_main_src):
                        bookmark = post_id

            # Gallery
            gallery_items = chain_get(data, 'gallery_data', 'items')
            media_metadata = chain_get(data, 'media_metadata')
            if gallery_items and media_metadata:
                all_went_well = True
                for image_number, gallery_item in enumerate(gallery_items, 1):
                    media_id = chain_get(gallery_item, 'media_id')
                    ext = chain_get(media_metadata, media_id, 'm')
                    if not ext:
                        continue
                    ext = ext[ext.rfind('/')+1:]
                    all_went_well = all_went_well and simple_download(download_dirname, '{}_{}'.format(download_fileroot, str(image_number).zfill(2)), ext, 'https://i.redd.it/{}.{}'.format(media_id, ext))
                if all_went_well:
                    bookmark = post_id

            # Video
            dash_url = chain_get(data, 'media', 'reddit_video', 'dash_url')
            if dash_url:
                dash_url = html.unescape(dash_url)
                download_main_filename = '{}.%(ext)s'.format(download_fileroot)
                download_main_abspath = os.path.join(download_dirname, download_main_filename)
                print('Downloading {} from {}'.format(download_main_filename, dash_url))
                ydl_opts = {
                    'noprogress': True,
                    'outtmpl': download_main_abspath,
                    'quiet': True,
                    'continuedl': False,
                    'overwrites': True
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    try:
                        ydl.download([dash_url])
                        print('Saved')
                        bookmark = post_id
                    except Exception as e:
                        pass
    except Exception as e:
        print()
        print(e)
        interrupted = True
    except KeyboardInterrupt as e:
        print()
        print(e)
        interrupted = True
    with open(bookmark_file_path, 'w') as bookmark_file:
        bookmark_file.write(bookmark)
    if interrupted:
        print(time.ctime())
        print('Left off at post {}'.format(bookmark))
    else:
        print('!!! ALL DONE !!!')
    return 0

if __name__ == '__main__':
    exit(main())

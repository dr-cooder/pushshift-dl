#!/usr/bin/env python

# TODO: Auto-install modules
import argparse
from bs4 import BeautifulSoup
from calendar import monthrange
from datetime import datetime
import html
import json
import math
import os
import re
import requests
import sys
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

# https://en.wikipedia.org/wiki/Reddit
REDDIT_LAUNCH_DAY = 23
REDDIT_LAUNCH_MONTH = 6
REDDIT_LAUNCH_YEAR = 2005

IMGUR_CLIENT_ID = '546c25a59c58ad7'
# TODO: Modifying this seems to circumvent Imgur's rate limiting! https://pypi.org/project/random-user-agent/
IMGUR_NECESSARY_HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/133.0' }
IMGUR_MEDIA_PATTERN = r'^https?:\/\/i\.imgur\.com\/([a-zA-Z0-9]+\.(jpg|jpeg|png|gif|mp4))'

IS_NOT_A_BOOKMARK = 0
IS_A_BOOKMARK = 1
IS_LAST_BOOKMARK = 2
END_BOOKMARK_STRING = "END"

def chain_get(parent, *keys):
    child = parent
    try:
        for key in keys:
            child = child[key]
    except:
        child = None
    return child

def folder_size(folder):
    file_count = 0
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # Skip symbolic links
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('zst_filename', type=str, help='Path to the Pushshift subreddit posts Zstandard archive')
    parser.add_argument('-o', '--output', type=str, help='Output folder for media downloads; If left blank, the folder is named after the Zstandard archive based on the convention used by REDARCS and kept in the same parent directory as this script')
    parser.add_argument('-e', '--estimate', action=argparse.BooleanOptionalAction, help='Estimate the space required for the remaining files instead of downloading them')
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
		print(f'Decoding error with {bytes_read:,} bytes, reading another chunk')
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**28).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**24, 2**26)
			if not chunk:
				break
			lines = (buffer + chunk).split('\n')
			for line in lines[:-1]:
				yield line
			buffer = lines[-1]
		reader.close()

class NoLog(object):
    @staticmethod
    def warning(content: str) -> None: pass
    @staticmethod
    def debug(content: str) -> None: pass
    @staticmethod
    def error(content: str) -> None: pass

class Silence(object):
    def __enter__(self):
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')
    
    def __exit__(self, *_):
        sys.stdout = self._stdout
        sys.stderr = self._stderr

class FileManager(object):
    def __init__(self, download_basedirname):
        bookmark_file_path = os.path.join(download_basedirname, 'bookmark.txt')
        bookmarks = set()
        started_with_end_of_archive_reached = False
        started_without_bookmarks = True
        try:
            with open(bookmark_file_path, 'r') as bookmark_file:
                for line in bookmark_file:
                    line = line.strip()
                    if line == END_BOOKMARK_STRING:
                        started_with_end_of_archive_reached = True
                    else:
                        try:
                            bookmarks.add(int(line))
                            started_without_bookmarks = False
                        except:
                            pass
        except:
            pass
        self.__bookmark_file_path = bookmark_file_path
        self.__bookmarks = bookmarks
        self.__bookmarks_changed = False
        self.__download_basedirname = download_basedirname
        self.__imgur_available_str = 'Imgur is off limits'
        self.__imgur_available_timestamp = 0
        self.__last_bookmark = None if started_without_bookmarks else max(bookmarks)
        self.__prepared_year_folders = set()
        self.__started_done = started_with_end_of_archive_reached and started_without_bookmarks
        self.__started_with_end_of_archive_reached = started_with_end_of_archive_reached
        self.__started_without_bookmarks = started_without_bookmarks

    @property
    def initial_status(self):
        return self.__initial_status
    
    @property
    def started_done(self):
        return self.__started_done
    
    @property
    def started_with_end_of_archive_reached(self):
        return self.__started_with_end_of_archive_reached
    
    @property
    def started_without_bookmarks(self):
        return self.__started_without_bookmarks

    def is_bookmark(self, value):
        # TODO: This could probably be more efficient
        return IS_LAST_BOOKMARK if value == self.__last_bookmark and not self.__started_with_end_of_archive_reached else IS_A_BOOKMARK if value in self.__bookmarks else IS_NOT_A_BOOKMARK
    
    def add_bookmark(self, value):
        if value not in self.__bookmarks:
            self.__bookmarks_changed = True
        self.__bookmarks.add(value)
    
    def __remove_bookmark(self, value):
        if value in self.__bookmarks:
            self.__bookmarks.remove(value)
            self.__bookmarks_changed = True
    
    def write_bookmark(self, not_interrupted):
        if self.__bookmarks_changed:
            with open(self.__bookmark_file_path, 'w') as bookmark_file:
                bookmark_file.write('\n'.join(map(str, sorted(self.__bookmarks))))
                if not_interrupted or self.__started_with_end_of_archive_reached:
                    if len(self.__bookmarks) == 0:
                        print('!!! ALL DONE !!!')
                    else:
                        bookmark_file.write('\n')
                    bookmark_file.write(END_BOOKMARK_STRING)
            print('Wrote to bookmark file')
        else:
            print('Bookmark unchanged')
        print(time.ctime())

    def __handle_download(self, year, line_number, download_function, *args, **kwargs):
        if year not in self.__prepared_year_folders:
            if year >= REDDIT_LAUNCH_YEAR:
                print('Preparing download folders for the year {}'.format(year))
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
                        day_dir = os.path.join(self.__download_basedirname, str(year), str(month).zfill(2), str(day).zfill(2))
                        if not os.path.isdir(day_dir):
                            os.makedirs(day_dir)
                self.__prepared_year_folders.add(year)
        download_function(*args, **kwargs)
        self.__remove_bookmark(line_number)
    
    def __try_to_get(self, src, is_imgur=False):
        src_secure = re.sub(r'^(https?:\/\/)?', 'https://', src)
        if is_imgur and datetime.now().timestamp() < self.__imgur_available_timestamp:
            raise Exception(self.__imgur_available_str)
        response = requests.get(src_secure, headers=IMGUR_NECESSARY_HEADERS if is_imgur else None)
        status_code = response.status_code
        if is_imgur and (status_code == 403 or status_code == 429 or status_code == 503):
            imgur_available_timestamp = datetime.now().timestamp() + 600 # 3600
            self.__imgur_available_timestamp = imgur_available_timestamp
            imgur_available_str = 'Imgur is off limits until {}'.format(datetime.fromtimestamp(imgur_available_timestamp).strftime("%H:%M:%S, on %a, %d %b %Y"))
            self.__imgur_available_str = imgur_available_str
            raise Exception(imgur_available_str)
        elif response.url == 'https://i.imgur.com/removed.png':
            print('Imgur image not found')
            return None
        elif response.url == 'https://imgur.com/':
            print('Imgur page not found')
            return None
        elif status_code == 200:
            return response.content
        else:
            error_message = '{} error'.format(status_code)
            if status_code == 404:
                print(error_message)
                return None
            else:
                raise Exception(error_message)
    
    def try_to_get(self, line_number, src, is_imgur=False):
        content = self.__try_to_get(src, is_imgur=is_imgur)
        self.__remove_bookmark(line_number)
        return content

    def __simple_download(self, download_dirname, download_fileroot, download_main_ext, download_main_src, is_imgur=False):
        content = None
        download_main_filename = '{}.{}'.format(download_fileroot, download_main_ext)
        download_main_abspath = os.path.join(download_dirname, download_main_filename)
        if not os.path.isfile(download_main_abspath):
            print('Downloading {} from {}'.format(download_main_filename, download_main_src))
            content = self.__try_to_get(download_main_src, is_imgur=is_imgur)
            if content:
                with open(download_main_abspath, 'wb') as download_main:
                    download_main.write(content)
                print('Saved')
        else:
            print('{} has already been downloaded'.format(download_main_filename))
        return bool(content)
    
    def simple_download(self, year, line_number, download_dirname, download_fileroot, download_main_ext, download_main_src, is_imgur=False):
        return self.__handle_download(year, line_number, self.__simple_download, download_dirname, download_fileroot, download_main_ext, download_main_src, is_imgur=is_imgur)

    def __imgur_gallery_download(self, download_dirname, download_fileroot, media_infos):
        all_went_well = True
        for media_number, media_info in enumerate(media_infos, 1):
            media_url = chain_get(media_info, 'url')
            media_url_match = re.match(IMGUR_MEDIA_PATTERN, media_url)
            if not media_url_match:
                continue
            # TODO: zfill more?
            all_went_well = all_went_well and self.__simple_download(
                download_dirname,
                '{}_{}'.format(download_fileroot, str(media_number).zfill(2)),
                media_url_match.group(2),
                media_url,
                is_imgur=True)
        return all_went_well

    def imgur_gallery_download(self, year, line_number, download_dirname, download_fileroot, media_infos):
        return self.__handle_download(year, line_number, self.__imgur_gallery_download, download_dirname, download_fileroot, media_infos)

    def __reddit_gallery_download(self, download_dirname, download_fileroot, gallery_items):
        all_went_well = True
        for media_number, gallery_item in enumerate(gallery_items, 1):
            media_id = chain_get(gallery_item, 'media_id')
            ext = chain_get(media_metadata, media_id, 'm')
            if not ext:
                continue
            ext = ext[ext.rfind('/')+1:]
            all_went_well = all_went_well and self.__simple_download(
                download_dirname,
                '{}_{}'.format(download_fileroot, str(media_number).zfill(2)),
                ext,
                'https://i.redd.it/{}.{}'.format(media_id, ext))
        return all_went_well
    
    def reddit_gallery_download(self, year, line_number, download_dirname, download_fileroot, gallery_items):
        return self.__handle_download(year, line_number, self.__reddit_gallery_download, download_dirname, download_fileroot, gallery_items)

    def __reddit_video_download(self, download_dirname, download_fileroot, dash_url_escaped):
        # TODO: Mix with storyboard if video is not available, like RapidSave does
        dash_url = html.unescape(dash_url_escaped)
        download_main_filename = '{}.%(ext)s'.format(download_fileroot)
        download_main_abspath = os.path.join(download_dirname, download_main_filename)
        print('Downloading {} from {}'.format(download_main_filename, dash_url))
        ydl_opts = {
            'logger': NoLog,
            'outtmpl': download_main_abspath,
            'continuedl': False,
            'overwrites': True
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                ydl.download([dash_url])
                print('Saved')
                return True
            except Exception as e:
                exc_info = e.exc_info
                if isinstance(exc_info, tuple):
                    exc_info_1 = exc_info[1]
                    if isinstance(exc_info_1, yt_dlp.networking.exceptions.HTTPError):
                        status_code = exc_info_1.status
                        print('{} error'.format(status_code))
                        if status_code != 403:
                            raise e
                        return False
                    else:
                        raise e
                else:
                    raise e

    def reddit_video_download(self, year, line_number, download_dirname, download_fileroot, dash_url_escaped):
        return self.__handle_download(year, line_number, self.__reddit_video_download, download_dirname, download_fileroot, dash_url_escaped)

def main() -> int:
    cwd = getcwd()
    args = parse_args()

    download_mode = not args.estimate

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

    file_manager = FileManager(download_basedirname)
    last_bookmark_not_reached = True
    not_interrupted = True

    try:
        if download_mode:
            if file_manager.started_done:
                print('Archive is already done!')
                return 0
            if not os.path.isdir(download_basedirname):
                os.makedirs(download_basedirname)
            if file_manager.started_without_bookmarks:
                print('Starting from the beginning')
                last_bookmark_not_reached = False
            else:
                print('Resuming')
        elif file_manager.started_done:
            print('Archive is done, no more space needed!')
            return 0
        elif file_manager.started_without_bookmarks:
            print('Please download some files from this archive before trying to estimate the remaining space needed!')
            return 1
        else:
            print('Checking space used...')
            current_post_count = 0
            total_post_count = 0
            current_total_size = folder_size(download_basedirname)
            print('Checking archive progress...')

        line_number = -1
        for line in read_lines_zst(zst_full_filename):
            line_number += 1
            try:
                line_is_bookmark = file_manager.is_bookmark(line_number)
                if last_bookmark_not_reached:
                    if download_mode and line_is_bookmark == IS_NOT_A_BOOKMARK:
                        continue
                    if line_is_bookmark == IS_LAST_BOOKMARK:
                        last_bookmark_not_reached = False

                data = json.loads(line)
                post_id = chain_get(data, 'id')

                if download_mode:
                    created_timestamp = int(chain_get(data, 'created_utc'))
                    created_datetime = datetime.utcfromtimestamp(created_timestamp)
                    created_year = created_datetime.year

                    created_formatted = created_datetime.strftime('%Y-%m-%d_%H-%M-%S_UTC')
                    download_fileroot = created_formatted+'_'+post_id
                    download_dirname = os.path.join(download_basedirname, created_datetime.strftime('%Y'), created_datetime.strftime('%m'), created_datetime.strftime('%d'))

                url = chain_get(data, 'url')
                if url is not None and url != '':
                    # Reddit/Imgur image
                    single_image_match = re.match(r'^(https?:\/\/(i\.redd\.it|(i\.)?imgur\.com)\/([a-zA-Z0-9]+\.(jpg|jpeg|png|gif|mp4)))', url)
                    if single_image_match:
                        if download_mode:
                            file_manager.simple_download(
                                created_year,
                                line_number,
                                download_dirname,
                                download_fileroot,
                                single_image_match.group(5),
                                single_image_match.group(1),
                                is_imgur = single_image_match.group(2).endswith('imgur.com'))
                        else:
                            # TODO: Make a post count tracker object for estimate mode
                            if last_bookmark_not_reached and line_is_bookmark == IS_NOT_A_BOOKMARK:
                                current_post_count += 1
                            total_post_count += 1
                    else:
                        # Imgur gallery
                        json_unexpected = False
                        imgur_gallery_match = re.match(r'^https?:\/\/imgur\.com\/(a|gallery)\/([a-zA-Z0-9]+)', url)
                        if imgur_gallery_match:
                            if download_mode:
                                imgur_gallery_id = imgur_gallery_match.group(2)
                                print('Downloading info for Imgur gallery {}'.format(imgur_gallery_id))
                                imgur_gallery_info_content = file_manager.try_to_get(line_number, 'https://api.imgur.com/post/v1/albums/{}?client_id={}&include=media'.format(imgur_gallery_id, IMGUR_CLIENT_ID), is_imgur=True)
                                if imgur_gallery_info_content:
                                    media_infos = chain_get(json.loads(imgur_gallery_info_content), 'media')
                                    if media_infos:
                                        file_manager.imgur_gallery_download(
                                            created_year,
                                            line_number,
                                            download_dirname,
                                            download_fileroot,
                                            media_infos)
                                    else:
                                        json_unexpected = True
                            else:
                                if last_bookmark_not_reached and line_is_bookmark == IS_NOT_A_BOOKMARK:
                                    current_post_count += 1
                                total_post_count += 1

                        # Imgur page
                        imgur_page_match = re.match(r'^(https?:\/\/imgur\.com\/([a-zA-Z0-9]+)($|[^\/a-zA-Z0-9]))', url)
                        if json_unexpected or imgur_page_match:
                            if download_mode:
                                url_simplified = 'https://imgur.com/gallery/{}'.format(imgur_gallery_id) if json_unexpected else imgur_page_match.group(1)
                                print('Downloading Imgur page {}'.format(url_simplified))
                                imgur_page_content = file_manager.try_to_get(line_number, url_simplified, is_imgur=True)
                                if imgur_page_content:
                                    # TODO: For some inexplicable reason, downloading from certain (very rare) Imgur page URLs yields the image;
                                    # if BeautifulSoup fails, this has probably happened, and the filetype can be inferred by the Magic Bytes https://pypi.org/project/pyfsig/
                                    with Silence():
                                        imgur_page_head = BeautifulSoup(imgur_page_content, 'html.parser').head
                                    # TODO: Video and image are very similar; DRY better here
                                    imgur_og_video = imgur_page_head.find('meta', { 'property': 'og:video' })
                                    if imgur_og_video:
                                        imgur_video_url = imgur_og_video['content']
                                        imgur_video_url_match = re.match(IMGUR_MEDIA_PATTERN, imgur_video_url)
                                        if imgur_video_url_match:
                                            file_manager.simple_download(
                                                created_year,
                                                line_number,
                                                download_dirname,
                                                download_fileroot,
                                                imgur_video_url_match.group(2),
                                                imgur_video_url,
                                                is_imgur=True)
                                        else:
                                            raise Exception('Unexpected URL format {}'.format(imgur_video_url))
                                    else: # If there is a video, this is the thumbnail
                                        imgur_og_image = imgur_page_head.find('meta', { 'property': 'og:image' })
                                        if imgur_og_image:
                                            imgur_image_url = imgur_og_image['content']
                                            imgur_image_url_match = re.match(IMGUR_MEDIA_PATTERN, imgur_image_url)
                                            if imgur_image_url_match:
                                                file_manager.simple_download(
                                                    created_year,
                                                    line_number,
                                                    download_dirname,
                                                    download_fileroot,
                                                    imgur_image_url_match.group(2),
                                                    imgur_image_url,
                                                    is_imgur=True)
                                            else:
                                                raise Exception('Unexpected URL format {}'.format(imgur_image_url))
                                        else:
                                            print('No image or video found in page')
                            else:
                                if last_bookmark_not_reached and line_is_bookmark == IS_NOT_A_BOOKMARK:
                                    current_post_count += 1
                                total_post_count += 1

                # Reddit gallery
                gallery_items = chain_get(data, 'gallery_data', 'items')
                media_metadata = chain_get(data, 'media_metadata')
                if gallery_items and media_metadata:
                    if download_mode:
                        file_manager.reddit_gallery_download(
                            created_year,
                            line_number,
                            download_dirname,
                            download_fileroot,
                            gallery_items)
                    else:
                        if last_bookmark_not_reached and line_is_bookmark == IS_NOT_A_BOOKMARK:
                            current_post_count += 1
                        total_post_count += 1

                # Reddit video
                dash_url = chain_get(data, 'media', 'reddit_video', 'dash_url')
                if dash_url:
                    if download_mode:
                        file_manager.reddit_video_download(
                            created_year,
                            line_number,
                            download_dirname,
                            download_fileroot,
                            dash_url)
                    else:
                        if last_bookmark_not_reached and line_is_bookmark == IS_NOT_A_BOOKMARK:
                            current_post_count += 1
                        total_post_count += 1
            except Exception as e:
                print(e)
                file_manager.add_bookmark(line_number)
    except KeyboardInterrupt as e:
        print()
        print(e)
        file_manager.add_bookmark(line_number)
        not_interrupted = False
    if download_mode:
        file_manager.write_bookmark(not_interrupted)
    elif not_interrupted:
        # In estimate mode and both the bookmark and the end of the archive
        # were reached, i.e. the estimate can and will be made
        post_count_ratio = current_post_count / total_post_count
        estimated_total_size = current_total_size / post_count_ratio
        print('About {:,} more bytes needed, with {}% confidence\n(currently at {:,} of {:,} posts downloaded with {:,} bytes used)'.format(math.ceil(estimated_total_size * (1 - post_count_ratio)), math.ceil(post_count_ratio * 100000000) / 1000000, current_post_count, total_post_count, current_total_size))
    else:
        # In estimate mode and the end of the archive was not reached
        print('Estimation cancelled!')
    return 0

if __name__ == '__main__':
    exit(main())

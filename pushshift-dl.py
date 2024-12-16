#!/usr/bin/env python

# TODO: Auto-install modules
import argparse
from calendar import monthrange
from datetime import datetime
import html
import json
import math
import os
from random_user_agent.user_agent import UserAgent
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

# TODO: Make specifiable
AUTOSAVE_FREQUENCY = 1000
IMGUR_CLIENT_ID = '546c25a59c58ad7'
STREAM_CHUNK_SIZE = 8192

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
        self.__downloads_since_last_autosave = 0
        self.__last_bookmark = None if started_without_bookmarks else max(bookmarks)
        self.__prepared_year_folders = set()
        self.__started_done = started_with_end_of_archive_reached and started_without_bookmarks
        self.__started_with_end_of_archive_reached = started_with_end_of_archive_reached
        self.__started_without_bookmarks = started_without_bookmarks
        self.__user_agent_not_initialized = True

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
    
    def __initialize_user_agent(self):
        self.__user_agent_rotator = UserAgent()
        self.__change_user_agent()
        self.__user_agent_not_initialized = False
        print('User agent is "{}"'.format(self.__user_agent))
    
    def __change_user_agent(self):
        self.__user_agent = self.__user_agent_rotator.get_random_user_agent()

    def is_bookmark(self, value):
        # TODO: This could probably be more efficient
        return IS_LAST_BOOKMARK if value == self.__last_bookmark and not self.__started_with_end_of_archive_reached else IS_A_BOOKMARK if value in self.__bookmarks else IS_NOT_A_BOOKMARK
    
    def add_bookmark(self, value):
        if value not in self.__bookmarks:
            self.__bookmarks_changed = True
        self.__bookmarks.add(value)
        self.__increment_downloads_since_last_autosave()
    
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
    
    def __increment_downloads_since_last_autosave(self):
        self.__downloads_since_last_autosave += 1
        if self.__downloads_since_last_autosave >= AUTOSAVE_FREQUENCY:
            self.write_bookmark(False)
            self.__downloads_since_last_autosave %= AUTOSAVE_FREQUENCY

    def __handle_download(self, year, line_number, download_function, *args, **kwargs):
        if year not in self.__prepared_year_folders:
            print('Preparing download folders for the year {}'.format(year))
            for month in range(1, 13):
                end_day = monthrange(year, month)[1] + 1
                for day in range(1, end_day):
                    day_dir = os.path.join(self.__download_basedirname, str(year), str(month).zfill(2), str(day).zfill(2))
                    if not os.path.isdir(day_dir):
                        os.makedirs(day_dir)
            self.__prepared_year_folders.add(year)
        download_function(*args, **kwargs)
        # If no error occurred while trying to download, then...
        self.__increment_downloads_since_last_autosave()
        self.__remove_bookmark(line_number)
    
    def __try_to_get(self, src, is_imgur=False, stream=False):
        src_secure = re.sub(r'^(https?:\/\/)?', 'https://', src)
        if self.__user_agent_not_initialized and is_imgur:
            self.__initialize_user_agent()
        while True:
            response = requests.get(src_secure, headers = { 'User-Agent': self.__user_agent } if is_imgur else None)
            status_code = response.status_code
            if is_imgur and (status_code == 403 or status_code == 429 or status_code == 503):
                self.__change_user_agent()
                print('Got {} error; Switched user agent to "{}"; Retrying download'.format(status_code, self.__user_agent))
            elif response.url == 'https://i.imgur.com/removed.png':
                print('Imgur media not found')
                return None
            elif response.url == 'https://imgur.com/':
                print('Imgur page not found')
                return None
            elif status_code == 200:
                if stream:
                    return response.iter_content(chunk_size=STREAM_CHUNK_SIZE)
                else:
                    return response.content
            else:
                error_message = '{} error'.format(status_code)
                if status_code == 404:
                    print(error_message)
                    return None
                else:
                    raise Exception(error_message)
    
    def try_to_get(self, line_number, src, is_imgur=False, stream=False):
        content = self.__try_to_get(src, is_imgur=is_imgur)
        self.__remove_bookmark(line_number)
        return content

    def __simple_download(self, download_dirname, download_fileroot, download_ext, download_src, is_imgur=False):
        content = None
        download_filename = '{}.{}'.format(download_fileroot, download_ext)
        download_abspath = os.path.join(download_dirname, download_filename)
        if not os.path.isfile(download_abspath):
            print('Downloading {} from {}'.format(download_filename, download_src))
            iter_content = self.__try_to_get(download_src, is_imgur=is_imgur, stream=True)
            if iter_content:
                with open(download_abspath, 'wb') as download:
                    for chunk in iter_content:
                        download.write(chunk)
                print('Saved')
        else:
            print('{} has already been downloaded'.format(download_filename))
        return bool(content)
    
    def simple_download(self, year, line_number, download_dirname, download_fileroot, download_ext, download_src, is_imgur=False):
        return self.__handle_download(year, line_number, self.__simple_download, download_dirname, download_fileroot, download_ext, download_src, is_imgur=is_imgur)

    def __imgur_page_download(self, download_dirname, download_fileroot, media_infos):
        all_went_well = True
        only_one_media_info = len(media_infos) == 1
        for media_number, media_info in enumerate(media_infos, 1):
            media_url = chain_get(media_info, 'url')
            media_url_match = re.match(r'^https?:\/\/(i\.)?imgur\.com\/([a-zA-Z0-9]+\.(jpg|jpeg|png|gif|mp4))', media_url)
            if not media_url_match:
                continue
            # TODO: zfill more?
            all_went_well = all_went_well and self.__simple_download(
                download_dirname,
                download_fileroot if only_one_media_info else '{}_{}'.format(download_fileroot, str(media_number).zfill(2)),
                media_url_match.group(3),
                media_url,
                is_imgur=True)
        return all_went_well

    def imgur_page_download(self, year, line_number, download_dirname, download_fileroot, media_infos):
        return self.__handle_download(year, line_number, self.__imgur_page_download, download_dirname, download_fileroot, media_infos)

    def __reddit_gallery_download(self, download_dirname, download_fileroot, gallery_items):
        all_went_well = True
        only_one_gallery_item = len(gallery_items) == 1
        for media_number, gallery_item in enumerate(gallery_items, 1):
            media_id = chain_get(gallery_item, 'media_id')
            ext = chain_get(media_metadata, media_id, 'm')
            if not ext:
                continue
            ext = ext[ext.rfind('/')+1:]
            all_went_well = all_went_well and self.__simple_download(
                download_dirname,
                download_filreoot if only_one_gallery_item else '{}_{}'.format(download_fileroot, str(media_number).zfill(2)),
                ext,
                'https://i.redd.it/{}.{}'.format(media_id, ext))
        return all_went_well
    
    def reddit_gallery_download(self, year, line_number, download_dirname, download_fileroot, gallery_items):
        return self.__handle_download(year, line_number, self.__reddit_gallery_download, download_dirname, download_fileroot, gallery_items)

    def __reddit_video_download(self, download_dirname, download_fileroot, dash_url_escaped):
        # TODO: Mix with storyboard if video is not available, like RapidSave does
        dash_url = html.unescape(dash_url_escaped)
        download_filename = '{}.%(ext)s'.format(download_fileroot)
        download_abspath = os.path.join(download_dirname, download_filename)
        print('Downloading {} from {}'.format(download_filename, dash_url))
        ydl_opts = {
            'logger': NoLog,
            'outtmpl': download_abspath,
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

class Estimator(object):
    def __init__(self, current_total_size):
        self.__current_total_size = current_total_size
        self.__downloaded_post_count = 0
        self.__total_post_count = 0
    
    def increment(self, is_downloaded):
        if is_downloaded:
            self.__downloaded_post_count += 1
        self.__total_post_count += 1
    
    def estimate(self):
        post_count_ratio = self.__downloaded_post_count / self.__total_post_count
        estimated_total_size = self.__current_total_size / post_count_ratio
        print('About {:,} more bytes needed, with {}% confidence\n(currently at {:,} of {:,} posts downloaded with {:,} bytes used)'.format(math.ceil(estimated_total_size * (1 - post_count_ratio)), math.ceil(post_count_ratio * 100000000) / 1000000, self.__downloaded_post_count, self.__total_post_count, self.__current_total_size))

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
            estimator = Estimator(folder_size(download_basedirname))
            print('Checking archive progress...')

        line_number = -1
        for line in read_lines_zst(zst_full_filename):
            line_number += 1
            try:
                line_is_bookmark = file_manager.is_bookmark(line_number)
                line_could_be_a_downloaded_post = last_bookmark_not_reached and line_is_bookmark == IS_NOT_A_BOOKMARK
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
                            estimator.increment(line_could_be_a_downloaded_post)
                    else:
                        # Imgur page (media or album)
                        imgur_match = re.match(r'^https?:\/\/imgur\.com\/((a|gallery)\/)?([a-zA-Z0-9,]+)($|[^\/a-zA-Z0-9])', url)
                        if imgur_match:
                            if download_mode:
                                endpoint_name = 'album' if imgur_match.group(2) else 'media'
                                imgur_ids = imgur_match.group(3).split(',')
                                media_infos = list()
                                for imgur_id in imgur_ids:
                                    print('Downloading info for Imgur {} {}'.format(endpoint_name, imgur_id))
                                    imgur_gallery_info_content = file_manager.try_to_get(line_number, 'https://api.imgur.com/post/v1/{}/{}?client_id={}&include=media'.format(endpoint_name, imgur_id, IMGUR_CLIENT_ID), is_imgur=True)
                                    if imgur_gallery_info_content:
                                        current_media_infos = chain_get(json.loads(imgur_gallery_info_content), 'media')
                                        if current_media_infos and isinstance(current_media_infos, list):
                                            media_infos.extend(current_media_infos)
                                file_manager.imgur_page_download(
                                    created_year,
                                    line_number,
                                    download_dirname,
                                    download_fileroot,
                                    media_infos)
                            else:
                                estimator.increment(line_could_be_a_downloaded_post)

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
                        estimator.increment(line_could_be_a_downloaded_post)

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
                        estimator.increment(line_could_be_a_downloaded_post)
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
        estimator.estimate()
    else:
        # In estimate mode and the end of the archive was not reached
        print('Estimation cancelled!')
    return 0

if __name__ == '__main__':
    exit(main())

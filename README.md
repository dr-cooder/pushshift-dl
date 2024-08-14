# Pushshift Media Downloader
Downloads media linked by posts from Pushshift subreddit submission Zstandard archives (currently only supports media hosted by Reddit itself, i.e. no Imgur, YouTube, etc.), organized by UTC creation date.
## Usage
1. Get the "submissions" Zstandard archive of the subreddit of choice. I recommend downloading it from REDARCS: https://the-eye.eu/redarcs/
2. Run `pushshift-dl.py path/to/subreddit_submissions.zst`
3. You can pause the download at any time with Ctrl+C and resume where you left off.
4. If you are unsure of how much space you will need, pause the download after downloading a few files, then run `pushshift-dl.py -e path/to/subreddit_submissions.zst`
## Credits
### Code Snippets
- https://github.com/obskyr/khinsider
- https://github.com/Watchful1/PushshiftDumps
- https://stackoverflow.com/questions/1392413/calculating-a-directorys-size-using-python
# Pushshift Media Downloader
Downloads media linked by posts from Pushshift subreddit submission Zstandard archives (currently only supports media hosted by Reddit itself, i.e. no Imgur, YouTube, etc.), organized by UTC creation date.
## Usage
1. Make sure you have enough storage space. Estimations depend on the kind of posts made to the subreddit on average (built-in estimation feature coming soon(?)), but for reference, me_irl had a 688MB compressed archive and 466.5GB worth of media. External drives with reasonable allocation unit sizes are recommended.
2. Get the "submissions" Zstandard archive of the subreddit of choice. I recommend downloading it from REDARCS: https://the-eye.eu/redarcs/
3. Run `pushshift-dl.py path/to/subreddit_submissions.zst`
4. You can pause the download at any time with Ctrl+C and resume where you left off.
## Credits
### Code Snippets
- https://github.com/obskyr/khinsider
- https://github.com/Watchful1/PushshiftDumps
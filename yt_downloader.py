#!/usr/bin/env python3
"""
YouTube Channel Bulk Video Downloader
Scrapes channel videos and downloads them with SQLite tracking
"""

import sqlite3
import subprocess
import re
import os
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright
import argparse
import sys


class YTDownloader:
    def __init__(self, db_path="database.sqlite", download_dir="downloads"):
        self.db_path = db_path
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database with videos table"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_url TEXT UNIQUE NOT NULL,
                video_id TEXT NOT NULL,
                title TEXT,
                channel_url TEXT,
                status TEXT DEFAULT 'pending',
                download_path TEXT,
                custom_filename TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                downloaded_at TIMESTAMP,
                error_msg TEXT
            )
        """)
        conn.commit()
        conn.close()

    def scrape_channel_videos(self, channel_url):
        """Scrape all video URLs from a YouTube channel using Playwright"""
        print(f"[*] Scraping videos from: {channel_url}")
        video_urls = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            try:
                page.goto(channel_url, wait_until="networkidle", timeout=30000)

                # Scroll to load all videos
                prev_height = 0
                while True:
                    page.evaluate(
                        "window.scrollTo(0, document.documentElement.scrollHeight)"
                    )
                    page.wait_for_timeout(2000)
                    new_height = page.evaluate("document.documentElement.scrollHeight")
                    if new_height == prev_height:
                        break
                    prev_height = new_height

                # Extract video links
                links = page.query_selector_all("a#video-title-link")
                for link in links:
                    href = link.get_attribute("href")
                    if href and "/watch?v=" in href:
                        full_url = (
                            f"https://www.youtube.com{href}"
                            if href.startswith("/")
                            else href
                        )
                        # Clean URL (remove playlist params, etc)
                        full_url = re.sub(r"&list=.*", "", full_url)
                        video_urls.append(full_url)

                video_urls = list(set(video_urls))  # Remove duplicates
                print(f"[+] Found {len(video_urls)} videos")

            except Exception as e:
                print(f"[-] Error scraping channel: {e}")
            finally:
                browser.close()

        return video_urls

    def scrape_playlist_videos(self, playlist_url):
        """Scrape all video URLs from a YouTube playlist using yt-dlp."""
        print(f"[*] Scraping videos from playlist: {playlist_url}")
        video_urls = []
        try:
            command = ["yt-dlp", "--print", "id", "--flat-playlist", playlist_url]
            result = subprocess.run(
                command, capture_output=True, text=True, check=True, timeout=60
            )
            video_ids = result.stdout.strip().split("\n")
            video_urls = [
                f"https://www.youtube.com/watch?v={vid_id}"
                for vid_id in video_ids
                if vid_id
            ]
            print(f"[+] Found {len(video_urls)} videos in playlist.")
        except subprocess.CalledProcessError as e:
            print(f"[-] Error scraping playlist: {e.stderr}")
        except subprocess.TimeoutExpired:
            print("[-] Playlist scraping timed out.")
        except Exception as e:
            print(f"[-] An unexpected error occurred: {e}")
        return video_urls

    def add_videos_to_db(self, video_urls, channel_url):
        """Add scraped video URLs to database"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        added = 0
        for url in video_urls:
            video_id = self._extract_video_id(url)
            try:
                c.execute(
                    """
                    INSERT INTO videos (video_url, video_id, channel_url, status)
                    VALUES (?, ?, ?, 'pending')
                """,
                    (url, video_id, channel_url),
                )
                added += 1
            except sqlite3.IntegrityError:
                pass  # Already exists

        conn.commit()
        conn.close()
        print(f"[+] Added {added} new videos to database")
        return added

    def _extract_video_id(self, url):
        """Extract video ID from YouTube URL"""
        match = re.search(r"v=([a-zA-Z0-9_-]{11})", url)
        return match.group(1) if match else None

    def get_pending_videos(self, channel_url=None):
        """Get all pending videos from database, optionally filtered by channel/playlist URL"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        if channel_url:
            c.execute(
                "SELECT id, video_url, video_id FROM videos WHERE status='pending' AND channel_url=?",
                (channel_url,),
            )
        else:
            c.execute(
                "SELECT id, video_url, video_id FROM videos WHERE status='pending'"
            )
        videos = c.fetchall()
        conn.close()
        return videos

    def download_video(self, video_id, video_url, custom_name=None, use_title=True):
        """Download video using yt-dlp with live progress output"""
        try:
            # Get video info first
            print(f"[*] Fetching video information...")
            info_cmd = ["yt-dlp", "--print", "title", video_url]
            result = subprocess.run(
                info_cmd, capture_output=True, text=True, timeout=30
            )
            title = (
                result.stdout.strip() if result.returncode == 0 else f"video_{video_id}"
            )

            # Sanitize filename
            if custom_name:
                filename = self._sanitize_filename(custom_name)
            elif use_title:
                filename = self._sanitize_filename(title)
            else:
                filename = video_id

            output_path = self.download_dir / f"{filename}.mp4"

            # Download video with live output
            print(f"\n{'=' * 80}")
            print(f"[*] Downloading: {title}")
            print(f"[*] Output: {output_path}")
            print(f"{'=' * 80}\n")

            cmd = [
                "yt-dlp",
                "-f",
                "mp4",
                "-o",
                str(output_path),
                "--progress",
                "--newline",
                video_url,
            ]

            # Run with live output streaming
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
            )

            # Stream output in real-time
            output_lines = []
            for line in process.stdout:
                print(line, end="")
                output_lines.append(line)

            process.wait()

            if process.returncode == 0:
                self._update_video_status(
                    video_url, "completed", str(output_path), title, custom_name
                )
                print(f"\n{'=' * 80}")
                print(f"[+] Successfully downloaded: {output_path}")
                print(f"{'=' * 80}\n")
                return True, str(output_path)
            else:
                error = "".join(output_lines[-10:])[:500]
                self._update_video_status(video_url, "failed", error_msg=error)
                print(f"\n{'=' * 80}")
                print(f"[-] Download failed")
                print(f"[-] Error: {error}")
                print(f"{'=' * 80}\n")
                return False, error

        except subprocess.TimeoutExpired:
            error = "Download timeout"
            self._update_video_status(video_url, "failed", error_msg=error)
            print(f"\n[-] {error}")
            return False, error
        except Exception as e:
            error = str(e)[:500]
            self._update_video_status(video_url, "failed", error_msg=error)
            print(f"\n[-] Error: {error}")
            return False, error

    def _sanitize_filename(self, filename):
        """Sanitize filename for filesystem"""
        filename = re.sub(r'[<>:"/\\|?*]', "", filename)
        filename = filename.strip()
        return filename[:200]  # Limit length

    def _update_video_status(
        self,
        video_url,
        status,
        download_path=None,
        title=None,
        custom_name=None,
        error_msg=None,
    ):
        """Update video status in database"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        if status == "completed":
            c.execute(
                """
                UPDATE videos 
                SET status=?, download_path=?, title=?, custom_filename=?, downloaded_at=?
                WHERE video_url=?
            """,
                (status, download_path, title, custom_name, datetime.now(), video_url),
            )
        else:
            c.execute(
                """
                UPDATE videos 
                SET status=?, error_msg=?
                WHERE video_url=?
            """,
                (status, error_msg, video_url),
            )

        conn.commit()
        conn.close()

    def download_all_pending(
        self,
        use_title=True,
        interactive=False,
        channel_url=None,
        skip_confirmation=False,
    ):
        """Download all pending videos, optionally filtered by channel/playlist URL"""
        videos = self.get_pending_videos(channel_url=channel_url)

        if not videos:
            print("[!] No pending videos to download")
            return

        print(f"[*] Found {len(videos)} pending videos")

        if not skip_confirmation:
            print(f"[*] You are about to download {len(videos)} videos.")
            choice = input("Do you want to proceed? [y/N]: ").lower()
            if choice != "y":
                print("[-] Download aborted.")
                return

        for idx, (db_id, url, vid_id) in enumerate(videos, 1):
            print(f"\n[{idx}/{len(videos)}] Processing: {url}")

            custom_name = None
            if interactive:
                choice = input("Use video title as filename? (y/n/custom): ").lower()
                if choice == "n":
                    use_title = False
                elif choice == "custom":
                    custom_name = input("Enter custom filename (without extension): ")

            self.download_video(vid_id, url, custom_name, use_title)

    def list_videos(self, status=None):
        """List videos from database"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        if status:
            c.execute("SELECT * FROM videos WHERE status=?", (status,))
        else:
            c.execute("SELECT * FROM videos")

        videos = c.fetchall()
        conn.close()

        if not videos:
            print(
                f"[!] No videos found" + (f" with status '{status}'" if status else "")
            )
            return

        print(f"\n{'ID':<5} {'Status':<12} {'Title':<50} {'URL':<40}")
        print("=" * 120)
        for v in videos:
            (
                vid_id,
                url,
                video_id,
                title,
                channel,
                status,
                path,
                custom,
                scraped,
                downloaded,
                error,
            ) = v
            title_display = (
                (title or "N/A")[:47] + "..."
                if title and len(title) > 50
                else (title or "N/A")
            )
            print(
                f"{vid_id:<5} {status:<12} {title_display:<50} {url[:37] + '...' if len(url) > 40 else url:<40}"
            )

    def delete_videos(self, status, skip_confirmation=False):
        """Delete videos from database by status"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        if status == "all":
            c.execute("SELECT COUNT(*) FROM videos")
        else:
            c.execute("SELECT COUNT(*) FROM videos WHERE status=?", (status,))
        
        count = c.fetchone()[0]

        if count == 0:
            print(f"[!] No videos found with status '{status}'")
            conn.close()
            return

        if not skip_confirmation:
            print(f"[*] You are about to DELETE {count} videos with status '{status}'.")
            choice = input("Are you sure? This cannot be undone. [y/N]: ").lower()
            if choice != "y":
                print("[-] Deletion aborted.")
                conn.close()
                return

        if status == "all":
            c.execute("DELETE FROM videos")
        else:
            c.execute("DELETE FROM videos WHERE status=?", (status,))
        
        conn.commit()
        conn.close()
        print(f"[+] Successfully deleted {count} videos.")


def main():
    parser = argparse.ArgumentParser(
        description="YouTube Channel Bulk Video Downloader"
    )
    parser.add_argument("--channel", "-c", help="YouTube channel URL")
    parser.add_argument("--video", "-v", help="Single YouTube video URL to download")
    parser.add_argument("--playlist", "-p", help="YouTube playlist URL to download")
    parser.add_argument(
        "--scrape", action="store_true", help="Scrape videos from channel"
    )
    parser.add_argument(
        "--download", "-d", action="store_true", help="Download pending videos"
    )
    parser.add_argument(
        "--list",
        "-l",
        choices=["all", "pending", "completed", "failed"],
        help="List videos by status",
    )
    parser.add_argument(
        "--delete",
        choices=["all", "pending", "completed", "failed"],
        help="Delete videos by status",
    )
    parser.add_argument(
        "--interactive",
        "-i",
        action="store_true",
        help="Interactive mode for custom filenames",
    )
    parser.add_argument(
        "--use-id",
        action="store_true",
        help="Use video ID as filename instead of title",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )
    parser.add_argument(
        "--db",
        default="database.sqlite",
        help="Database path (default: database.sqlite)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="downloads",
        help="Download directory (default: downloads)",
    )

    args = parser.parse_args()

    downloader = YTDownloader(db_path=args.db, download_dir=args.output)

    if args.scrape:
        if not args.channel:
            print("[-] Error: --channel required for scraping")
            sys.exit(1)

        urls = downloader.scrape_channel_videos(args.channel)
        downloader.add_videos_to_db(urls, args.channel)

    if args.video:
        video_url = args.video
        video_id = downloader._extract_video_id(video_url)
        if video_id:
            downloader.add_videos_to_db([video_url], "Single Video")
            use_title = not args.use_id
            custom_name = None
            if args.interactive:
                choice = input("Use video title as filename? (y/n/custom): ").lower()
                if choice == "n":
                    use_title = False
                elif choice == "custom":
                    custom_name = input("Enter custom filename (without extension): ")
            downloader.download_video(video_id, video_url, custom_name, use_title)
        else:
            print(f"[-] Invalid YouTube video URL: {args.video}")
            sys.exit(1)

    if args.playlist:
        playlist_url = args.playlist
        urls = downloader.scrape_playlist_videos(playlist_url)
        if urls:
            downloader.add_videos_to_db(urls, playlist_url)
            print(
                f"[*] Starting download for {len(urls)} videos from playlist: {playlist_url}"
            )
            use_title = not args.use_id
            downloader.download_all_pending(
                use_title=use_title,
                interactive=args.interactive,
                channel_url=playlist_url,
                skip_confirmation=args.yes,
            )

    if args.download:
        use_title = not args.use_id
        downloader.download_all_pending(
            use_title=use_title,
            interactive=args.interactive,
            skip_confirmation=args.yes,
        )

    if args.list:
        status = None if args.list == "all" else args.list
        downloader.list_videos(status=status)

    if args.delete:
        downloader.delete_videos(args.delete, skip_confirmation=args.yes)

    if not any([args.scrape, args.download, args.list, args.video, args.playlist, args.delete]):
        parser.print_help()


if __name__ == "__main__":
    main()

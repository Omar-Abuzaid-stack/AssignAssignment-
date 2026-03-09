import os
import sys
import json
import logging
import asyncio
import subprocess
from datetime import datetime
import instaloader
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_ytdlp(command):
    try:
        # We capture stdout which yt-dlp dumps as json-lines when --dump-json is used
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        lines = result.stdout.strip().split('\n')
        data = []
        for line in lines:
            if not line.strip(): continue
            try:
                data.append(json.loads(line))
            except Exception:
                pass
        return data
    except subprocess.CalledProcessError as e:
        logger.error(f"yt-dlp command failed: {e.stderr}")
        return []

async def get_yt_dlp_metadata(url):
    """Attempt to get metadata using yt-dlp."""
    cmd = f"python3 -m yt_dlp --dump-json \"{url}\""
    data = run_ytdlp(cmd)
    if data and isinstance(data, list) and len(data) > 0:
        item = data[0]
        return {
            "view_count": item.get("view_count", 0),
            "likes": item.get("like_count", 0),
            "comments": item.get("comment_count", 0),
            "shares": item.get("repost_count", item.get("share_count", 0)),
            "caption": item.get("description", item.get("title", ""))
        }
    return None

async def scrape_profile_playwright(url, platform, target_count=10):
    logger.info(f"Targeted scraping for {platform} profile: {url}")
    results = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            await page.goto(url)
            await page.wait_for_timeout(5000)
            
            # Simple link extraction
            links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('a'))
                    .map(a => a.href)
                    .filter(href => href.includes('/p/') || href.includes('/reel/') || href.includes('/watch/') || href.includes('/videos/') || href.includes('/video/'));
            }''')
            
            unique_links = list(set(links))
            logger.info(f"Found {len(unique_links)} potential links on {url}")
            
            for link in unique_links:
                if len(results) >= target_count: break
                
                # For TikTok/YouTube, yt-dlp is best
                if "tiktok.com" in link or "youtube.com" in link:
                    meta = await get_yt_dlp_metadata(link)
                    if meta and meta.get("view_count", 0) > 0:
                        results.append({
                            "platform": platform,
                            "data": {
                                "url": link,
                                "view_count": meta["view_count"],
                                "likes": meta["likes"],
                                "comments": meta["comments"],
                                "shares": meta["shares"],
                                "creator_handle": url.split('/')[-1],
                                "caption": meta["caption"]
                            }
                        })
                else:
                    # For IG/FB, we might need to visit the link to get metadata if yt-dlp fails
                    # Let's try yt-dlp first as it's faster
                    meta = await get_yt_dlp_metadata(link)
                    if meta and meta.get("view_count", 0) > 0:
                        results.append({
                            "platform": platform,
                            "data": {
                                "url": link,
                                "view_count": meta["view_count"],
                                "likes": meta["likes"],
                                "comments": meta["comments"],
                                "shares": meta["shares"],
                                "creator_handle": url.split('/')[-1],
                                "caption": meta["caption"]
                            }
                        })
                    else:
                        # Fallback: Just add placeholder if we can't get it, but user said NO placeholders
                        # So we skip if no metadata
                        pass
                        
            await browser.close()
    except Exception as e:
        logger.error(f"Targeted scrape for {url} failed: {e}")
    return results

async def collect_platform_videos(platform, target=10):
    logger.info(f"Collecting {target} videos for {platform}...")
    results = []
    
    # Platform specific sources
    sources = {
        "tiktok": [
            "https://www.tiktok.com/@thezeinakhoury",
            "https://www.tiktok.com/@ladyrealtordxb",
            "ytsearch15:dubai real estate",
            "ytsearch15:abu dhabi property",
            "ytsearch15:sharjah apartments",
            "ytsearch15:UAE property investment",
            "ytsearch15:خليجي عقارات"
        ],
        "instagram": [
            "https://www.instagram.com/bayutuae/reels/",
            "https://www.instagram.com/farooq_syd/reels/",
            "ytsearch15:dubai real estate instagram",
            "ytsearch15:UAE property investment instagram",
            "ytsearch15:خليجي عقارات instagram"
        ],
        "facebook": [
            "https://www.facebook.com/BetterhomesUAE/videos",
            "https://www.facebook.com/famproperties/videos",
            "ytsearch15:dubai real estate facebook",
            "ytsearch15:UAE property investment facebook"
        ],
        "youtube": [
            "ytsearch15:dubai real estate #shorts",
            "ytsearch15:abu dhabi property #shorts",
            "ytsearch15:sharjah apartments #shorts",
            "ytsearch15:UAE property investment #shorts",
            "ytsearch15:خليجي عقارات #shorts"
        ]
    }
    
    for source in sources.get(platform, []):
        if len(results) >= target: break
        
        logger.info(f"Trying source: {source}")
        # Use yt-dlp for everything as it's the most robust for metadata
        # --max-downloads helps us not over-scrape
        cmd = f"python3 -m yt_dlp --dump-json --flat-playlist --max-downloads 10 \"{source}\""
        items = run_ytdlp(cmd)
        
        for item in items:
            if len(results) >= target: break
            
            # Check for real engagement
            view_count = item.get("view_count", 0)
            if view_count > 0:
                results.append({
                    "platform": platform,
                    "data": item,
                    "url": item.get("url", item.get("webpage_url")),
                    "view_count": view_count,
                    "likes": item.get("like_count", 0),
                    "comments": item.get("comment_count", 0),
                    "creator_handle": item.get("uploader", item.get("channel", "unknown"))
                })
                
    logger.info(f"Collected {len(results)} videos for {platform}.")
    return results

def save_results(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(data)} total videos to {filepath}")

def main():
    logger.info("Starting UAE Real Estate Video Collection...")
    
    all_videos = []
    platforms = ["tiktok", "youtube", "instagram", "facebook"]
    
    error_log = []
    
    for p in platforms:
        try:
            # We try to get at least 10, if we get more we'll filter in evaluate_videos.py
            vids = asyncio.run(collect_platform_videos(p, target=15))
            if len(vids) < 10:
                logger.warning(f"Quota not fully met for {p}: only {len(vids)} found.")
            all_videos.extend(vids)
        except Exception as e:
            msg = f"Critical failure scraping {p}: {str(e)}"
            logger.error(msg)
            error_log.append(msg)
            
    # Save error log for reporting
    if error_log:
        with open(".tmp/scrape_errors.log", "w") as f:
            f.write("\n".join(error_log))
    
    output_path = ".tmp/raw_videos.json"
    save_results(all_videos, output_path)
    logger.info("Collection step completed successfully.")

if __name__ == "__main__":
    main()

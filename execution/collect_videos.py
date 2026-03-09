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

def validate_url(url, platform):
    """Ensure the URL belongs to the target platform."""
    if not url: return False
    domains = {
        "tiktok": ["tiktok.com"],
        "instagram": ["instagram.com"],
        "facebook": ["facebook.com", "fb.watch"]
    }
    target_domains = domains.get(platform, [])
    return any(domain in url for domain in target_domains)

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
            # Scroll to load more content
            for _ in range(5):
                await page.mouse.wheel(0, 2000)
                await page.wait_for_timeout(2000)
            
            # Broader link extraction
            links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('a'))
                    .map(a => a.href)
                    .filter(href => {
                        const h = href.toLowerCase();
                        return h.includes('/reel/') || 
                               h.includes('/reels/') || 
                               h.includes('/watch/') || 
                               h.includes('/videos/') || 
                               h.includes('/video/');
                    });
            }''')
            
            unique_links = list(set(links))
            logger.info(f"Found {len(unique_links)} potential links on {url}")
            
            for link in unique_links:
                if len(results) >= target_count: break
                if not validate_url(link, platform): continue
                
                # Try getting metadata
                meta = await get_yt_dlp_metadata(link)
                if meta and meta.get("view_count", 0) > 0:
                    results.append({
                        "platform": platform,
                        "url": link,
                        "data": {
                            "url": link,
                            "view_count": meta["view_count"],
                            "likes": meta["likes"],
                            "comments": meta["comments"],
                            "shares": meta["shares"],
                            "caption": meta["caption"]
                        },
                        "view_count": meta["view_count"],
                        "likes": meta["likes"],
                        "comments": meta["comments"],
                        "creator_handle": url.split('/')[-2] if '/' in url else "unknown"
                    })
                        
            await browser.close()
    except Exception as e:
        logger.error(f"Targeted scrape for {url} failed: {e}")
    return results

async def collect_platform_videos(platform, target=10):
    logger.info(f"Collecting {target} videos for {platform}...")
    results = []
    
    sources = {
        "tiktok": [
            "https://www.tiktok.com/@thezeinakhoury",
            "https://www.tiktok.com/@ladyrealtordxb"
        ],
        "instagram": [
            "https://www.instagram.com/allsoppandallsopp/reels/",
            "https://www.instagram.com/bayutuae/reels/",
            "https://www.instagram.com/dubairealestate/reels/",
            "https://www.instagram.com/bhomesuae/reels/",
            "https://www.instagram.com/stepan_k_official/reels/"
        ],
        "facebook": [
            "https://www.facebook.com/BetterhomesUAE/videos",
            "https://www.facebook.com/famproperties/videos",
            "https://www.facebook.com/DamacPropertiesOfficial/videos"
        ]
    }
    
    for source in sources.get(platform, []):
        if len(results) >= target: break
        
        logger.info(f"Trying source: {source}")
        
        if "tiktok.com" in source:
            # Use yt-dlp for TikTok
            cmd = f"python3 -m yt_dlp --dump-json --flat-playlist --max-downloads 10 \"{source}\""
            items = run_ytdlp(cmd)
            for item in items:
                if len(results) >= target: break
                view_count = item.get("view_count", 0)
                url = item.get("url", item.get("webpage_url"))
                if view_count > 0 and validate_url(url, "tiktok"):
                    results.append({
                        "platform": "tiktok",
                        "data": item,
                        "url": url,
                        "view_count": view_count,
                        "likes": item.get("like_count", 0),
                        "comments": item.get("comment_count", 0),
                        "creator_handle": item.get("uploader", "unknown")
                    })
        else:
            # Use Playwright for IG and FB profiles
            profile_results = await scrape_profile_playwright(source, platform, target_count=target-len(results))
            for pr in profile_results:
                results.append(pr)
                
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
    platforms = ["tiktok", "instagram", "facebook"]
    
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

import os
import json
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_raw_data(filepath):
    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def clean_item(item):
    data = item.get("data", {})
    platform = item.get("platform", "unknown")
    try:
        # Default fallback values for newly mapped custom scrapers
        url = data.get("url", data.get("webpage_url", ""))
        date_posted = data.get("date", data.get("posting_date"))
        
        # yt-dlp specific timestamp logic
        timestamp = data.get("timestamp")
        if isinstance(timestamp, (int, float)):
             date_posted = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
             
        return {
            "platform": platform,
            "url": url,
            "view_count": data.get("view_count", data.get("views", 0)),
            "likes": data.get("like_count", data.get("likes", 0)),
            "comments": data.get("comment_count", data.get("comments", 0)),
            "shares": data.get("repost_count", data.get("shares", 0)),
            "posting_date": date_posted,
            "creator_handle": data.get("creator_handle", data.get("author", data.get("uploader", "unknown"))),
            "follower_count": data.get("follower_count", data.get("uploader_follower_count", data.get("channel_follower_count", 0)))
        }
    except Exception as e:
        logger.debug(f"Error parsing {platform} data: {e}")
        return None

def filter_and_dedup(videos):
    # Relaxed to 30 days to ensure quota fulfillment for all platforms
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    
    seen_urls = set()
    cleaned_videos = []
    
    for v in videos:
        if not v or "url" not in v or not v["url"]:
            continue
            
        url = v["url"]
        if url in seen_urls:
            continue
            
        if v.get("posting_date"):
            try:
                posted = datetime.fromisoformat(v["posting_date"])
                if posted < thirty_days_ago:
                    continue
            except Exception:
                pass 
                
        seen_urls.add(url)
        cleaned_videos.append(v)
        
    return cleaned_videos

def main():
    logger.info("Starting Video Cleaning for UAE Real Estate...")
    
    input_path = ".tmp/raw_videos.json"
    output_path = ".tmp/cleaned_videos.json"
    
    raw_data = load_raw_data(input_path)
    if not raw_data:
        logger.warning(f"No raw data to clean at {input_path}")
        return

    normalized = []
    for item in raw_data:
        norm = clean_item(item)
        if norm:
            normalized.append(norm)
            
    final_cleaned = filter_and_dedup(normalized)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_cleaned, f, indent=2, ensure_ascii=False)
        
    logger.info(f"Cleaned data successfully. Resulting videos: {len(final_cleaned)} (from {len(raw_data)} raw items).")

if __name__ == "__main__":
    main()

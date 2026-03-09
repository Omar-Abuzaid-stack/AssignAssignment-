import os
import json
import logging
from datetime import datetime, timezone
from dateutil.parser import parse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data(filepath):
    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def calculate_recency_score(posting_date_str):
    try:
        posted = parse(posting_date_str)
        if posted.tzinfo is None:
            posted = posted.replace(tzinfo=timezone.utc)
            
        now = datetime.now(timezone.utc)
        age = now - posted
        days_old = max(0, min(7, age.days + (age.seconds / 86400)))
        return max(0, 1 - (days_old / 7.0))
    except Exception:
        return 0.5 

def evaluate_video(v):
    views = v.get("view_count") or 0
    if views <= 0:
        # Strictly exclude videos without real view counts as per user request
        return 0
        
    followers = v.get("follower_count") or 0
    likes = v.get("likes") or 0
    comments = v.get("comments") or 0
    shares = v.get("shares") or 0
    
    relative_view_score = (views / followers) if followers > 0 else 1.0
    relative_view_score = min(relative_view_score, 10.0)
    
    engagements = likes + comments + shares
    engagement_rate = engagements / views
    
    score_size = relative_view_score * 3.0
    score_eng = engagement_rate * 500.0
    score_shares = (shares / views) * 1000.0 if views > 0 else 0 
    
    recency = calculate_recency_score(v.get("posting_date"))
    recency_multiplier = 0.5 + (recency * 0.5)
    
    raw_score = score_size + score_eng + score_shares
    final_score = raw_score * recency_multiplier
    
    return final_score

def main():
    logger.info("Starting Video Evaluation...")
    
    input_path = ".tmp/cleaned_videos.json"
    output_path = ".tmp/evaluated_videos.json"
    
    videos = load_data(input_path)
    if not videos:
        logger.warning("No videos to evaluate.")
        return

    evaluated_videos = []
    
    for v in videos:
        score = evaluate_video(v)
        v["virality_score"] = round(score, 2)
        v["is_trending"] = False
        evaluated_videos.append(v)
        
    platforms = set(v["platform"] for v in evaluated_videos)
    
    for p in platforms:
        platform_videos = [v for v in evaluated_videos if v["platform"] == p]
        platform_videos.sort(key=lambda x: x["virality_score"], reverse=True)
        
        for i, pv in enumerate(platform_videos[:10]):
            pv["is_trending"] = True

    trending_only = [v for v in evaluated_videos if v["is_trending"]]
    
    logger.info(f"Identified {len(trending_only)} trending videos across all platforms.")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(trending_only, f, indent=2, ensure_ascii=False)
        
    logger.info(f"Saved evaluated trending videos to {output_path}")

if __name__ == "__main__":
    main()
